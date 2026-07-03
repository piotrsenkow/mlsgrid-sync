package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// fakeStore is an in-memory store.Store capturing engine interactions.
type fakeStore struct {
	count        int64
	state        *store.SyncState
	stateHistory []store.SyncState
	upsertedKeys []string
	deletedKeys  []string
}

func (f *fakeStore) Migrate(ctx context.Context) error                   { return nil }
func (f *fakeStore) ContractVersion(ctx context.Context) (string, error) { return "1.0.0", nil }
func (f *fakeStore) Close()                                              {}

func (f *fakeStore) UpsertProperties(ctx context.Context, recs []mlsgrid.Record) (store.UpsertStats, error) {
	var stats store.UpsertStats
	for _, rec := range recs {
		if rec.ListingKey() == "" {
			stats.Skipped++
			continue
		}
		f.upsertedKeys = append(f.upsertedKeys, rec.ListingKey())
		stats.Inserted++
	}
	return stats, nil
}

func (f *fakeStore) DeleteProperties(ctx context.Context, keys []string) (int64, error) {
	f.deletedKeys = append(f.deletedKeys, keys...)
	return int64(len(keys)), nil
}

func (f *fakeStore) UpsertOpenHouses(ctx context.Context, recs []mlsgrid.Record) (store.UpsertStats, error) {
	return store.UpsertStats{}, nil
}

func (f *fakeStore) DeleteOpenHouses(ctx context.Context, keys []string) (int64, error) {
	return 0, nil
}

func (f *fakeStore) SyncState(ctx context.Context, resource, system string) (*store.SyncState, error) {
	if f.state == nil {
		return nil, nil
	}
	cp := *f.state
	return &cp, nil
}

func (f *fakeStore) SetSyncState(ctx context.Context, s store.SyncState) error {
	cp := s
	f.state = &cp
	f.stateHistory = append(f.stateHistory, s)
	return nil
}

func (f *fakeStore) PropertyCount(ctx context.Context) (int64, error) { return f.count, nil }

// fakeFetcher serves canned pages (or errors) by URL.
type fakeFetcher struct {
	pages   map[string]*mlsgrid.PageResult
	errs    map[string]error
	fetched []string
}

func (f *fakeFetcher) Fetch(ctx context.Context, url string) (*mlsgrid.PageResult, error) {
	f.fetched = append(f.fetched, url)
	if err, ok := f.errs[url]; ok {
		delete(f.errs, url) // errors fire once, like a transient stale link
		return nil, err
	}
	if page, ok := f.pages[url]; ok {
		return page, nil
	}
	return nil, fmt.Errorf("fakeFetcher: unexpected URL %s", url)
}

func rec(t *testing.T, key, modTS string, canView bool) mlsgrid.Record {
	t.Helper()
	r, err := mlsgrid.ParseRecord([]byte(fmt.Sprintf(
		`{"ListingKey":%q,"ListingId":"ID-%s","OriginatingSystemName":"testmls","ModificationTimestamp":%q,"MlgCanView":%v}`,
		key, key, modTS, canView)))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func testConfig() BackfillConfig {
	return BackfillConfig{
		BaseURL:           "https://replay.example.test/v2",
		Resource:          "Property",
		OriginatingSystem: "testmls",
		PageSize:          2,
		Expand:            []string{"Media", "Rooms", "UnitTypes"},
		Log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// initialURL computes the URL the engine builds for a fresh run, so tests
// don't hand-encode OData filters.
func initialURL(t *testing.T, cfg BackfillConfig, since *time.Time) string {
	t.Helper()
	u, err := mlsgrid.Query{
		Resource:                cfg.Resource,
		OriginatingSystem:       cfg.OriginatingSystem,
		MlgCanViewTrue:          true,
		ModificationTimestampGE: since,
		Expand:                  cfg.Expand,
		Top:                     cfg.PageSize,
	}.URL(cfg.BaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestBackfillHappyPath(t *testing.T) {
	cfg := testConfig()
	first := initialURL(t, cfg, nil)
	next := cfg.BaseURL + "/Property?$skip=2"

	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		first: {
			Records: []mlsgrid.Record{
				rec(t, "TST1", "2026-06-01T10:00:00.000Z", true),
				rec(t, "TST2", "2026-06-01T11:00:00.500Z", true),
			},
			NextLink:  next,
			WireBytes: 100,
		},
		next: {
			Records:   []mlsgrid.Record{rec(t, "TST3", "2026-06-01T12:00:00.999Z", true)},
			WireBytes: 50,
		},
	}}
	st := &fakeStore{}

	if err := NewBackfill(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(st.upsertedKeys) != 3 {
		t.Errorf("upserted %v", st.upsertedKeys)
	}
	if !strings.Contains(fetcher.fetched[0], "MlgCanView%20eq%20true") {
		t.Errorf("backfill must filter MlgCanView eq true: %s", fetcher.fetched[0])
	}

	// Cursor persisted after page 1 with the resume link and page-1 watermark.
	if len(st.stateHistory) != 3 {
		t.Fatalf("state writes = %d, want 3 (per page + completion)", len(st.stateHistory))
	}
	mid := st.stateHistory[0]
	if mid.InProgressURL == nil || *mid.InProgressURL != next {
		t.Errorf("page-1 state must persist the nextLink, got %+v", mid.InProgressURL)
	}
	if mid.LastModificationTS == nil || !mid.LastModificationTS.Equal(time.Date(2026, 6, 1, 11, 0, 0, 5e8, time.UTC)) {
		t.Errorf("page-1 watermark = %v", mid.LastModificationTS)
	}
	if mid.BackfillCompletedAt != nil {
		t.Error("backfill must not be complete mid-run")
	}

	final := st.stateHistory[2]
	if final.InProgressURL != nil || final.BackfillCompletedAt == nil {
		t.Errorf("final state: %+v", final)
	}
	if !final.LastModificationTS.Equal(time.Date(2026, 6, 1, 12, 0, 0, 999e6, time.UTC)) {
		t.Errorf("final watermark = %v", final.LastModificationTS)
	}
}

func TestBackfillRefusesNonEmptyWithoutForce(t *testing.T) {
	cfg := testConfig()
	st := &fakeStore{count: 42}
	err := NewBackfill(&fakeFetcher{}, st, cfg).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("want --force refusal, got %v", err)
	}

	// Same store with Force runs (single empty page).
	cfg.Force = true
	first := initialURL(t, cfg, nil)
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{first: {}}}
	if err := NewBackfill(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatalf("--force must allow the run: %v", err)
	}
}

func TestBackfillRefusesCompletedWithoutForce(t *testing.T) {
	cfg := testConfig()
	done := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	ts := done
	st := &fakeStore{state: &store.SyncState{
		Resource: "Property", OriginatingSystem: "testmls",
		LastModificationTS: &ts, BackfillCompletedAt: &done,
	}}
	err := NewBackfill(&fakeFetcher{}, st, cfg).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("completed backfill must require --force, got %v", err)
	}
}

func TestBackfillResumesFromPersistedURL(t *testing.T) {
	cfg := testConfig()
	resume := cfg.BaseURL + "/Property?$skip=4000"
	ts := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	st := &fakeStore{
		count: 4000, // existing rows must not require --force when resuming
		state: &store.SyncState{
			Resource: "Property", OriginatingSystem: "testmls",
			LastModificationTS: &ts, InProgressURL: &resume,
		},
	}
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		resume: {Records: []mlsgrid.Record{rec(t, "TST9", "2026-06-01T13:00:00.000Z", true)}},
	}}
	if err := NewBackfill(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fetcher.fetched[0] != resume {
		t.Errorf("must resume from persisted URL, fetched %s", fetcher.fetched[0])
	}
	if st.state.BackfillCompletedAt == nil {
		t.Error("resumed run must complete the backfill")
	}
}

func TestBackfillRebuildsAfter400(t *testing.T) {
	cfg := testConfig()
	first := initialURL(t, cfg, nil)
	stale := cfg.BaseURL + "/Property?$skip=500000"
	watermark := time.Date(2026, 6, 1, 11, 0, 0, 5e8, time.UTC)
	rebuilt := initialURL(t, cfg, &watermark)

	fetcher := &fakeFetcher{
		pages: map[string]*mlsgrid.PageResult{
			first: {
				Records:  []mlsgrid.Record{rec(t, "TST1", "2026-06-01T11:00:00.500Z", true)},
				NextLink: stale,
			},
			rebuilt: {Records: []mlsgrid.Record{rec(t, "TST2", "2026-06-01T12:00:00.000Z", true)}},
		},
		errs: map[string]error{
			stale: &mlsgrid.HTTPError{StatusCode: 400, Body: "$skip limit exceeded"},
		},
	}
	st := &fakeStore{}
	if err := NewBackfill(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(st.upsertedKeys) != 2 {
		t.Errorf("upserted %v — the run must continue past the $skip wall", st.upsertedKeys)
	}
	found := false
	for _, u := range fetcher.fetched {
		if u == rebuilt {
			found = true
		}
	}
	if !found {
		t.Errorf("rebuilt URL not fetched; fetched: %v", fetcher.fetched)
	}
}

func TestBackfillDoubles400IsFatal(t *testing.T) {
	cfg := testConfig()
	first := initialURL(t, cfg, nil)
	stale := cfg.BaseURL + "/Property?stale"
	watermark := time.Date(2026, 6, 1, 11, 0, 0, 5e8, time.UTC)
	rebuilt := initialURL(t, cfg, &watermark)

	fetcher := &fakeFetcher{
		pages: map[string]*mlsgrid.PageResult{
			first: {
				Records:  []mlsgrid.Record{rec(t, "TST1", "2026-06-01T11:00:00.500Z", true)},
				NextLink: stale,
			},
		},
		errs: map[string]error{
			stale:   &mlsgrid.HTTPError{StatusCode: 400, Body: "bad"},
			rebuilt: &mlsgrid.HTTPError{StatusCode: 400, Body: "still bad"},
		},
	}
	err := NewBackfill(fetcher, &fakeStore{}, cfg).Run(context.Background())
	if err == nil {
		t.Fatal("consecutive 400s must abort, not loop")
	}
}

func TestBackfillMaxPagesStopsWithResumeCursor(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPages = 1
	first := initialURL(t, cfg, nil)
	next := cfg.BaseURL + "/Property?$skip=2"
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		first: {
			Records:  []mlsgrid.Record{rec(t, "TST1", "2026-06-01T10:00:00.000Z", true)},
			NextLink: next,
		},
	}}
	st := &fakeStore{}
	if err := NewBackfill(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fetcher.fetched) != 1 {
		t.Errorf("fetched %d pages, want 1", len(fetcher.fetched))
	}
	if st.state.InProgressURL == nil || *st.state.InProgressURL != next {
		t.Errorf("resume cursor must survive a capped run: %+v", st.state)
	}
	if st.state.BackfillCompletedAt != nil {
		t.Error("a capped run must not mark the backfill complete")
	}
}

func TestBackfillDeletesRevokedRecords(t *testing.T) {
	cfg := testConfig()
	first := initialURL(t, cfg, nil)
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		first: {Records: []mlsgrid.Record{
			rec(t, "TST1", "2026-06-01T10:00:00.000Z", true),
			rec(t, "TSTGONE", "2026-06-01T10:30:00.000Z", false),
		}},
	}}
	st := &fakeStore{}
	if err := NewBackfill(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(st.deletedKeys) != 1 || st.deletedKeys[0] != "TSTGONE" {
		t.Errorf("deleted = %v", st.deletedKeys)
	}
	if len(st.upsertedKeys) != 1 || st.upsertedKeys[0] != "TST1" {
		t.Errorf("upserted = %v", st.upsertedKeys)
	}
}

func TestBackfillEmptyFeedStillSetsCursor(t *testing.T) {
	cfg := testConfig()
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	cfg.Since = &since
	first := initialURL(t, cfg, &since)
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{first: {}}}
	st := &fakeStore{}
	if err := NewBackfill(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st.state == nil || st.state.BackfillCompletedAt == nil {
		t.Fatal("empty feed must still complete")
	}
	if st.state.LastModificationTS == nil {
		t.Error("watermark must never be NULL after a completed backfill — incremental sync refuses NULL cursors")
	}
	if !strings.Contains(fetcher.fetched[0], "ModificationTimestamp%20ge%20") {
		t.Errorf("--since must add the ge filter: %s", fetcher.fetched[0])
	}
}
