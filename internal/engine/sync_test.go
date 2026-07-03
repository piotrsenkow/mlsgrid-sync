package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

func testSyncConfig() SyncConfig {
	return SyncConfig{
		BaseURL:           "https://replay.example.test/v2",
		Resources:         []string{"Property"},
		OriginatingSystem: "testmls",
		PageSize:          2,
		Expand:            []string{"Media", "Rooms", "UnitTypes"},
		Interval:          5 * time.Millisecond,
		Log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// withStateMaybe seeds a fakeStore with an optional cursor row.
func withStateMaybe(s *store.SyncState) *fakeStore {
	if s == nil {
		return &fakeStore{}
	}
	return withState(0, s)
}

// syncURL computes the incremental URL for a watermark, mirroring the engine.
// Only Property expands children.
func syncURL(t *testing.T, cfg SyncConfig, resource string, watermark time.Time) string {
	t.Helper()
	q := mlsgrid.Query{
		Resource:                resource,
		OriginatingSystem:       cfg.OriginatingSystem,
		ModificationTimestampGE: &watermark,
		Top:                     cfg.PageSize,
	}
	if resource == "Property" {
		q.Expand = cfg.Expand
		q.Select = cfg.PropertySelect
	}
	u, err := q.URL(cfg.BaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestSyncPropertySelect(t *testing.T) {
	cfg := testSyncConfig()
	cfg.PropertySelect = []string{"ListingKey", "ListPrice", "ModificationTimestamp", "MlgCanView"}
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	url := syncURL(t, cfg, "Property", wm)
	if !strings.Contains(url, "%24select=ListingKey") {
		t.Fatalf("expected $select in URL: %s", url)
	}
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{url: {}}}
	st := withState(0, readyState(wm))
	if _, err := NewSync(fetcher, st, cfg).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSyncSelectNotAppliedToOpenHouse(t *testing.T) {
	cfg := testSyncConfig()
	cfg.Resources = []string{"OpenHouse"}
	cfg.PropertySelect = []string{"ListingKey", "ListPrice"}
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	// Build the expected URL without Select; if the engine wrongly applied
	// PropertySelect to OpenHouse it would fetch a different URL and fail.
	plain := testSyncConfig()
	plain.Resources = []string{"OpenHouse"}
	url := syncURL(t, plain, "OpenHouse", wm)
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{url: {}}}
	ready := readyState(wm)
	ready.Resource = "OpenHouse"
	st := withState(0, ready)
	if _, err := NewSync(fetcher, st, cfg).RunOnce(context.Background()); err != nil {
		t.Fatalf("PropertySelect must not leak into OpenHouse queries: %v", err)
	}
}

// readyState is a cursor row for a completed backfill.
func readyState(watermark time.Time) *store.SyncState {
	done := watermark
	return &store.SyncState{
		Resource: "Property", OriginatingSystem: "testmls",
		LastModificationTS: &watermark, BackfillCompletedAt: &done,
	}
}

func TestSyncRefusesWithoutCompletedBackfill(t *testing.T) {
	cfg := testSyncConfig()
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		state *store.SyncState
	}{
		{"no cursor at all", nil},
		{"backfill incomplete", &store.SyncState{
			Resource: "Property", OriginatingSystem: "testmls",
			LastModificationTS: &wm, // watermark exists, completion missing
		}},
		{"null watermark", &store.SyncState{
			Resource: "Property", OriginatingSystem: "testmls",
			BackfillCompletedAt: &wm,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := withStateMaybe(tc.state)
			_, err := NewSync(&fakeFetcher{}, st, cfg).RunOnce(context.Background())
			if !errors.Is(err, ErrBackfillRequired) {
				t.Fatalf("want ErrBackfillRequired, got %v", err)
			}
		})
	}
}

func TestSyncHappyPathDeletesRevoked(t *testing.T) {
	cfg := testSyncConfig()
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	first := syncURL(t, cfg, "Property", wm)

	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		first: {Records: []mlsgrid.Record{
			rec(t, "TSTNEW", "2026-06-01T13:00:00.250Z", true),
			rec(t, "TSTGONE", "2026-06-01T13:30:00.000Z", false),
		}},
	}}
	st := withState(0, readyState(wm))

	res, err := NewSync(fetcher, st, cfg).RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Upserted != 1 || res.Deleted != 1 {
		t.Errorf("result = %+v, want 1 upserted / 1 deleted", res)
	}
	if len(st.deletedKeys) != 1 || st.deletedKeys[0] != "TSTGONE" {
		t.Errorf("deleted = %v", st.deletedKeys)
	}

	// The incremental filter must include the ge watermark but must NOT
	// filter MlgCanView — revoked records have to arrive to be deleted.
	if !strings.Contains(first, "ModificationTimestamp%20ge%20") {
		t.Errorf("incremental URL missing ge filter: %s", first)
	}
	if strings.Contains(first, "MlgCanView") {
		t.Errorf("incremental URL must not filter MlgCanView: %s", first)
	}

	// Watermark advanced to the revoked record's timestamp (it was the
	// newest) and completion state survived the cursor write.
	final := st.states["Property"]
	want := time.Date(2026, 6, 1, 13, 30, 0, 0, time.UTC)
	if final.LastModificationTS == nil || !final.LastModificationTS.Equal(want) {
		t.Errorf("watermark = %v, want %v", final.LastModificationTS, want)
	}
	if final.BackfillCompletedAt == nil {
		t.Error("sync must preserve backfill_completed_at when advancing the cursor")
	}
}

func TestSyncRebuildsAfter400(t *testing.T) {
	cfg := testSyncConfig()
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	first := syncURL(t, cfg, "Property", wm)
	stale := cfg.BaseURL + "/Property?stale-next"
	advanced := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	rebuilt := syncURL(t, cfg, "Property", advanced)

	fetcher := &fakeFetcher{
		pages: map[string]*mlsgrid.PageResult{
			first: {
				Records:  []mlsgrid.Record{rec(t, "TST1", "2026-06-01T13:00:00.000Z", true)},
				NextLink: stale,
			},
			rebuilt: {},
		},
		errs: map[string]error{
			stale: &mlsgrid.HTTPError{StatusCode: 400, Body: "stale skiptoken"},
		},
	}
	st := withState(0, readyState(wm))
	res, err := NewSync(fetcher, st, cfg).RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Pages != 2 {
		t.Errorf("pages = %d, want 2 (original + rebuilt)", res.Pages)
	}
}

func TestDaemonLoopsUntilCancelled(t *testing.T) {
	cfg := testSyncConfig()
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	url := syncURL(t, cfg, "Property", wm)
	// Empty pages leave the watermark unchanged, so every pass fetches the
	// same URL — a permanently caught-up feed.
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{url: {}}}
	st := withState(0, readyState(wm))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := NewSync(fetcher, st, cfg).RunDaemon(ctx); err != nil {
		t.Fatalf("cancelled daemon must exit clean: %v", err)
	}
	if len(fetcher.fetched) < 2 {
		t.Errorf("daemon ran %d passes, want several — it must not exit when caught up", len(fetcher.fetched))
	}
}

func TestDaemonHaltsOnOpenCircuit(t *testing.T) {
	cfg := testSyncConfig()
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	url := syncURL(t, cfg, "Property", wm)
	fetcher := &fakeFetcher{stickyErrs: map[string]error{
		url: fmt.Errorf("fetch: %w", ratelimit.ErrCircuitOpen),
	}}
	st := withState(0, readyState(wm))

	err := NewSync(fetcher, st, cfg).RunDaemon(context.Background())
	if !errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Fatalf("daemon must halt loudly on an open circuit, got %v", err)
	}
	if len(fetcher.fetched) != 1 {
		t.Errorf("daemon must not retry through a suspension: %d fetches", len(fetcher.fetched))
	}
}

func TestDaemonExitsWhenBackfillMissing(t *testing.T) {
	cfg := testSyncConfig()
	err := NewSync(&fakeFetcher{}, &fakeStore{}, cfg).RunDaemon(context.Background())
	if !errors.Is(err, ErrBackfillRequired) {
		t.Fatalf("daemon must exit on a missing backfill (retrying cannot fix it), got %v", err)
	}
}

func TestHealthEndpoint(t *testing.T) {
	health := &daemonHealth{}
	srv, addr, err := startHealthServer("127.0.0.1:0", health)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Close() }()

	get := func() int {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	if code := get(); code != http.StatusOK {
		t.Errorf("before first pass: %d, want 200", code)
	}
	health.recordFailure(errors.New("boom"))
	if code := get(); code != http.StatusServiceUnavailable {
		t.Errorf("after failure: %d, want 503", code)
	}
	health.recordSuccess(time.Now().Add(time.Second))
	if code := get(); code != http.StatusOK {
		t.Errorf("after recovery: %d, want 200", code)
	}
}
