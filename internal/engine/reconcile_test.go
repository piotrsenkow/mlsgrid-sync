package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
)

func testReconcileConfig() ReconcileConfig {
	return ReconcileConfig{
		BaseURL:           "https://replay.example.test/v2",
		Resources:         []string{"Property"},
		OriginatingSystem: "testmls",
		PageSize:          100,
		Expand:            []string{"Media", "Rooms", "UnitTypes"},
		ChunkSize:         50,
		Log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// sweepURL mirrors the engine's key-sweep query.
func sweepURL(t *testing.T, cfg ReconcileConfig) string {
	t.Helper()
	u, err := mlsgrid.Query{
		Resource:          "Property",
		OriginatingSystem: cfg.OriginatingSystem,
		MlgCanViewTrue:    true,
		Select:            []string{"ListingKey", "ModificationTimestamp"},
		Top:               cfg.PageSize,
	}.URL(cfg.BaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// refetchURL mirrors the engine's stale-record re-fetch query.
func refetchURL(t *testing.T, cfg ReconcileConfig, keys []string) string {
	t.Helper()
	u, err := mlsgrid.Query{
		Resource:          "Property",
		OriginatingSystem: cfg.OriginatingSystem,
		MlgCanViewTrue:    true,
		KeyField:          "ListingKey",
		Keys:              keys,
		Expand:            cfg.Expand,
		Top:               cfg.PageSize,
	}.URL(cfg.BaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// sweepRec is a key+timestamp-only record, as a $select sweep returns.
func sweepRec(t *testing.T, key, modTS string) mlsgrid.Record {
	t.Helper()
	r, err := mlsgrid.ParseRecord([]byte(
		`{"ListingKey":"` + key + `","ModificationTimestamp":"` + modTS + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestReconcilePurgesAndRefreshes(t *testing.T) {
	cfg := testReconcileConfig()
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// Remote feed: TSTKEEP unchanged, TSTSTALE newer than local, TSTNEVER
	// never stored locally. Local extra: TSTZOMBIE (deleted upstream while
	// sync was down).
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		sweepURL(t, cfg): {Records: []mlsgrid.Record{
			sweepRec(t, "TSTKEEP", "2026-06-01T10:00:00Z"),
			sweepRec(t, "TSTSTALE", "2026-06-01T11:30:00Z"),
			sweepRec(t, "TSTNEVER", "2026-06-01T09:00:00Z"),
		}},
		refetchURL(t, cfg, []string{"TSTSTALE"}): {Records: []mlsgrid.Record{
			rec(t, "TSTSTALE", "2026-06-01T11:30:00Z", true),
		}},
	}}
	st := withState(3, readyState(wm))
	st.localKeys = map[string]time.Time{
		"TSTKEEP":   time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
		"TSTSTALE":  time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC), // older than remote
		"TSTZOMBIE": time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
	}

	if err := NewReconcile(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(st.deletedKeys) != 1 || st.deletedKeys[0] != "TSTZOMBIE" {
		t.Errorf("purged = %v, want [TSTZOMBIE] — deletions missed while down must be caught here", st.deletedKeys)
	}
	if len(st.upsertedKeys) != 1 || st.upsertedKeys[0] != "TSTSTALE" {
		t.Errorf("refetched = %v, want [TSTSTALE]", st.upsertedKeys)
	}
	// TSTNEVER stays out: a bounded backfill deliberately skipped it.
	for _, k := range st.upsertedKeys {
		if k == "TSTNEVER" {
			t.Error("missing-locally records must not be imported without IncludeMissing")
		}
	}
	final := st.states["Property"]
	if final.LastFullReconcileAt == nil {
		t.Error("reconcile must stamp last_full_reconcile_at")
	}
	if final.BackfillCompletedAt == nil || final.LastModificationTS == nil {
		t.Error("reconcile must preserve the sync cursor")
	}
}

func TestReconcileIncludeMissing(t *testing.T) {
	cfg := testReconcileConfig()
	cfg.IncludeMissing = true
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		sweepURL(t, cfg): {Records: []mlsgrid.Record{
			sweepRec(t, "TSTNEVER", "2026-06-01T09:00:00Z"),
		}},
		refetchURL(t, cfg, []string{"TSTNEVER"}): {Records: []mlsgrid.Record{
			rec(t, "TSTNEVER", "2026-06-01T09:00:00Z", true),
		}},
	}}
	st := withState(0, readyState(wm))
	st.localKeys = map[string]time.Time{}

	if err := NewReconcile(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(st.upsertedKeys) != 1 || st.upsertedKeys[0] != "TSTNEVER" {
		t.Errorf("--include-missing must import absent records, got %v", st.upsertedKeys)
	}
}

func TestReconcileRequiresCompletedBackfill(t *testing.T) {
	cfg := testReconcileConfig()
	err := NewReconcile(&fakeFetcher{}, &fakeStore{}, cfg).Run(context.Background())
	if !errors.Is(err, ErrBackfillRequired) {
		t.Fatalf("want ErrBackfillRequired, got %v", err)
	}
}

func TestDaemonSchedulesReconcile(t *testing.T) {
	cfg := testSyncConfig()
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rcfg := testReconcileConfig()

	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		syncURL(t, cfg, "Property", wm): {},
		sweepURL(t, rcfg):               {},
	}}
	st := withState(0, readyState(wm)) // LastFullReconcileAt nil -> due immediately
	st.localKeys = map[string]time.Time{}

	s := NewSync(fetcher, st, cfg)
	s.SetReconciler(NewReconcile(fetcher, st, rcfg), time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := s.RunDaemon(ctx); err != nil {
		t.Fatal(err)
	}
	if st.states["Property"].LastFullReconcileAt == nil {
		t.Error("daemon must run a due reconcile and stamp completion")
	}

	// A second daemon window must NOT re-run it (freshly stamped, every=1h):
	// count sweep fetches once only.
	sweeps := 0
	for _, u := range fetcher.fetched {
		if u == sweepURL(t, rcfg) {
			sweeps++
		}
	}
	if sweeps != 1 {
		t.Errorf("sweep ran %d times, want exactly 1 in the window", sweeps)
	}
}

func TestSyncOpenHouseResource(t *testing.T) {
	cfg := testSyncConfig()
	cfg.Resources = []string{"OpenHouse"}
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	url := syncURL(t, cfg, "OpenHouse", wm)

	ohRec := func(key string, canView bool) mlsgrid.Record {
		r, err := mlsgrid.ParseRecord([]byte(
			`{"OpenHouseKey":"` + key + `","ModificationTimestamp":"2026-06-01T13:00:00Z","MlgCanView":` +
				map[bool]string{true: "true", false: "false"}[canView] + `}`))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		url: {Records: []mlsgrid.Record{ohRec("TSTOH1", true), ohRec("TSTOHGONE", false)}},
	}}
	ready := readyState(wm)
	ready.Resource = "OpenHouse"
	st := withState(0, ready)

	res, err := NewSync(fetcher, st, cfg).RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st.ohUpserted) != 1 || st.ohUpserted[0] != "TSTOH1" {
		t.Errorf("open house upserts = %v", st.ohUpserted)
	}
	if len(st.ohDeleted) != 1 || st.ohDeleted[0] != "TSTOHGONE" {
		t.Errorf("open house deletes = %v", st.ohDeleted)
	}
	if res.Upserted != 1 || res.Deleted != 1 {
		t.Errorf("result = %+v", res)
	}
	// OpenHouse has no child expansions.
	if len(fetcher.fetched) == 0 || strings.Contains(fetcher.fetched[0], "expand") {
		t.Errorf("OpenHouse sync must not request $expand: %v", fetcher.fetched)
	}
}

// newTestLimiter is an unconstrained limiter: budget plumbing is exercised
// without any sleeping.
func newTestLimiter() *ratelimit.Limiter {
	return ratelimit.New(ratelimit.Config{}, nil)
}

func TestSyncPersistsRateBudget(t *testing.T) {
	cfg := testSyncConfig()
	wm := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	url := syncURL(t, cfg, "Property", wm)
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		url: {Records: []mlsgrid.Record{rec(t, "TST1", "2026-06-01T13:00:00Z", true)}},
	}}
	st := withState(0, readyState(wm))

	cfg.Limiter = newTestLimiter()
	if _, err := NewSync(fetcher, st, cfg).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st.budgetReads == 0 {
		t.Error("sync must restore the persisted budget at start — restarts cannot launder usage")
	}
	if st.budgetWrites == 0 {
		t.Error("sync must persist budget snapshots as it works")
	}
}

func TestChunks(t *testing.T) {
	got := chunks([]string{"a", "b", "c", "d", "e"}, 2)
	if len(got) != 3 || len(got[0]) != 2 || len(got[2]) != 1 {
		t.Errorf("chunks = %v", got)
	}
	if chunks(nil, 2) != nil {
		t.Error("empty input yields no chunks")
	}
}

func TestBackfillPersistsRateBudget(t *testing.T) {
	cfg := testConfig()
	cfg.Limiter = newTestLimiter()
	first := initialURL(t, cfg, nil)
	fetcher := &fakeFetcher{pages: map[string]*mlsgrid.PageResult{
		first: {Records: []mlsgrid.Record{rec(t, "TST1", "2026-06-01T10:00:00Z", true)}},
	}}
	st := &fakeStore{}
	if err := NewBackfill(fetcher, st, cfg).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st.budgetReads == 0 || st.budgetWrites == 0 {
		t.Errorf("backfill budget persistence: reads=%d writes=%d, want both > 0",
			st.budgetReads, st.budgetWrites)
	}
}
