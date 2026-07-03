//go:build integration

// Integration tests run against a disposable testcontainers Postgres — never
// a shared or production database. One container serves the whole package;
// each test isolates itself in its own schema (the store is schema-agnostic
// by design, so this doubles as a test of that property).
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/piotrsenkow/mlsgrid-sync/internal/fieldscope"
	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

var testDSN string

func TestMain(m *testing.M) {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("mlsgrid_test"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting postgres container:", err)
		os.Exit(1)
	}
	testDSN, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "container connection string:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = ctr.Terminate(ctx)
	os.Exit(code)
}

// newTestStore returns a migrated store scoped to its own schema.
func newTestStore(t *testing.T, schema string, opts Options) *Store {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Fatal(err)
	}
	opts.Schema = schema
	s := NewWithPool(pool, opts)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// loadFixtureRecords decodes one OData fixture page into records.
func loadFixtureRecords(t *testing.T, name string) []mlsgrid.Record {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "odata", name))
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Value []json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	recs := make([]mlsgrid.Record, 0, len(page.Value))
	for _, raw := range page.Value {
		rec, err := mlsgrid.ParseRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, rec)
	}
	return recs
}

// mutateFixture re-serializes a record with overrides, simulating a feed
// update to an already-stored listing.
func mutateFixture(t *testing.T, rec mlsgrid.Record, overrides map[string]any) mlsgrid.Record {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Raw(), &m); err != nil {
		t.Fatal(err)
	}
	for k, v := range overrides {
		if v == nil {
			delete(m, k)
			continue
		}
		m[k] = v
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	out, err := mlsgrid.ParseRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestMigrateIdempotentAndVersioned(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_migrate", Options{})
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second migrate must be a no-op: %v", err)
	}
	v, err := s.ContractVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v != "1.0.0" {
		t.Errorf("contract version = %q, want 1.0.0", v)
	}
}

func TestMigrateRefusesChecksumDrift(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_drift", Options{})
	if _, err := s.pool.Exec(ctx,
		`UPDATE t_drift.schema_migrations SET checksum = 'tampered'`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err == nil {
		t.Fatal("migrate must refuse a modified already-applied migration")
	}
}

func TestUpsertFixturePage(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_upsert", Options{})
	recs := loadFixtureRecords(t, "property_page1.json")

	stats, err := s.UpsertProperties(ctx, recs)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Inserted != 2 || stats.Updated != 0 || stats.Skipped != 0 || stats.Events != 2 {
		t.Errorf("stats = %+v, want 2 inserted with 2 new_listing events", stats)
	}

	var city string
	var listPrice string
	var modTS time.Time
	var canUse []string
	var rawStatus *string
	err = s.pool.QueryRow(ctx, `
		SELECT city, list_price::text, modification_timestamp, mlg_can_use, raw->>'StandardStatus'
		FROM t_upsert.property WHERE listing_key = 'TST0000000001'`).
		Scan(&city, &listPrice, &modTS, &canUse, &rawStatus)
	if err != nil {
		t.Fatal(err)
	}
	if city != "Testville" || listPrice != "425000" {
		t.Errorf("core columns: city=%q price=%q", city, listPrice)
	}
	want := time.Date(2026, 6, 1, 12, 0, 0, 123e6, time.UTC)
	if !modTS.Equal(want) {
		t.Errorf("modification_timestamp = %v, want %v (millisecond precision)", modTS, want)
	}
	if len(canUse) != 2 || canUse[0] != "IDX" {
		t.Errorf("mlg_can_use = %v", canUse)
	}
	if rawStatus == nil || *rawStatus != "Active" {
		t.Errorf("raw JSONB must hold the record as received, got StandardStatus=%v", rawStatus)
	}

	var mediaCount, roomCount int
	var status string
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*), min(storage_status) FROM t_upsert.media
		WHERE listing_key = 'TST0000000001'`).Scan(&mediaCount, &status); err != nil {
		t.Fatal(err)
	}
	if mediaCount != 2 || status != "skipped" {
		t.Errorf("media: count=%d status=%q, want 2 skipped (metadata-only default)", mediaCount, status)
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM t_upsert.room WHERE listing_key = 'TST0000000001'`).Scan(&roomCount); err != nil {
		t.Fatal(err)
	}
	if roomCount != 1 {
		t.Errorf("rooms = %d, want 1", roomCount)
	}

	n, err := s.Count(ctx, "Property")
	if err != nil || n != 2 {
		t.Errorf("PropertyCount = %d, %v", n, err)
	}

	// Idempotent re-upsert: same page again — updates, no new events. This
	// is what makes the ge (not gt) cursor safe.
	stats, err = s.UpsertProperties(ctx, recs)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Updated != 2 || stats.Inserted != 0 || stats.Events != 0 {
		t.Errorf("re-upsert stats = %+v, want 2 updated with 0 events", stats)
	}
}

func TestReupsertCapturesChangeEvents(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_events", Options{})
	recs := loadFixtureRecords(t, "property_page1.json")
	subject := recs[0] // TST0000000001, Active @ 425000

	if _, err := s.UpsertProperties(ctx, []mlsgrid.Record{subject}); err != nil {
		t.Fatal(err)
	}

	// Price drop + goes pending.
	changed := mutateFixture(t, subject, map[string]any{
		"ListPrice":             399000,
		"StandardStatus":        "Pending",
		"ModificationTimestamp": "2026-06-02T09:00:00.000Z",
	})
	stats, err := s.UpsertProperties(ctx, []mlsgrid.Record{changed})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Events != 2 {
		t.Errorf("price+status change: %d events, want 2", stats.Events)
	}

	// Deal falls through: Pending -> Active is back_on_market.
	relisted := mutateFixture(t, changed, map[string]any{
		"StandardStatus":        "Active",
		"ModificationTimestamp": "2026-06-03T09:00:00.000Z",
	})
	if _, err := s.UpsertProperties(ctx, []mlsgrid.Record{relisted}); err != nil {
		t.Fatal(err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT event_type, old_value, new_value FROM t_events.listing_event
		WHERE listing_key = 'TST0000000001' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	type ev struct{ typ, old, new string }
	var got []ev
	var typ string
	var oldV, newV *string
	deref := func(p *string) string {
		if p == nil {
			return "<null>"
		}
		return *p
	}
	if _, err := pgx.ForEachRow(rows, []any{&typ, &oldV, &newV}, func() error {
		got = append(got, ev{typ, deref(oldV), deref(newV)})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []ev{
		{"new_listing", "<null>", "425000"},
		{"price_change", "425000", "399000"},
		{"status_change", "Active", "Pending"},
		{"back_on_market", "Pending", "Active"},
	}
	if len(got) != len(want) {
		t.Fatalf("event timeline = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestChildReplacementSemantics(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_children", Options{})
	recs := loadFixtureRecords(t, "property_page1.json")
	subject := recs[0] // 2 media, 1 room

	if _, err := s.UpsertProperties(ctx, []mlsgrid.Record{subject}); err != nil {
		t.Fatal(err)
	}

	// A media row that was already downloaded must keep its pipeline state
	// across metadata refreshes (MediaKeys are immutable upstream).
	if _, err := s.pool.Exec(ctx, `
		UPDATE t_children.media SET storage_status = 'downloaded', local_path = 'ab/TSTMEDIA0001'
		WHERE media_key = 'TSTMEDIA0001'`); err != nil {
		t.Fatal(err)
	}

	// Feed now returns only the first photo -> second is an orphan.
	oneMedia := mutateFixture(t, subject, map[string]any{
		"Media": []map[string]any{{
			"MediaKey":                   "TSTMEDIA0001",
			"MediaURL":                   "https://media.example.test/TSTMEDIA0001.jpg",
			"Order":                      0,
			"MediaModificationTimestamp": "2026-05-20T08:00:00Z",
		}},
	})
	if _, err := s.UpsertProperties(ctx, []mlsgrid.Record{oneMedia}); err != nil {
		t.Fatal(err)
	}
	var mediaCount int
	var status, localPath string
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*), min(storage_status), min(local_path) FROM t_children.media
		WHERE listing_key = 'TST0000000001'`).Scan(&mediaCount, &status, &localPath); err != nil {
		t.Fatal(err)
	}
	if mediaCount != 1 {
		t.Errorf("orphan media not cleaned: count=%d", mediaCount)
	}
	if status != "downloaded" || localPath != "ab/TSTMEDIA0001" {
		t.Errorf("download state must survive re-upsert: status=%q path=%q", status, localPath)
	}

	// Empty Media array means "no expansion data" -> must NOT delete rows.
	noMedia := mutateFixture(t, subject, map[string]any{"Media": []any{}, "Rooms": []any{}})
	if _, err := s.UpsertProperties(ctx, []mlsgrid.Record{noMedia}); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM t_children.media WHERE listing_key = 'TST0000000001'`).Scan(&mediaCount); err != nil {
		t.Fatal(err)
	}
	if mediaCount != 1 {
		t.Errorf("empty incoming array must not orphan-delete, count=%d", mediaCount)
	}

	// Rooms are replaced when non-empty.
	newRooms := mutateFixture(t, subject, map[string]any{
		"Rooms": []map[string]any{
			{"RoomKey": "TSTROOM0002", "RoomType": "Kitchen", "RoomLevel": "First"},
			{"RoomKey": "TSTROOM0003", "RoomType": "Den", "RoomLevel": "First"},
		},
	})
	if _, err := s.UpsertProperties(ctx, []mlsgrid.Record{newRooms}); err != nil {
		t.Fatal(err)
	}
	var roomKeys []string
	rows, err := s.pool.Query(ctx, `
		SELECT room_key FROM t_children.room WHERE listing_key = 'TST0000000001' ORDER BY room_key`)
	if err != nil {
		t.Fatal(err)
	}
	var rk string
	if _, err := pgx.ForEachRow(rows, []any{&rk}, func() error {
		roomKeys = append(roomKeys, rk)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(roomKeys) != 2 || roomKeys[0] != "TSTROOM0002" {
		t.Errorf("rooms must be replaced per parent, got %v", roomKeys)
	}
}

func TestDeleteProperties(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_delete", Options{})
	recs := loadFixtureRecords(t, "property_page1.json")
	if _, err := s.UpsertProperties(ctx, recs); err != nil {
		t.Fatal(err)
	}

	n, err := s.DeleteProperties(ctx, []string{"TST0000000001", "TSTNOSUCHKEY"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("deleted %d rows, want 1 (nonexistent keys are not counted)", n)
	}

	var count int
	for _, table := range []string{"property", "media", "room"} {
		if err := s.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM t_delete.%s WHERE listing_key = 'TST0000000001'`, table)).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("%s rows survived delete: %d", table, count)
		}
	}

	// Events outlive the listing; the terminal one is delisted with the
	// last-known status.
	var typ string
	var oldValue *string
	if err := s.pool.QueryRow(ctx, `
		SELECT event_type, old_value FROM t_delete.listing_event
		WHERE listing_key = 'TST0000000001' ORDER BY id DESC LIMIT 1`).Scan(&typ, &oldValue); err != nil {
		t.Fatal(err)
	}
	if typ != "delisted" || oldValue == nil || *oldValue != "Active" {
		t.Errorf("terminal event = %s/%v, want delisted/Active", typ, oldValue)
	}

	if n, err := s.DeleteProperties(ctx, nil); err != nil || n != 0 {
		t.Errorf("empty key list: %d, %v", n, err)
	}
}

func TestAliasFedUpsert(t *testing.T) {
	ctx := context.Background()
	am, err := fieldscope.Builtin("mred")
	if err != nil {
		t.Fatal(err)
	}
	s := newTestStore(t, "t_alias", Options{Aliases: am})
	recs := loadFixtureRecords(t, "property_page1.json")
	legacy := mutateFixture(t, recs[0], map[string]any{
		"MRD_RENTAL_PROPERTY_TYPE": true, // pre-2025-04 spelling
	})
	if _, err := s.UpsertProperties(ctx, []mlsgrid.Record{legacy}); err != nil {
		t.Fatal(err)
	}
	var attached *bool
	var rawLegacy *string
	if err := s.pool.QueryRow(ctx, `
		SELECT property_attached_yn, raw->>'MRD_RENTAL_PROPERTY_TYPE'
		FROM t_alias.property WHERE listing_key = 'TST0000000001'`).Scan(&attached, &rawLegacy); err != nil {
		t.Fatal(err)
	}
	if attached == nil || !*attached {
		t.Errorf("legacy spelling must feed property_attached_yn, got %v", attached)
	}
	if rawLegacy == nil {
		t.Error("raw must preserve the key as received")
	}
}

func TestSkipsMalformedRecords(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_skip", Options{})
	// The quirks fixture records lack ListingId — structurally incomplete
	// for storage, so they count as skipped, not as a page failure.
	recs := loadFixtureRecords(t, "property_quirks.json")
	stats, err := s.UpsertProperties(ctx, recs)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Skipped != 2 || stats.Inserted != 0 {
		t.Errorf("stats = %+v, want all skipped", stats)
	}
}

func TestSyncStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_state", Options{})

	got, err := s.SyncState(ctx, "Property", "testmls")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("absent cursor must be nil (callers decide semantics), got %+v", got)
	}

	ts := time.Date(2026, 6, 1, 12, 0, 0, 123e6, time.UTC)
	url := "https://replay.example.test/Property?$skip=1000"
	if err := s.SetSyncState(ctx, store.SyncState{
		Resource:           "Property",
		OriginatingSystem:  "testmls",
		LastModificationTS: &ts,
		InProgressURL:      &url,
	}); err != nil {
		t.Fatal(err)
	}
	got, err = s.SyncState(ctx, "Property", "testmls")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || !got.LastModificationTS.Equal(ts) || *got.InProgressURL != url {
		t.Fatalf("round trip: %+v", got)
	}
	if got.BackfillCompletedAt != nil {
		t.Error("backfill_completed_at must stay NULL until backfill finishes")
	}

	// Backfill completes: URL clears, completion stamps.
	done := ts.Add(time.Hour)
	if err := s.SetSyncState(ctx, store.SyncState{
		Resource:            "Property",
		OriginatingSystem:   "testmls",
		LastModificationTS:  &ts,
		BackfillCompletedAt: &done,
	}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.SyncState(ctx, "Property", "testmls")
	if got.InProgressURL != nil || got.BackfillCompletedAt == nil {
		t.Errorf("update: %+v", got)
	}
}

func TestOpenHouses(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_oh", Options{})
	recs := loadFixtureRecords(t, "openhouse_page.json")

	stats, err := s.UpsertOpenHouses(ctx, recs)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Inserted != 1 {
		t.Errorf("stats = %+v", stats)
	}
	stats, err = s.UpsertOpenHouses(ctx, recs)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Updated != 1 || stats.Inserted != 0 {
		t.Errorf("re-upsert stats = %+v", stats)
	}

	var date time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT open_house_date FROM t_oh.open_house WHERE open_house_key = 'TSTOH00000001'`).Scan(&date); err != nil {
		t.Fatal(err)
	}
	if date.Format("2006-01-02") != "2026-06-07" {
		t.Errorf("open_house_date = %v", date)
	}

	n, err := s.DeleteOpenHouses(ctx, []string{"TSTOH00000001"})
	if err != nil || n != 1 {
		t.Errorf("delete: %d, %v", n, err)
	}
}

func TestCountAndListKeys(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_keys", Options{})
	recs := loadFixtureRecords(t, "property_page1.json")
	if _, err := s.UpsertProperties(ctx, recs); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertOpenHouses(ctx, loadFixtureRecords(t, "openhouse_page.json")); err != nil {
		t.Fatal(err)
	}

	if n, err := s.Count(ctx, "Property"); err != nil || n != 2 {
		t.Errorf("Count(Property) = %d, %v", n, err)
	}
	if n, err := s.Count(ctx, "OpenHouse"); err != nil || n != 1 {
		t.Errorf("Count(OpenHouse) = %d, %v", n, err)
	}
	if _, err := s.Count(ctx, "Nope"); err == nil {
		t.Error("unknown resource must error")
	}

	keys, err := s.ListKeys(ctx, "Property")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 6, 1, 12, 0, 0, 123e6, time.UTC)
	if len(keys) != 2 || !keys["TST0000000001"].Equal(want) {
		t.Errorf("ListKeys = %v", keys)
	}
	ohKeys, err := s.ListKeys(ctx, "OpenHouse")
	if err != nil || len(ohKeys) != 1 {
		t.Errorf("ListKeys(OpenHouse) = %v, %v", ohKeys, err)
	}
}

func TestRateBudgetRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, "t_budget", Options{})

	// Empty table: zero usage, no error.
	u, err := s.RateBudget(ctx)
	if err != nil || !u.HourStart.IsZero() {
		t.Fatalf("empty budget = %+v, %v", u, err)
	}

	// Windows must be current: SetRateBudget prunes anything older than 48h
	// (which is also why fixed past dates cannot be used here).
	hour := time.Now().UTC().Truncate(time.Hour)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	if err := s.SetRateBudget(ctx, ratelimit.Usage{
		HourStart: hour, HourRequests: 42, HourBytes: 1 << 20,
		DayStart: day, DayRequests: 99,
	}); err != nil {
		t.Fatal(err)
	}
	// Same window updated, not duplicated.
	if err := s.SetRateBudget(ctx, ratelimit.Usage{
		HourStart: hour, HourRequests: 50, HourBytes: 2 << 20,
		DayStart: day, DayRequests: 120,
	}); err != nil {
		t.Fatal(err)
	}

	u, err = s.RateBudget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if u.HourRequests != 50 || u.HourBytes != 2<<20 || u.DayRequests != 120 {
		t.Errorf("round trip = %+v", u)
	}
	if !u.HourStart.Equal(hour) || !u.DayStart.Equal(day) {
		t.Errorf("window starts = %v / %v", u.HourStart, u.DayStart)
	}

	var rows int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM t_budget.rate_budget`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Errorf("rate_budget rows = %d, want 2 (hour + day, upserted)", rows)
	}
}
