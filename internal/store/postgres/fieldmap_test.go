package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/fieldscope"
	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
)

func mustRecord(t *testing.T, jsonStr string) mlsgrid.Record {
	t.Helper()
	rec, err := mlsgrid.ParseRecord([]byte(jsonStr))
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func argFor(t *testing.T, rec mlsgrid.Record, am *fieldscope.AliasMap, column string) any {
	t.Helper()
	for _, c := range propertyCols {
		if c.name == column {
			return extractValue(rec, am, c)
		}
	}
	t.Fatalf("no property column %q", column)
	return nil
}

func TestExtractQuirkValues(t *testing.T) {
	rec := mustRecord(t, `{
		"DaysOnMarket": 12.0,
		"ModificationTimestamp": "2026-06-01T10:00:00",
		"CloseDate": "TBD",
		"OffMarketDate": "2026-05-30",
		"MlgCanUse": ["IDX", "VOW"],
		"ListPrice": 425000
	}`)

	if got := argFor(t, rec, nil, "days_on_market"); got != 12 {
		t.Errorf("float-serialized integer: got %v (%T), want 12", got, got)
	}
	wantTS := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	if got := argFor(t, rec, nil, "modification_timestamp"); got != wantTS {
		t.Errorf("naive timestamp: got %v, want %v UTC", got, wantTS)
	}
	if got := argFor(t, rec, nil, "close_date"); got != nil {
		t.Errorf("free-text date must yield NULL, got %v", got)
	}
	wantDate := time.Date(2026, 5, 30, 0, 0, 0, 0, time.UTC)
	if got := argFor(t, rec, nil, "off_market_date"); got != wantDate {
		t.Errorf("parseable date: got %v, want %v", got, wantDate)
	}
	if got, ok := argFor(t, rec, nil, "mlg_can_use").([]string); !ok || len(got) != 2 {
		t.Errorf("mlg_can_use: got %v", got)
	}
	if got := argFor(t, rec, nil, "list_price"); got != float64(425000) {
		t.Errorf("list_price: got %v", got)
	}
	if got := argFor(t, rec, nil, "latitude"); got != nil {
		t.Errorf("absent field must be NULL, got %v", got)
	}
}

func TestExtractAliasFedColumn(t *testing.T) {
	am, err := fieldscope.Builtin("mred")
	if err != nil {
		t.Fatal(err)
	}
	// Historical record: only the pre-2025-04 spelling is present.
	old := mustRecord(t, `{"MRD_RENTAL_PROPERTY_TYPE": true}`)
	if got := argFor(t, old, am, "property_attached_yn"); got != true {
		t.Errorf("legacy spelling must feed the column, got %v", got)
	}
	// Post-rename record: canonical name wins even if both appear.
	both := mustRecord(t, `{"PropertyAttachedYN": false, "MRD_RENTAL_PROPERTY_TYPE": true}`)
	if got := argFor(t, both, am, "property_attached_yn"); got != false {
		t.Errorf("canonical name must win, got %v", got)
	}
	// Legacy value that does not coerce to the column type yields NULL —
	// the original stays in raw.
	junk := mustRecord(t, `{"MRD_RENTAL_PROPERTY_TYPE": "Attached Single"}`)
	if got := argFor(t, junk, am, "property_attached_yn"); got != nil {
		t.Errorf("non-coercible legacy value must yield NULL, got %v", got)
	}
}

func TestPropertyColumnsUniqueAndComplete(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range propertyCols {
		if seen[c.name] {
			t.Errorf("duplicate column %q in field map", c.name)
		}
		seen[c.name] = true
	}
	// The contract's core column count: propertyCols + listing_key + raw +
	// first_seen_at + updated_at. Guards against silently dropping an entry.
	if got := len(propertyCols); got != 74 {
		t.Errorf("propertyCols has %d entries, want 74 — update this if the contract version was bumped", got)
	}
}

func TestGeneratedSQLPlaceholderCounts(t *testing.T) {
	table := "\"mlsgrid\".\"property\""
	sql := propertyUpsertSQL(table)
	wantArgs := len(propertyCols) + 2 // listing_key + cols + raw
	if got := strings.Count(sql, "$"); got != wantArgs {
		t.Errorf("property upsert has %d placeholders, want %d", got, wantArgs)
	}
	if strings.Contains(sql, "EXCLUDED.listing_key") {
		t.Error("conflict key must not be in the update arm")
	}
	if !strings.Contains(sql, "updated_at = now()") {
		t.Error("upsert must touch updated_at")
	}

	media := mediaUpsertSQL(table)
	for _, frozen := range []string{"EXCLUDED.storage_status", "EXCLUDED.local_path", "EXCLUDED.failure_count"} {
		if strings.Contains(media, frozen) {
			t.Errorf("media upsert must not overwrite download state: found %s", frozen)
		}
	}
}
