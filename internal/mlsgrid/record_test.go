package mlsgrid

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// loadFixture reads a page from testdata/odata, substituting {{BASE}}.
func loadFixture(t *testing.T, name, base string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "odata", name))
	if err != nil {
		t.Fatal(err)
	}
	return []byte(strings.ReplaceAll(string(b), "{{BASE}}", base))
}

func fixtureRecords(t *testing.T, name string) []Record {
	t.Helper()
	var env envelope
	if err := json.Unmarshal(loadFixture(t, name, "http://test.invalid"), &env); err != nil {
		t.Fatal(err)
	}
	recs := make([]Record, 0, len(env.Value))
	for _, raw := range env.Value {
		rec, err := ParseRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, rec)
	}
	return recs
}

func TestQuirkIntAsFloat(t *testing.T) {
	rec := fixtureRecords(t, "property_quirks.json")[0]
	if dom, ok := rec.Int("DaysOnMarket"); !ok || dom != 12 {
		t.Errorf("DaysOnMarket = %d, %v; want 12, true (feed sends 12.0)", dom, ok)
	}
	if y, ok := rec.Int("TaxYear"); !ok || y != 2024 {
		t.Errorf("TaxYear = %d, %v; want 2024, true", y, ok)
	}
}

func TestQuirkNaiveTimestampIsUTC(t *testing.T) {
	rec := fixtureRecords(t, "property_quirks.json")[0]
	ts, ok := rec.ModificationTimestamp()
	if !ok {
		t.Fatal("naive timestamp should parse")
	}
	want := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	if !ts.Equal(want) {
		t.Errorf("naive timestamp = %v, want %v (interpreted as UTC)", ts, want)
	}
}

func TestQuirkSubMillisecondPrecisionPreserved(t *testing.T) {
	rec := fixtureRecords(t, "property_quirks.json")[1]
	ts, ok := rec.ModificationTimestamp()
	if !ok || ts.Nanosecond() != 999_000_000 {
		t.Errorf("timestamp = %v (ok=%v), want .999 fraction preserved", ts, ok)
	}
}

func TestQuirkStringOrArray(t *testing.T) {
	recs := fixtureRecords(t, "property_quirks.json")
	if got := recs[0].StringList("Contingency"); len(got) != 2 || got[0] != "Attorney Review" {
		t.Errorf("array Contingency = %v", got)
	}
	if got := recs[1].StringList("Contingency"); len(got) != 1 || got[0] != "A/R" {
		t.Errorf("string Contingency = %v", got)
	}
	if got := recs[0].StringList("Possession"); len(got) != 1 || got[0] != "Closing" {
		t.Errorf("string Possession = %v", got)
	}
}

func TestQuirkFreeTextDate(t *testing.T) {
	recs := fixtureRecords(t, "property_quirks.json")
	if _, ok := recs[0].Date("CloseDate"); ok {
		t.Error(`CloseDate "TBD" must not parse; original text stays in Raw`)
	}
	if d, ok := recs[0].Date("OffMarketDate"); !ok || d.Day() != 30 {
		t.Errorf("OffMarketDate = %v, %v", d, ok)
	}
	if d, ok := recs[1].Date("CloseDate"); !ok || !d.Equal(time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("CloseDate = %v, %v", d, ok)
	}
}

func TestQuirkOldAndNewFieldNamesCoexist(t *testing.T) {
	recs := fixtureRecords(t, "property_quirks.json")
	// Pre-2025-04 record: old local keys.
	if !recs[0].Has("MRD_BAS") || !recs[0].Has("MRD_ACTV_DATE") {
		t.Error("historical record should carry old MRD_* keys")
	}
	// Post-2025-04 record: RESO keys.
	if !recs[1].Has("Basement") || !recs[1].Has("ActivationDate") {
		t.Error("modern record should carry RESO keys")
	}
}

func TestCanViewSemantics(t *testing.T) {
	recs := fixtureRecords(t, "property_deleted.json")
	if recs[0].CanView() {
		t.Error("MlgCanView=false record must report not viewable")
	}
	if !recs[1].CanView() {
		t.Error("MlgCanView=true record must report viewable")
	}
	// Absent field counts as viewable: treating absence as false would
	// delete the entire local dataset.
	rec, _ := ParseRecord([]byte(`{"ListingKey":"X"}`))
	if !rec.CanView() {
		t.Error("absent MlgCanView must count as viewable")
	}
}

func TestChildrenAndRaw(t *testing.T) {
	rec := fixtureRecords(t, "property_page1.json")[0]
	media := rec.Children("Media")
	if len(media) != 2 || media[0].String("MediaKey") != "TSTMEDIA0001" {
		t.Errorf("Media children = %d records", len(media))
	}
	if w, ok := media[0].Int("ImageWidth"); !ok || w != 1024 {
		t.Errorf("child accessor: ImageWidth = %d, %v", w, ok)
	}
	if rec.ListingKey() != "TST0000000001" {
		t.Errorf("ListingKey = %q", rec.ListingKey())
	}
	// Raw round-trips losslessly.
	var m map[string]any
	if err := json.Unmarshal(rec.Raw(), &m); err != nil {
		t.Fatalf("Raw not valid JSON: %v", err)
	}
	if m["PublicRemarks"] == "" {
		t.Error("Raw should preserve all fields")
	}
}
