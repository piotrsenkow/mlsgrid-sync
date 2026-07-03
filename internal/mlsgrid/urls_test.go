package mlsgrid

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestBackfillURL(t *testing.T) {
	got, err := Query{
		Resource:          "Property",
		OriginatingSystem: "testmls",
		MlgCanViewTrue:    true,
		Expand:            []string{"Media", "Rooms", "UnitTypes"},
		Top:               1000,
	}.URL("https://example.test/v2")
	if err != nil {
		t.Fatal(err)
	}
	want := "https://example.test/v2/Property?%24expand=Media%2CRooms%2CUnitTypes&%24filter=OriginatingSystemName%20eq%20%27testmls%27%20and%20MlgCanView%20eq%20true&%24top=1000"
	if got != want {
		t.Errorf("backfill URL:\n got %s\nwant %s", got, want)
	}
}

func TestIncrementalURLUsesGEAndOmitsCanView(t *testing.T) {
	ts := time.Date(2026, 6, 1, 12, 0, 0, 123_000_000, time.UTC)
	got, err := Query{
		Resource:                "Property",
		OriginatingSystem:       "testmls",
		ModificationTimestampGE: &ts,
		Expand:                  []string{"Media"},
	}.URL(DefaultBaseURL)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := url.QueryUnescape(got)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(decoded, "ModificationTimestamp ge 2026-06-01T12:00:00.123Z") {
		t.Errorf("want ge comparison with ms precision, got %s", decoded)
	}
	if strings.Contains(decoded, "MlgCanView") {
		t.Errorf("incremental URL must not filter MlgCanView (deletes would never arrive): %s", decoded)
	}
}

func TestReconcileSelectURL(t *testing.T) {
	got, err := Query{
		Resource:          "Property",
		OriginatingSystem: "testmls",
		MlgCanViewTrue:    true,
		Select:            []string{"ListingKey", "ModificationTimestamp"},
		Top:               5000,
	}.URL(DefaultBaseURL)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _ := url.QueryUnescape(got)
	if !strings.Contains(decoded, "$select=ListingKey,ModificationTimestamp") {
		t.Errorf("want $select, got %s", decoded)
	}
}

func TestQueryValidation(t *testing.T) {
	if _, err := (Query{Resource: "Property"}).URL(DefaultBaseURL); err == nil {
		t.Error("missing originating system must error — every request filters exactly one")
	}
	if _, err := (Query{OriginatingSystem: "testmls"}).URL(DefaultBaseURL); err == nil {
		t.Error("missing resource must error")
	}
}

func TestQueryKeyFilter(t *testing.T) {
	u, err := Query{
		Resource:          "Property",
		OriginatingSystem: "testmls",
		KeyField:          "ListingKey",
		Keys:              []string{"TST1", "TST2"},
	}.URL("https://replay.example.test/v2")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := url.QueryUnescape(u)
	if err != nil {
		t.Fatal(err)
	}
	want := "(ListingKey eq 'TST1' or ListingKey eq 'TST2')"
	if !strings.Contains(decoded, want) {
		t.Errorf("URL %q missing key filter %q", decoded, want)
	}

	if _, err := (Query{
		Resource:          "Property",
		OriginatingSystem: "testmls",
		Keys:              []string{"TST1"},
	}).URL("https://replay.example.test/v2"); err == nil {
		t.Error("Keys without KeyField must error")
	}
}
