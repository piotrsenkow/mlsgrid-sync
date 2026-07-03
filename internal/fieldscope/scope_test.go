package fieldscope

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// sampleRecord mixes core-backed fields, standard-tail fields, analytics
// fields, vendor junk, children, and OData noise.
var sampleRecord = []byte(`{
	"ListingKey": "TST1",
	"ListPrice": 500000,
	"PublicRemarks": "Sunny corner unit",
	"ArchitecturalStyle": ["Bungalow"],
	"TaxAnnualAmount": 8000,
	"GrossIncome": 42000,
	"MRD_BAS": "Full",
	"SomeVendorInternal": "x",
	"@odata.id": "Property('TST1')",
	"Media": [{"MediaKey": "M1"}],
	"Rooms": [{"RoomKey": "R1"}],
	"UnitTypes": []
}`)

func filtered(t *testing.T, s *Scope) map[string]json.RawMessage {
	t.Helper()
	raw, err := s.FilterRaw(sampleRecord)
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		return nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func mustScope(t *testing.T, spec string) *Scope {
	t.Helper()
	s, err := LoadScope(spec)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestScopePresets(t *testing.T) {
	if s := mustScope(t, ""); s.Name() != "standard" {
		t.Errorf("empty spec must default to standard, got %s", s.Name())
	}

	if got := filtered(t, mustScope(t, "minimal")); got != nil {
		t.Errorf("minimal must store no raw, got %v", got)
	}

	full := filtered(t, mustScope(t, "full"))
	if _, ok := full["SomeVendorInternal"]; !ok {
		t.Error("full must keep everything")
	}
	if _, ok := full["Media"]; !ok {
		t.Error("full keeps the record byte-for-byte as received, children included")
	}

	std := filtered(t, mustScope(t, "standard"))
	for _, want := range []string{"PublicRemarks", "ArchitecturalStyle", "ListPrice"} {
		if _, ok := std[want]; !ok {
			t.Errorf("standard must keep %s", want)
		}
	}
	for _, drop := range []string{"SomeVendorInternal", "MRD_BAS", "Media", "Rooms", "UnitTypes", "@odata.id"} {
		if _, ok := std[drop]; ok {
			t.Errorf("standard must drop %s", drop)
		}
	}

	an := filtered(t, mustScope(t, "analytics"))
	for _, want := range []string{"TaxAnnualAmount", "GrossIncome", "MRD_BAS", "PublicRemarks"} {
		if _, ok := an[want]; !ok {
			t.Errorf("analytics must keep %s", want)
		}
	}
	if _, ok := an["SomeVendorInternal"]; ok {
		t.Error("analytics must still drop unmatched vendor fields")
	}
}

func TestScopeCustomYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scope.yaml")
	if err := os.WriteFile(path, []byte("include: [\"Tax*\", \"ListingKey\"]\nexclude: [\"TaxAnnualAmount\"]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := filtered(t, mustScope(t, path))
	if _, ok := got["ListingKey"]; !ok {
		t.Error("custom include must keep ListingKey")
	}
	if _, ok := got["TaxAnnualAmount"]; ok {
		t.Error("exclude must beat include")
	}
	if _, ok := got["PublicRemarks"]; ok {
		t.Error("a non-empty include list keeps only what it matches")
	}
}

func TestScopeCustomExcludeOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scope.yaml")
	if err := os.WriteFile(path, []byte("exclude: [\"*Remarks\"]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := filtered(t, mustScope(t, path))
	if _, ok := got["PublicRemarks"]; ok {
		t.Error("excluded field survived")
	}
	if _, ok := got["SomeVendorInternal"]; !ok {
		t.Error("empty include means keep-everything-not-excluded")
	}
	if _, ok := got["Media"]; ok {
		t.Error("children never survive a filtering scope")
	}
}

func TestScopeCustomErrors(t *testing.T) {
	if _, err := LoadScope(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Error("missing custom scope file must error")
	}
	empty := filepath.Join(t.TempDir(), "empty.yaml")
	if err := os.WriteFile(empty, []byte("# nothing\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScope(empty); err == nil {
		t.Error("a scope with no globs is a config mistake, not silence")
	}
	bad := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(bad, []byte("include: [\"[\"]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScope(bad); err == nil {
		t.Error("malformed globs must fail at load time, not silently never match")
	}
}

func TestScopeExpandsAndSelect(t *testing.T) {
	min := mustScope(t, "minimal")
	if got := min.Expands(); len(got) != 1 || got[0] != "Media" {
		t.Errorf("minimal expands = %v, want Media only (media table is core contract)", got)
	}
	if !min.NarrowSelect() {
		t.Error("minimal must narrow requests with $select")
	}
	std := mustScope(t, "standard")
	if got := std.Expands(); len(got) != 3 {
		t.Errorf("standard expands = %v", got)
	}
	if std.NarrowSelect() {
		t.Error("only minimal narrows $select — other scopes need the whole record for raw")
	}
	var nilScope *Scope
	if nilScope.NarrowSelect() || len(nilScope.Expands()) != 3 || nilScope.Name() != "full" {
		t.Error("nil scope must behave as full")
	}
}

func TestCustomAliasFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aliases.yaml")
	if err := os.WriteFile(path, []byte("aliases:\n  ActivationDate: [XX_ACTV]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	got := m.Candidates("ActivationDate")
	if len(got) != 2 || got[0] != "ActivationDate" || got[1] != "XX_ACTV" {
		t.Errorf("Candidates = %v, want canonical first then legacy", got)
	}

	empty := filepath.Join(t.TempDir(), "empty.yaml")
	if err := os.WriteFile(empty, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(empty); err == nil {
		t.Error("an alias file without an aliases: mapping must error")
	}
}
