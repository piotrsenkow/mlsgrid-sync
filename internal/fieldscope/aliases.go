// Package fieldscope handles per-MLS field-name normalization and (from M8)
// field-scope presets. MLSs occasionally rename local fields to RESO names —
// MRED did so on 2025-04-02 — but only records created or modified after the
// rename carry the new keys, and MLS Grid instructs consumers not to re-pull
// older records. An AliasMap folds both spellings into the same core column;
// raw JSONB keeps whichever key actually arrived.
package fieldscope

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// AliasMap maps canonical (RESO) field names to the legacy spellings that may
// still appear on historical records. The zero value performs no aliasing.
type AliasMap struct {
	legacy map[string][]string
}

// NewAliasMap builds a map from canonical name → legacy spellings.
func NewAliasMap(legacy map[string][]string) *AliasMap {
	return &AliasMap{legacy: legacy}
}

// Candidates returns the source keys to try for a canonical field, in priority
// order: the canonical name first (newer records win), then legacy spellings.
func (m *AliasMap) Candidates(field string) []string {
	if m == nil || len(m.legacy[field]) == 0 {
		return []string{field}
	}
	return append([]string{field}, m.legacy[field]...)
}

// builtinMaps ships alias maps for MLSs with documented renames. The map for
// each system is specified in docs/schema-contract.md (Appendix A for mred);
// contributions for other systems are welcome via "MLS quirk report" issues.
var builtinMaps = map[string]map[string][]string{
	"mred": {
		// Renamed 2025-04-02 (old key applies to unmodified older records).
		"ActivationDate":     {"MRD_ACTV_DATE"},
		"BackOnMarketDate":   {"MRD_BMD"},
		"BodyType":           {"MRD_DBL"},
		"PropertyAttachedYN": {"MRD_RENTAL_PROPERTY_TYPE"},
		"EntryLevel":         {"MRD_UFL"},
		// Legacy fields merged into RESO multi-value fields.
		"Basement": {"MRD_BAS"},
		"ParkingFeatures": {
			"MRD_DRV", "MRD_GAR", "MRD_GARAGE_TYPE", "MRD_GARAGE_OWNERSHIP",
			"MRD_GARAGE_ONSITE", "MRD_PARKING_OWNERSHIP", "MRD_PARKING_ONSITE",
			"MRD_PKN",
		},
		// UnitType expansion floor number never got a RESO name.
		"FloorNumber": {"MRD_FloorNumber"},
	},
}

// Builtin returns the alias map shipped for the named originating system.
func Builtin(system string) (*AliasMap, error) {
	m, ok := builtinMaps[system]
	if !ok {
		return nil, fmt.Errorf("no builtin alias map for %q (available: mred); omit field_aliases or supply a custom map", system)
	}
	return NewAliasMap(m), nil
}

// Load resolves a profile's field_aliases spec: "" means no aliasing,
// "builtin:<system>" selects a shipped map, anything else is a path to a
// custom alias YAML:
//
//	aliases:
//	  ActivationDate: [MRD_ACTV_DATE]
//	  ParkingFeatures: [MRD_GAR, MRD_PKN]
func Load(spec string) (*AliasMap, error) {
	switch {
	case spec == "":
		return nil, nil
	case strings.HasPrefix(spec, "builtin:"):
		return Builtin(strings.TrimPrefix(spec, "builtin:"))
	}
	data, err := os.ReadFile(spec)
	if err != nil {
		return nil, fmt.Errorf("fieldscope: reading custom alias map %s: %w", spec, err)
	}
	var doc struct {
		Aliases map[string][]string `yaml:"aliases"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("fieldscope: parsing custom alias map %s: %w", spec, err)
	}
	if len(doc.Aliases) == 0 {
		return nil, fmt.Errorf("fieldscope: custom alias map %s has no aliases: mapping (canonical name -> legacy spellings)", spec)
	}
	return NewAliasMap(doc.Aliases), nil
}
