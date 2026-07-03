package fieldscope

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
)

// Scope controls how much of each Property record is retained beyond the
// core columns (which are always populated): what survives into the row's
// raw JSONB, which child expansions are requested, and whether requests are
// narrowed with $select. Child-table and open-house raw columns are not
// scope-filtered (see docs/schema-contract.md) — they are small and stay
// lossless.
type Scope struct {
	name     string
	keepAll  bool
	keepNone bool
	// include/exclude are path.Match globs over feed field names. A field
	// survives when it matches any include glob (an empty include list means
	// "everything") and no exclude glob.
	include []string
	exclude []string
}

// Name returns the preset name or the custom file path.
func (s *Scope) Name() string {
	if s == nil {
		return "full"
	}
	return s.name
}

// standardInclude is the default keep-list: the fields people actually query
// on listings, grouped by concern. Everything else — vendor internals,
// odd locals, fields nobody has asked for — is the long tail that bloats
// raw across millions of rows.
var standardInclude = []string{
	// Remarks, showing, disclosure
	"*Remarks", "ShowingInstructions", "Disclosures", "Exclusions", "Inclusions",
	// Participants
	"ListAgent*", "ListOffice*", "CoListAgent*", "CoListOffice*", "ListTeam*",
	"BuyerAgent*", "BuyerOffice*", "CoBuyerAgent*", "CoBuyerOffice*",
	// Lifecycle: every date, timestamp, and price the feed carries
	"*Date", "*Timestamp", "*Price*", "MlsStatus", "DaysOnMarket", "CumulativeDaysOnMarket",
	// Structure
	"Architectural*", "Construction*", "Roof*", "Foundation*", "Basement*",
	"Levels", "Stories*", "YearBuilt*", "PropertyCondition", "NewConstruction*",
	"Builder*", "Model", "StructureType", "Ownership*", "CommonInterest",
	// Interior
	"Interior*", "Appliances", "Flooring", "Fireplace*", "Rooms*", "Bedrooms*",
	"Bathrooms*", "Laundry*", "Window*", "DoorFeatures", "SecurityFeatures",
	"Accessibility*", "OtherEquipment", "MainLevel*", "UpperLevel*", "LowerLevel*",
	// Exterior, lot, land
	"Exterior*", "PatioAndPorch*", "Lot*", "Frontage*", "Waterfront*", "View*",
	"Fencing", "OtherStructures", "Horse*", "RoadSurfaceType", "RoadFrontageType",
	"WaterBodyName", "Township",
	// Parking
	"Garage*", "Carport*", "Parking*", "CoveredSpaces", "OpenParking*",
	// Systems and utilities
	"Heating*", "Cooling*", "WaterSource", "Sewer", "Electric*", "Gas",
	"Utilities", "Green*",
	// HOA and deal terms
	"Association*", "Pets*", "SpecialListingConditions", "ListingTerms",
	"Possession", "Contingency", "HomeWarrantyYN", "LandLease*",
	// Schools and area
	"ElementarySchool*", "MiddleOrJuniorSchool*", "HighSchool*", "SchoolDistrict*",
	"Directions", "CrossStreet", "MLSArea*", "Zoning*", "CommunityFeatures",
	// Rental and occupancy
	"Lease*", "Rent*", "Availability*", "Furnished", "OccupantType",
	"CurrentUse", "PossibleUse",
	// Marketing counts
	"VirtualTour*", "PhotosCount", "DocumentsCount",
}

// analyticsInclude adds the investment math: tax, income, expenses, and the
// MLS-local (e.g. MRD_*) fields those tend to hide in.
var analyticsInclude = append(append([]string{}, standardInclude...),
	"Tax*", "MRD_*", "*Income*", "*Expense*", "NetOperating*", "CapRate",
	"NumberOfUnits*", "TenantPays", "OwnerPays", "Concessions*",
	"ExistingLeaseType", "AdditionalParcels*", "TotalActualRent",
	"FinancialDataSource", "UnitsFurnished",
)

// expandedChildren are relational here (room/unit_type/media tables), so a
// filtering scope never duplicates them inside property.raw. The full scope
// keeps the record byte-for-byte as received, arrays included.
var expandedChildren = map[string]bool{"Media": true, "Rooms": true, "UnitTypes": true}

// LoadScope resolves a profile's field_scope spec: a preset name (empty
// means standard) or a path to a custom YAML with include/exclude globs.
func LoadScope(spec string) (*Scope, error) {
	switch spec {
	case "", "standard":
		return &Scope{name: "standard", include: standardInclude}, nil
	case "minimal":
		return &Scope{name: "minimal", keepNone: true}, nil
	case "analytics":
		return &Scope{name: "analytics", include: analyticsInclude}, nil
	case "full":
		return &Scope{name: "full", keepAll: true}, nil
	}
	data, err := os.ReadFile(spec)
	if err != nil {
		return nil, fmt.Errorf("fieldscope: reading custom scope %s: %w", spec, err)
	}
	var doc struct {
		Include []string `yaml:"include"`
		Exclude []string `yaml:"exclude"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("fieldscope: parsing custom scope %s: %w", spec, err)
	}
	if len(doc.Include) == 0 && len(doc.Exclude) == 0 {
		return nil, fmt.Errorf("fieldscope: custom scope %s defines no include or exclude globs", spec)
	}
	for _, g := range append(append([]string{}, doc.Include...), doc.Exclude...) {
		if _, err := path.Match(g, "probe"); err != nil {
			return nil, fmt.Errorf("fieldscope: bad glob %q in %s: %w", g, spec, err)
		}
	}
	return &Scope{name: spec, include: doc.Include, exclude: doc.Exclude}, nil
}

// FilterRaw returns the subset of a Property record that the scope retains
// for the raw column, or nil when nothing is retained. A nil Scope keeps
// everything (full).
func (s *Scope) FilterRaw(raw json.RawMessage) (json.RawMessage, error) {
	if s == nil || s.keepAll {
		return raw, nil
	}
	if s.keepNone {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("fieldscope: filtering record: %w", err)
	}
	out := make(map[string]json.RawMessage, len(fields))
	for k, v := range fields {
		if expandedChildren[k] || strings.HasPrefix(k, "@odata") {
			continue
		}
		if s.retains(k) {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return json.Marshal(out)
}

func (s *Scope) retains(field string) bool {
	if len(s.include) > 0 && !matchAny(s.include, field) {
		return false
	}
	return !matchAny(s.exclude, field)
}

func matchAny(globs []string, field string) bool {
	for _, g := range globs {
		// Errors are impossible: globs are validated at load time and field
		// names contain no separators.
		if ok, _ := path.Match(g, field); ok {
			return true
		}
	}
	return false
}

// Expands returns the child expansions worth requesting under this scope.
// minimal skips Rooms/UnitTypes — their tables stay empty — but always keeps
// Media: the media table is part of the core contract (and feeds download
// mode).
func (s *Scope) Expands() []string {
	if s != nil && s.keepNone {
		return []string{"Media"}
	}
	return []string{"Media", "Rooms", "UnitTypes"}
}

// NarrowSelect reports whether Property requests should be narrowed with
// $select to just the core-column fields. Only minimal qualifies: its raw is
// NULL anyway, so fetching the long tail is pure wasted bandwidth against
// the hourly byte budget.
func (s *Scope) NarrowSelect() bool {
	return s != nil && s.keepNone
}
