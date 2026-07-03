package postgres

import (
	"fmt"
	"strings"

	"github.com/piotrsenkow/mlsgrid-sync/internal/fieldscope"
	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
)

// The field maps below are the single place feed fields are bound to columns:
// adding a core column means one entry here plus a migration (plus a contract
// version bump). Extraction goes through Record's quirk-tolerant accessors and
// the profile's alias map, so legacy spellings (see fieldscope) feed the same
// column as their RESO names.

type kind int

const (
	kText kind = iota
	kInt
	kNumeric
	kBool
	kTime
	kDate
	kTextArray
)

type col struct {
	name   string // column name (schema contract)
	source string // canonical feed field name
	k      kind
}

// extractValue returns the column value for a record, trying the canonical
// field name first, then legacy aliases. Absent or unparseable values yield
// nil (SQL NULL); the original text always survives inside raw.
func extractValue(rec mlsgrid.Record, am *fieldscope.AliasMap, c col) any {
	for _, key := range am.Candidates(c.source) {
		if !rec.Has(key) {
			continue
		}
		switch c.k {
		case kText:
			if s := rec.String(key); s != "" {
				return s
			}
		case kInt:
			if v, ok := rec.Int(key); ok {
				return v
			}
		case kNumeric:
			if v, ok := rec.Float(key); ok {
				return v
			}
		case kBool:
			if v, ok := rec.Bool(key); ok {
				return v
			}
		case kTime:
			if v, ok := rec.Time(key); ok {
				return v
			}
		case kDate:
			if v, ok := rec.Date(key); ok {
				return v
			}
		case kTextArray:
			if v := rec.StringList(key); len(v) > 0 {
				return v
			}
		}
	}
	return nil
}

// propertyCols covers every core column of the property table except the
// specially-handled listing_key, raw, and housekeeping timestamps. Order here
// defines placeholder order in the generated upsert.
var propertyCols = []col{
	{"listing_id", "ListingId", kText},
	{"originating_system_name", "OriginatingSystemName", kText},
	{"standard_status", "StandardStatus", kText},
	{"mls_status", "MlsStatus", kText},
	{"property_type", "PropertyType", kText},
	{"property_sub_type", "PropertySubType", kText},
	{"list_price", "ListPrice", kNumeric},
	{"original_list_price", "OriginalListPrice", kNumeric},
	{"previous_list_price", "PreviousListPrice", kNumeric},
	{"close_price", "ClosePrice", kNumeric},
	{"listing_contract_date", "ListingContractDate", kDate},
	{"purchase_contract_date", "PurchaseContractDate", kDate},
	{"close_date", "CloseDate", kDate},
	{"off_market_date", "OffMarketDate", kDate},
	{"days_on_market", "DaysOnMarket", kInt},
	{"cumulative_days_on_market", "CumulativeDaysOnMarket", kInt},
	{"street_number", "StreetNumber", kText},
	{"street_dir_prefix", "StreetDirPrefix", kText},
	{"street_name", "StreetName", kText},
	{"street_suffix", "StreetSuffix", kText},
	{"unit_number", "UnitNumber", kText},
	{"city", "City", kText},
	{"postal_code", "PostalCode", kText},
	{"county_or_parish", "CountyOrParish", kText},
	{"state_or_province", "StateOrProvince", kText},
	{"township", "Township", kText},
	{"subdivision_name", "SubdivisionName", kText},
	{"latitude", "Latitude", kNumeric},
	{"longitude", "Longitude", kNumeric},
	{"bedrooms_total", "BedroomsTotal", kInt},
	{"bathrooms_full", "BathroomsFull", kInt},
	{"bathrooms_half", "BathroomsHalf", kInt},
	{"rooms_total", "RoomsTotal", kInt},
	{"living_area", "LivingArea", kNumeric},
	{"building_area_total", "BuildingAreaTotal", kNumeric},
	{"lot_size_acres", "LotSizeAcres", kNumeric},
	{"lot_size_square_feet", "LotSizeSquareFeet", kNumeric},
	{"year_built", "YearBuilt", kInt},
	{"stories_total", "StoriesTotal", kNumeric},
	{"garage_spaces", "GarageSpaces", kNumeric},
	{"parking_total", "ParkingTotal", kInt},
	{"number_of_units_total", "NumberOfUnitsTotal", kInt},
	{"new_construction_yn", "NewConstructionYN", kBool},
	{"property_attached_yn", "PropertyAttachedYN", kBool},
	{"waterfront_yn", "WaterfrontYN", kBool},
	{"public_remarks", "PublicRemarks", kText},
	{"virtual_tour_url", "VirtualTourURLUnbranded", kText},
	{"internet_address_display_yn", "InternetAddressDisplayYN", kBool},
	{"internet_entire_listing_display_yn", "InternetEntireListingDisplayYN", kBool},
	{"elementary_school", "ElementarySchool", kText},
	{"middle_or_junior_school", "MiddleOrJuniorSchool", kText},
	{"high_school", "HighSchool", kText},
	{"elementary_school_district", "ElementarySchoolDistrict", kText},
	{"middle_or_junior_school_district", "MiddleOrJuniorSchoolDistrict", kText},
	{"high_school_district", "HighSchoolDistrict", kText},
	{"list_agent_full_name", "ListAgentFullName", kText},
	{"list_agent_mls_id", "ListAgentMlsId", kText},
	{"list_agent_key", "ListAgentKey", kText},
	{"list_office_name", "ListOfficeName", kText},
	{"list_office_mls_id", "ListOfficeMlsId", kText},
	{"buyer_agent_full_name", "BuyerAgentFullName", kText},
	{"buyer_agent_mls_id", "BuyerAgentMlsId", kText},
	{"buyer_office_name", "BuyerOfficeName", kText},
	{"tax_annual_amount", "TaxAnnualAmount", kNumeric},
	{"tax_year", "TaxYear", kInt},
	{"association_fee", "AssociationFee", kNumeric},
	{"association_fee_frequency", "AssociationFeeFrequency", kText},
	{"parcel_number", "ParcelNumber", kText},
	{"modification_timestamp", "ModificationTimestamp", kTime},
	{"original_entry_timestamp", "OriginalEntryTimestamp", kTime},
	{"status_change_timestamp", "StatusChangeTimestamp", kTime},
	{"photos_change_timestamp", "PhotosChangeTimestamp", kTime},
	{"photos_count", "PhotosCount", kInt},
	{"mlg_can_use", "MlgCanUse", kTextArray},
}

var roomCols = []col{
	{"room_type", "RoomType", kText},
	{"room_level", "RoomLevel", kText},
	{"room_dimensions", "RoomDimensions", kText},
}

var unitTypeCols = []col{
	{"unit_number", "UnitTypeType", kText},
	{"floor_number", "FloorNumber", kText}, // alias-fed on some MLSs
	{"beds_total", "UnitTypeBedsTotal", kInt},
	{"baths_total", "UnitTypeBathsTotal", kInt},
	{"actual_rent", "UnitTypeActualRent", kNumeric},
}

var mediaCols = []col{
	{"media_url", "MediaURL", kText},
	{"display_order", "Order", kInt},
	{"caption", "LongDescription", kText},
	{"image_width", "ImageWidth", kInt},
	{"image_height", "ImageHeight", kInt},
	{"permission", "Permission", kTextArray},
	{"media_modification_timestamp", "MediaModificationTimestamp", kTime},
}

var openHouseCols = []col{
	{"listing_key", "ListingKey", kText},
	{"listing_id", "ListingId", kText},
	{"open_house_date", "OpenHouseDate", kDate},
	{"start_time", "OpenHouseStartTime", kTime},
	{"end_time", "OpenHouseEndTime", kTime},
	{"remarks", "OpenHouseRemarks", kText},
	{"modification_timestamp", "ModificationTimestamp", kTime},
}

func colNames(cols []col) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}
	return names
}

func extractArgs(rec mlsgrid.Record, am *fieldscope.AliasMap, cols []col) []any {
	args := make([]any, len(cols))
	for i, c := range cols {
		args[i] = extractValue(rec, am, c)
	}
	return args
}

// placeholders renders "$from, $from+1, ... $to".
func placeholders(from, to int) string {
	parts := make([]string, 0, to-from+1)
	for i := from; i <= to; i++ {
		parts = append(parts, fmt.Sprintf("$%d", i))
	}
	return strings.Join(parts, ", ")
}

// excludedSet renders "c1 = EXCLUDED.c1, ..." for the update arm of an upsert.
func excludedSet(names []string) string {
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = fmt.Sprintf("%s = EXCLUDED.%s", n, n)
	}
	return strings.Join(parts, ", ")
}

func propertyUpsertSQL(table string) string {
	names := append([]string{"listing_key"}, colNames(propertyCols)...)
	names = append(names, "raw")
	return fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
		 ON CONFLICT (listing_key) DO UPDATE SET %s, updated_at = now()`,
		table, strings.Join(names, ", "), placeholders(1, len(names)),
		excludedSet(names[1:]))
}

func roomInsertSQL(table string) string {
	names := append([]string{"listing_key", "room_key"}, colNames(roomCols)...)
	names = append(names, "raw")
	// Plain insert: the parent's rooms were deleted in this transaction.
	// DO NOTHING guards against a duplicated RoomKey within one record.
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		table, strings.Join(names, ", "), placeholders(1, len(names)))
}

func unitTypeInsertSQL(table string) string {
	names := append([]string{"listing_key", "unit_type_key"}, colNames(unitTypeCols)...)
	names = append(names, "raw")
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		table, strings.Join(names, ", "), placeholders(1, len(names)))
}

func mediaUpsertSQL(table string) string {
	names := append([]string{"media_key", "listing_key"}, colNames(mediaCols)...)
	names = append(names, "storage_status")
	// storage_status, local_path, and failure_count are download-pipeline
	// state and deliberately NOT overwritten on conflict: MediaKeys are
	// immutable upstream, so a downloaded key stays downloaded.
	updatable := names[1 : len(names)-1]
	return fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
		 ON CONFLICT (media_key) DO UPDATE SET %s, updated_at = now()`,
		table, strings.Join(names, ", "), placeholders(1, len(names)),
		excludedSet(updatable))
}

func openHouseUpsertSQL(table string) string {
	names := append([]string{"open_house_key"}, colNames(openHouseCols)...)
	names = append(names, "raw")
	return fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
		 ON CONFLICT (open_house_key) DO UPDATE SET %s, updated_at = now()`,
		table, strings.Join(names, ", "), placeholders(1, len(names)),
		excludedSet(names[1:]))
}
