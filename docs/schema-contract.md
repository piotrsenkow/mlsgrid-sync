# mlsgrid-sync Schema Contract

**Contract version: 1.0.0** (pre-release draft until mlsgrid-sync v0.1.0 ships)

This document is the authoritative specification of the database schema that `mlsgrid-sync` produces. It exists so that downstream consumers — most importantly [`mlsgrid-mcp`](https://github.com/piotrsenkow/mlsgrid-mcp) — can code against a stable, versioned contract instead of reverse-engineering DDL. The Postgres migrations in `internal/store/postgres/migrations/` implement this contract; a golden-schema test keeps them honest.

## Versioning rules

- Semantic versioning. **Additive** changes (new nullable column, new index, new table) bump **minor**. Column renames, type changes, semantic changes to existing columns, or table removals bump **major**.
- The live contract version is stored in the `schema_meta` table (`key = 'contract_version'`). Consumers (e.g. the mlsgrid-mcp Postgres adapter) MUST read it at startup and fail loudly on a major-version mismatch.
- Any change to this document requires a version bump in the same commit, plus a migration, plus an update to the golden-schema test fixture.

## Conventions

- All objects live in one Postgres schema, default **`mlsgrid`** (configurable; consumers must not hardcode it).
- Column names are `snake_case` renderings of RESO Data Dictionary 2.0 field names (`ListPrice` → `list_price`). Where an MLS ships a local alias for a RESO concept (see [MRED alias map](#appendix-a--mred-field-alias-map)), the RESO name wins for core columns; the original key is preserved in `raw`.
- All timestamps are `timestamptz`, stored in UTC. MLS Grid emits Data Dictionary date fields in UTC already; local (non-DD) date fields pass through as received inside `raw`.
- `ModificationTimestamp` is **Grid-set** (when MLS Grid processed the record), not the source-MLS edit time. The source time, when present, is `OriginatingSystemModificationTimestamp` inside `raw`. All sync cursors use the Grid-set field.
- Money is `numeric` in whole dollars (MLS feeds do not carry cents for prices).
- Multi-value RESO fields (features arrays, etc.) are **not** promoted to array columns except `mlg_can_use`; they live in `raw`. This keeps the core column set portable to future non-Postgres stores.
- Dates that MRED transmits as free text (`CloseDate`, `OffMarketDate`, …) are parsed into typed `date` columns; unparseable values yield `NULL` with the original string still available in `raw`.
- `raw` is `jsonb` holding the record **as received** (keys verbatim, values un-coerced), filtered by the configured field-scope preset. Consumers must tolerate both pre- and post-2025-04 MRED key names in `raw` for historical rows (see Appendix A).

## Tables

### `property`

Primary key: `listing_key` (MLS Grid's cross-MLS unique key, e.g. `MRD12345678`). `listing_id` is the human-facing MLS number and is unique only per originating system.

Core columns (always populated when the feed provides them, regardless of field-scope preset):

| Column | Type | Null | RESO / source field | Notes |
|---|---|---|---|---|
| `listing_key` | text | PK | ListingKey | |
| `listing_id` | text | not null | ListingId | indexed |
| `originating_system_name` | text | not null | OriginatingSystemName | e.g. `mred` |
| `standard_status` | text | yes | StandardStatus | RESO enum as delivered |
| `mls_status` | text | yes | MlsStatus | MLS-local status label |
| `property_type` | text | yes | PropertyType | |
| `property_sub_type` | text | yes | PropertySubType | |
| `list_price` | numeric | yes | ListPrice | |
| `original_list_price` | numeric | yes | OriginalListPrice | |
| `previous_list_price` | numeric | yes | PreviousListPrice | |
| `close_price` | numeric | yes | ClosePrice | |
| `listing_contract_date` | date | yes | ListingContractDate | |
| `purchase_contract_date` | date | yes | PurchaseContractDate | |
| `close_date` | date | yes | CloseDate | parsed from free text |
| `off_market_date` | date | yes | OffMarketDate | parsed from free text |
| `days_on_market` | integer | yes | DaysOnMarket | |
| `cumulative_days_on_market` | integer | yes | CumulativeDaysOnMarket | |
| `street_number` | text | yes | StreetNumber | |
| `street_dir_prefix` | text | yes | StreetDirPrefix | |
| `street_name` | text | yes | StreetName | |
| `street_suffix` | text | yes | StreetSuffix | |
| `unit_number` | text | yes | UnitNumber | |
| `city` | text | yes | City | |
| `postal_code` | text | yes | PostalCode | |
| `county_or_parish` | text | yes | CountyOrParish | |
| `state_or_province` | text | yes | StateOrProvince | |
| `township` | text | yes | Township | |
| `subdivision_name` | text | yes | SubdivisionName | |
| `latitude` | double precision | yes | Latitude | ⚠ many MLSs (incl. MRED) omit coordinates; consumers must handle NULL |
| `longitude` | double precision | yes | Longitude | ⚠ same |
| `bedrooms_total` | integer | yes | BedroomsTotal | |
| `bathrooms_full` | integer | yes | BathroomsFull | |
| `bathrooms_half` | integer | yes | BathroomsHalf | |
| `rooms_total` | integer | yes | RoomsTotal | |
| `living_area` | numeric | yes | LivingArea | sqft |
| `building_area_total` | numeric | yes | BuildingAreaTotal | |
| `lot_size_acres` | numeric | yes | LotSizeAcres | |
| `lot_size_square_feet` | numeric | yes | LotSizeSquareFeet | |
| `year_built` | integer | yes | YearBuilt | |
| `stories_total` | numeric | yes | StoriesTotal | |
| `garage_spaces` | numeric | yes | GarageSpaces | |
| `parking_total` | integer | yes | ParkingTotal | |
| `number_of_units_total` | integer | yes | NumberOfUnitsTotal | |
| `new_construction_yn` | boolean | yes | NewConstructionYN | |
| `property_attached_yn` | boolean | yes | PropertyAttachedYN | post-2025-04 MRED name; alias-fed |
| `waterfront_yn` | boolean | yes | WaterfrontYN | |
| `public_remarks` | text | yes | PublicRemarks | |
| `virtual_tour_url` | text | yes | VirtualTourURLUnbranded | |
| `internet_address_display_yn` | boolean | yes | InternetAddressDisplayYN | display-compliance flag |
| `internet_entire_listing_display_yn` | boolean | yes | InternetEntireListingDisplayYN | display-compliance flag |
| `elementary_school` | text | yes | ElementarySchool | |
| `middle_or_junior_school` | text | yes | MiddleOrJuniorSchool | |
| `high_school` | text | yes | HighSchool | |
| `elementary_school_district` | text | yes | ElementarySchoolDistrict | |
| `middle_or_junior_school_district` | text | yes | MiddleOrJuniorSchoolDistrict | |
| `high_school_district` | text | yes | HighSchoolDistrict | |
| `list_agent_full_name` | text | yes | ListAgentFullName | attribution for display rules |
| `list_agent_mls_id` | text | yes | ListAgentMlsId | |
| `list_agent_key` | text | yes | ListAgentKey | |
| `list_office_name` | text | yes | ListOfficeName | attribution for display rules |
| `list_office_mls_id` | text | yes | ListOfficeMlsId | |
| `buyer_agent_full_name` | text | yes | BuyerAgentFullName | |
| `buyer_agent_mls_id` | text | yes | BuyerAgentMlsId | |
| `buyer_office_name` | text | yes | BuyerOfficeName | |
| `tax_annual_amount` | numeric | yes | TaxAnnualAmount | |
| `tax_year` | integer | yes | TaxYear | |
| `association_fee` | numeric | yes | AssociationFee | |
| `association_fee_frequency` | text | yes | AssociationFeeFrequency | |
| `parcel_number` | text | yes | ParcelNumber | |
| `modification_timestamp` | timestamptz | not null | ModificationTimestamp | Grid-set; sync cursor field; indexed |
| `original_entry_timestamp` | timestamptz | yes | OriginalEntryTimestamp | |
| `status_change_timestamp` | timestamptz | yes | StatusChangeTimestamp | |
| `photos_change_timestamp` | timestamptz | yes | PhotosChangeTimestamp | drives media reconciliation |
| `photos_count` | integer | yes | PhotosCount | |
| `mlg_can_use` | text[] | yes | MlgCanUse | license tags: IDX / VOW / BO |
| `raw` | jsonb | yes | (entire record) | scope-filtered; NULL under `minimal` preset |
| `first_seen_at` | timestamptz | not null | — | row housekeeping, default now() |
| `updated_at` | timestamptz | not null | — | row housekeeping, set on upsert |

Notes:
- `MlgCanView` is deliberately **absent**: rows with `MlgCanView=false` are deleted, so every stored row is implicitly viewable (see [Delete semantics](#cursor--delete-semantics)).
- Investment/expense fields (`CapRate`, `GrossIncome`, operating expenses…), feature arrays, co-list/team fields, and all `MRD_*` locals live in `raw` (retained per field-scope preset). Promoting any of them to a core column later is a **minor** contract bump.

### `room`

| Column | Type | Null | Source |
|---|---|---|---|
| `listing_key` | text | PK part, FK → property (cascade delete) | parent |
| `room_key` | text | PK part | RoomKey |
| `room_type` | text | yes | RoomType |
| `room_level` | text | yes | RoomLevel |
| `room_dimensions` | text | yes | RoomDimensions |
| `raw` | jsonb | yes | full expanded object |

Rooms are **replaced per parent** on each property upsert (delete-then-insert within the same transaction). Child rows carry no delete flags in the feed; absence from a re-fetched parent means deletion — but replacement only runs when the incoming expansion is non-empty.

### `unit_type`

| Column | Type | Null | Source |
|---|---|---|---|
| `listing_key` | text | PK part, FK → property (cascade delete) | parent |
| `unit_type_key` | text | PK part | UnitTypeKey |
| `unit_number` | text | yes | UnitTypeType |
| `floor_number` | text | yes | (local, e.g. MRD_FloorNumber) |
| `beds_total` | integer | yes | UnitTypeBedsTotal |
| `baths_total` | integer | yes | UnitTypeBathsTotal |
| `actual_rent` | numeric | yes | UnitTypeActualRent |
| `raw` | jsonb | yes | full expanded object |

Same replace-per-parent semantics as `room`.

### `media`

| Column | Type | Null | Source / meaning |
|---|---|---|---|
| `media_key` | text | PK | MediaKey — **immutable**: a changed image arrives as a NEW key |
| `listing_key` | text | not null, FK → property (cascade delete), indexed | ResourceRecordID / parent |
| `media_url` | text | yes | MediaURL — download-only per MLS Grid rules; never hot-link |
| `display_order` | integer | yes | Order |
| `caption` | text | yes | LongDescription |
| `image_width` | integer | yes | ImageWidth |
| `image_height` | integer | yes | ImageHeight |
| `permission` | text[] | yes | Permission |
| `media_modification_timestamp` | timestamptz | yes | MediaModificationTimestamp |
| `storage_status` | text | not null | `pending` \| `downloaded` \| `failed` \| `skipped` (CHECK constraint); `skipped` = metadata-only mode |
| `local_path` | text | yes | sink-relative path/key once downloaded |
| `content_type` | text | yes | from download response |
| `bytes` | bigint | yes | from download response |
| `failure_count` | integer | not null default 0 | → `failed` after 3 |
| `updated_at` | timestamptz | not null | |

Because MediaKeys are immutable, a key with `storage_status='downloaded'` is never re-fetched. Orphan cleanup (keys absent from a re-fetched parent's Media array) runs only when the incoming array is non-empty.

### `open_house`

| Column | Type | Null | Source |
|---|---|---|---|
| `open_house_key` | text | PK | OpenHouseKey |
| `listing_key` | text | yes, indexed | ListingKey (FK not enforced — the listing may sync later) |
| `listing_id` | text | yes | ListingId |
| `open_house_date` | date | yes | OpenHouseDate |
| `start_time` | timestamptz | yes | OpenHouseStartTime |
| `end_time` | timestamptz | yes | OpenHouseEndTime |
| `remarks` | text | yes | OpenHouseRemarks |
| `modification_timestamp` | timestamptz | not null | ModificationTimestamp (cursor field) |
| `raw` | jsonb | yes | full record |
| `updated_at` | timestamptz | not null | |

Same `MlgCanView` delete semantics as `property`.

### `listing_event` — change capture

MLS Grid provides **no history API**. This append-only table is populated by the sync engine at upsert time by diffing incoming values against the stored row. It is what powers price-history/motivated-seller analysis downstream, and it is **best-effort from first sync forward** — no history exists for changes that predate your backfill.

| Column | Type | Notes |
|---|---|---|
| `id` | bigint generated always as identity | PK |
| `listing_key` | text | indexed with `observed_at` |
| `event_type` | text | `new_listing` \| `price_change` \| `status_change` \| `back_on_market` \| `delisted` (CHECK) |
| `old_value` | text | NULL for `new_listing` |
| `new_value` | text | NULL for `delisted` |
| `observed_at` | timestamptz | when the sync observed it |
| `source_modification_timestamp` | timestamptz | the record's Grid timestamp at observation |

No FK to `property`: events survive listing deletion (`delisted` is the terminal event).

### `sync_state` — replication cursor

| Column | Type | Notes |
|---|---|---|
| `resource` | text | PK part: `Property`, `OpenHouse`, … |
| `originating_system` | text | PK part |
| `last_modification_ts` | timestamptz | watermark; advances only after a page's transaction commits |
| `in_progress_url` | text | current `@odata.nextLink` for mid-backfill resume; NULL when idle |
| `backfill_completed_at` | timestamptz | NULL until initial backfill finishes; incremental sync **refuses to run** while NULL |
| `last_full_reconcile_at` | timestamptz | |
| `updated_at` | timestamptz | |

**Invariant (enforced in code, documented here because consumers may rely on it):** incremental sync never runs with a NULL/zero watermark. A missing cursor directs the operator to `backfill` — an empty cursor must never mean "fetch everything."

### `rate_budget` — persisted rate-limit accounting

| Column | Type | Notes |
|---|---|---|
| `window_kind` | text | PK part: `hour` \| `day` |
| `window_start` | timestamptz | PK part; aligned to UTC wall-clock boundaries |
| `requests` | integer | |
| `bytes_downloaded` | bigint | |
| `updated_at` | timestamptz | |

Persisting budgets means a crash-looping process cannot launder its rate-limit usage. Rows older than 48h are pruned opportunistically.

### `schema_meta`

Key/value: `key text PK, value text, updated_at timestamptz`. Required row: `contract_version` (e.g. `1.0.0`). Consumers assert compatibility against it at startup.

## Cursor & delete semantics

- **Backfill** filters `MlgCanView eq true`; **incremental** filters `ModificationTimestamp ge <watermark>` and deliberately does NOT filter `MlgCanView`, so `false` records arrive.
- `ge` (not `gt`): upserts are idempotent, so re-processing boundary records is harmless; skipping them would be silent data loss.
- On `MlgCanView=false`: the property row and its children (`room`, `unit_type`, `media` rows, and downloaded media files) are **hard-deleted** — this is a license obligation, not a design choice — and a `listing_event(delisted)` is recorded. Consumers can therefore assume every stored property is currently licensed for their use tier (subject to `mlg_can_use`).
- `MlgCanView=false` records remain in the feed only ~7 days. A periodic **reconcile pass** (cheap `$select=ListingKey,ModificationTimestamp` sweep with `MlgCanView eq true`) diffs the full remote key set against local keys, purging local rows missing remotely and re-queueing rows whose remote timestamp is newer. `sync_state.last_full_reconcile_at` records completion.

## Indexes (contract-guaranteed)

Consumers may rely on these existing:

- `property`: PK (`listing_key`); `(listing_id)`; `(standard_status)`; `(property_type, city)`; `(postal_code)`; `(list_price)`; `(modification_timestamp)`; `(latitude, longitude)`; GIN (`raw`); partial `(close_date)` WHERE `standard_status = 'Closed'`
- `listing_event`: `(listing_key, observed_at)`
- `media`: `(listing_key)`; partial `(storage_status)` WHERE `storage_status = 'pending'`
- `open_house`: `(listing_key)`; `(open_house_date)`

## Appendix A — MRED field alias map

MRED renamed/merged fields on **2025-04-02**, applied only to records created/modified after that date; older records still carry the old keys, and MLS Grid explicitly instructs consumers **not** to re-pull old records. `internal/fieldscope` ships this as the `builtin:mred` alias map; both spellings fold into the same core column (where one exists), and `raw` keeps whichever key actually arrived.

Renames:

| Old (pre-2025-04) | New (RESO) |
|---|---|
| `MRD_ACTV_DATE` | `ActivationDate` |
| `MRD_BMD` | `BackOnMarketDate` |
| `MRD_DBL` | `BodyType` |
| `MRD_RENTAL_PROPERTY_TYPE` | `PropertyAttachedYN` |
| `MRD_UFL` | `EntryLevel` |

Merged into `Basement`: `MRD_BAS`. Merged into `ParkingFeatures`: `MRD_DRV`, `MRD_GAR`, `MRD_GARAGE_TYPE`, `MRD_GARAGE_OWNERSHIP`, `MRD_GARAGE_ONSITE`, `MRD_PARKING_OWNERSHIP`, `MRD_PARKING_ONSITE`, `MRD_PKN`.

Added (RESO): `Fencing`, `HorseAmenities`, `Levels`, `WaterfrontFeatures`.

## Appendix B — known feed quirks the schema absorbs

Documented here because they shape column types; each has a corresponding test fixture:

- Numeric fields may arrive as JSON floats where integers are expected.
- Timestamps may arrive RFC3339 **or** timezone-naive (`2006-01-02T15:04:05`); naive values are interpreted as UTC.
- Some fields (`Contingency`, `Possession`) arrive as either a string or an array of strings.
- `CloseDate` and several other dates arrive as free text on some records.
- Records contain only the fields their source MLS populated — absence of a key is normal, not an error.
