-- Contract v1.0.0 — docs/schema-contract.md is the specification; this file
-- implements it. Objects are intentionally unqualified: the migrator sets
-- search_path to the configured schema before executing.

CREATE TABLE property (
    listing_key                        text PRIMARY KEY,
    listing_id                         text NOT NULL,
    originating_system_name            text NOT NULL,
    standard_status                    text,
    mls_status                         text,
    property_type                      text,
    property_sub_type                  text,
    list_price                         numeric,
    original_list_price                numeric,
    previous_list_price                numeric,
    close_price                        numeric,
    listing_contract_date              date,
    purchase_contract_date             date,
    close_date                         date,
    off_market_date                    date,
    days_on_market                     integer,
    cumulative_days_on_market          integer,
    street_number                      text,
    street_dir_prefix                  text,
    street_name                        text,
    street_suffix                      text,
    unit_number                        text,
    city                               text,
    postal_code                        text,
    county_or_parish                   text,
    state_or_province                  text,
    township                           text,
    subdivision_name                   text,
    latitude                           double precision,
    longitude                          double precision,
    bedrooms_total                     integer,
    bathrooms_full                     integer,
    bathrooms_half                     integer,
    rooms_total                        integer,
    living_area                        numeric,
    building_area_total                numeric,
    lot_size_acres                     numeric,
    lot_size_square_feet               numeric,
    year_built                         integer,
    stories_total                      numeric,
    garage_spaces                      numeric,
    parking_total                      integer,
    number_of_units_total              integer,
    new_construction_yn                boolean,
    property_attached_yn               boolean,
    waterfront_yn                      boolean,
    public_remarks                     text,
    virtual_tour_url                   text,
    internet_address_display_yn        boolean,
    internet_entire_listing_display_yn boolean,
    elementary_school                  text,
    middle_or_junior_school            text,
    high_school                        text,
    elementary_school_district         text,
    middle_or_junior_school_district   text,
    high_school_district               text,
    list_agent_full_name               text,
    list_agent_mls_id                  text,
    list_agent_key                     text,
    list_office_name                   text,
    list_office_mls_id                 text,
    buyer_agent_full_name              text,
    buyer_agent_mls_id                 text,
    buyer_office_name                  text,
    tax_annual_amount                  numeric,
    tax_year                           integer,
    association_fee                    numeric,
    association_fee_frequency          text,
    parcel_number                      text,
    modification_timestamp             timestamptz NOT NULL,
    original_entry_timestamp           timestamptz,
    status_change_timestamp            timestamptz,
    photos_change_timestamp            timestamptz,
    photos_count                       integer,
    mlg_can_use                        text[],
    raw                                jsonb,
    first_seen_at                      timestamptz NOT NULL DEFAULT now(),
    updated_at                         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX property_listing_id_idx ON property (listing_id);
CREATE INDEX property_standard_status_idx ON property (standard_status);
CREATE INDEX property_type_city_idx ON property (property_type, city);
CREATE INDEX property_postal_code_idx ON property (postal_code);
CREATE INDEX property_list_price_idx ON property (list_price);
CREATE INDEX property_modification_timestamp_idx ON property (modification_timestamp);
CREATE INDEX property_lat_lng_idx ON property (latitude, longitude);
CREATE INDEX property_raw_idx ON property USING gin (raw);
CREATE INDEX property_close_date_closed_idx ON property (close_date)
    WHERE standard_status = 'Closed';

CREATE TABLE room (
    listing_key     text NOT NULL REFERENCES property (listing_key) ON DELETE CASCADE,
    room_key        text NOT NULL,
    room_type       text,
    room_level      text,
    room_dimensions text,
    raw             jsonb,
    PRIMARY KEY (listing_key, room_key)
);

CREATE TABLE unit_type (
    listing_key   text NOT NULL REFERENCES property (listing_key) ON DELETE CASCADE,
    unit_type_key text NOT NULL,
    unit_number   text,
    floor_number  text,
    beds_total    integer,
    baths_total   integer,
    actual_rent   numeric,
    raw           jsonb,
    PRIMARY KEY (listing_key, unit_type_key)
);

CREATE TABLE media (
    media_key                    text PRIMARY KEY,
    listing_key                  text NOT NULL REFERENCES property (listing_key) ON DELETE CASCADE,
    media_url                    text,
    display_order                integer,
    caption                      text,
    image_width                  integer,
    image_height                 integer,
    permission                   text[],
    media_modification_timestamp timestamptz,
    storage_status               text NOT NULL
        CHECK (storage_status IN ('pending', 'downloaded', 'failed', 'skipped')),
    local_path                   text,
    content_type                 text,
    bytes                        bigint,
    failure_count                integer NOT NULL DEFAULT 0,
    updated_at                   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX media_listing_key_idx ON media (listing_key);
CREATE INDEX media_pending_idx ON media (storage_status)
    WHERE storage_status = 'pending';

CREATE TABLE open_house (
    open_house_key         text PRIMARY KEY,
    listing_key            text,
    listing_id             text,
    open_house_date        date,
    start_time             timestamptz,
    end_time               timestamptz,
    remarks                text,
    modification_timestamp timestamptz NOT NULL,
    raw                    jsonb,
    updated_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX open_house_listing_key_idx ON open_house (listing_key);
CREATE INDEX open_house_date_idx ON open_house (open_house_date);

-- Append-only change capture; no FK to property so events survive deletion
-- (delisted is the terminal event for a listing).
CREATE TABLE listing_event (
    id                            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    listing_key                   text NOT NULL,
    event_type                    text NOT NULL
        CHECK (event_type IN ('new_listing', 'price_change', 'status_change', 'back_on_market', 'delisted')),
    old_value                     text,
    new_value                     text,
    observed_at                   timestamptz NOT NULL DEFAULT now(),
    source_modification_timestamp timestamptz
);

CREATE INDEX listing_event_key_observed_idx ON listing_event (listing_key, observed_at);

CREATE TABLE sync_state (
    resource               text NOT NULL,
    originating_system     text NOT NULL,
    last_modification_ts   timestamptz,
    in_progress_url        text,
    backfill_completed_at  timestamptz,
    last_full_reconcile_at timestamptz,
    updated_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (resource, originating_system)
);

CREATE TABLE rate_budget (
    window_kind      text NOT NULL CHECK (window_kind IN ('hour', 'day')),
    window_start     timestamptz NOT NULL,
    requests         integer NOT NULL DEFAULT 0,
    bytes_downloaded bigint NOT NULL DEFAULT 0,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_kind, window_start)
);

CREATE TABLE schema_meta (
    key        text PRIMARY KEY,
    value      text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO schema_meta (key, value)
VALUES ('contract_version', '1.0.0')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
