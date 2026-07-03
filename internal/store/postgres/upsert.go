package postgres

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// priorListing is what change capture needs from the stored row before an
// upsert overwrites it. list_price is read as text so comparison happens in
// numeric's exact decimal rendering, not float.
type priorListing struct {
	listPrice *string
	status    *string
}

type eventRow struct {
	typ      string
	oldValue *string
	newValue *string
}

// statuses from which a transition to Active reads as back-on-market rather
// than a plain status change.
var backOnMarketFrom = map[string]bool{
	"Active Under Contract": true,
	"Pending":               true,
	"Canceled":              true,
	"Cancelled":             true,
	"Expired":               true,
	"Withdrawn":             true,
	"Hold":                  true,
}

// classifyEvents derives listing_event rows by diffing the incoming record
// against the stored one. MLS Grid has no history API, so this capture —
// best-effort from first sync forward — is the only price/status history
// downstream consumers get.
func classifyEvents(prior *priorListing, newPrice, newStatus *string) []eventRow {
	if prior == nil {
		return []eventRow{{typ: "new_listing", newValue: newPrice}}
	}
	var events []eventRow
	if newPrice != nil && !ptrEq(prior.listPrice, newPrice) {
		events = append(events, eventRow{typ: "price_change", oldValue: prior.listPrice, newValue: newPrice})
	}
	if newStatus != nil && !ptrEq(prior.status, newStatus) {
		typ := "status_change"
		if prior.status != nil && backOnMarketFrom[*prior.status] && *newStatus == "Active" {
			typ = "back_on_market"
		}
		events = append(events, eventRow{typ: typ, oldValue: prior.status, newValue: newStatus})
	}
	return events
}

func ptrEq(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// feedPrice renders ListPrice the way Postgres renders numeric::text, so
// classifyEvents compares like with like.
func feedPrice(rec mlsgrid.Record) *string {
	f, ok := rec.Float("ListPrice")
	if !ok {
		return nil
	}
	s := strconv.FormatFloat(f, 'f', -1, 64)
	return &s
}

func feedStatus(rec mlsgrid.Record) *string {
	if s := rec.String("StandardStatus"); s != "" {
		return &s
	}
	return nil
}

// UpsertProperties writes one page of records in a single transaction:
// property upserts, listing_event capture, and child replacement. Children
// (rooms, unit types, media) are replaced only when the incoming expansion is
// non-empty — an absent expansion means "not requested", never "delete".
func (s *Store) UpsertProperties(ctx context.Context, recs []mlsgrid.Record) (store.UpsertStats, error) {
	var stats store.UpsertStats

	type item struct {
		key       string
		modTS     time.Time
		args      []any
		newPrice  *string
		newStatus *string
		rooms     []mlsgrid.Record
		unitTypes []mlsgrid.Record
		media     []mlsgrid.Record
	}
	items := make([]item, 0, len(recs))
	keys := make([]string, 0, len(recs))
	for _, rec := range recs {
		key := rec.ListingKey()
		modTS, okTS := rec.ModificationTimestamp()
		if key == "" || !okTS || rec.String("ListingId") == "" || rec.String("OriginatingSystemName") == "" {
			stats.Skipped++
			continue
		}
		args := append([]any{key}, extractArgs(rec, s.aliases, propertyCols)...)
		args = append(args, rec.Raw())
		items = append(items, item{
			key:       key,
			modTS:     modTS,
			args:      args,
			newPrice:  feedPrice(rec),
			newStatus: feedStatus(rec),
			rooms:     rec.Children("Rooms"),
			unitTypes: rec.Children("UnitTypes"),
			media:     rec.Children("Media"),
		})
		keys = append(keys, key)
	}
	if len(items) == 0 {
		return stats, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.UpsertStats{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	prior, err := s.loadPrior(ctx, tx, keys)
	if err != nil {
		return store.UpsertStats{}, fmt.Errorf("loading prior rows: %w", err)
	}

	b := &pgx.Batch{}
	for _, it := range items {
		b.Queue(s.sqlPropertyUpsert, it.args...)

		var priorPtr *priorListing
		if p, exists := prior[it.key]; exists {
			priorPtr = &p
			stats.Updated++
		} else {
			stats.Inserted++
		}
		for _, ev := range classifyEvents(priorPtr, it.newPrice, it.newStatus) {
			b.Queue(s.sqlEventInsert, it.key, ev.typ, ev.oldValue, ev.newValue, it.modTS)
			stats.Events++
		}

		if len(it.rooms) > 0 {
			b.Queue(fmt.Sprintf("DELETE FROM %s WHERE listing_key = $1", s.table("room")), it.key)
			for _, room := range it.rooms {
				rk := room.String("RoomKey")
				if rk == "" {
					continue
				}
				args := append([]any{it.key, rk}, extractArgs(room, s.aliases, roomCols)...)
				b.Queue(s.sqlRoomInsert, append(args, room.Raw())...)
			}
		}
		if len(it.unitTypes) > 0 {
			b.Queue(fmt.Sprintf("DELETE FROM %s WHERE listing_key = $1", s.table("unit_type")), it.key)
			for _, ut := range it.unitTypes {
				uk := ut.String("UnitTypeKey")
				if uk == "" {
					continue
				}
				args := append([]any{it.key, uk}, extractArgs(ut, s.aliases, unitTypeCols)...)
				b.Queue(s.sqlUnitTypeInsert, append(args, ut.Raw())...)
			}
		}
		if len(it.media) > 0 {
			mediaKeys := make([]string, 0, len(it.media))
			for _, m := range it.media {
				mk := m.String("MediaKey")
				if mk == "" {
					continue
				}
				mediaKeys = append(mediaKeys, mk)
				args := append([]any{mk, it.key}, extractArgs(m, s.aliases, mediaCols)...)
				b.Queue(s.sqlMediaUpsert, append(args, s.mediaStatus)...)
			}
			// Orphan cleanup: keys no longer in the parent's Media array.
			// Runs only here, inside the non-empty guard.
			b.Queue(fmt.Sprintf(
				"DELETE FROM %s WHERE listing_key = $1 AND NOT (media_key = ANY($2))",
				s.table("media")), it.key, mediaKeys)
		}
	}

	if err := drainBatch(ctx, tx, b); err != nil {
		return store.UpsertStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.UpsertStats{}, err
	}
	return stats, nil
}

func (s *Store) loadPrior(ctx context.Context, tx pgx.Tx, keys []string) (map[string]priorListing, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(
		"SELECT listing_key, list_price::text, standard_status FROM %s WHERE listing_key = ANY($1)",
		s.table("property")), keys)
	if err != nil {
		return nil, err
	}
	prior := make(map[string]priorListing)
	var key string
	var price, status *string
	if _, err := pgx.ForEachRow(rows, []any{&key, &price, &status}, func() error {
		p := priorListing{}
		if price != nil {
			v := *price
			p.listPrice = &v
		}
		if status != nil {
			v := *status
			p.status = &v
		}
		prior[key] = p
		return nil
	}); err != nil {
		return nil, err
	}
	return prior, nil
}

func drainBatch(ctx context.Context, tx pgx.Tx, b *pgx.Batch) error {
	br := tx.SendBatch(ctx, b)
	for i := 0; i < b.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("batch statement %d: %w", i, err)
		}
	}
	return br.Close()
}

// DeleteProperties hard-deletes listings (children cascade) and records a
// delisted event per row that existed. Called for MlgCanView=false records —
// removing revoked records is a license obligation.
func (s *Store) DeleteProperties(ctx context.Context, listingKeys []string) (int64, error) {
	if len(listingKeys) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(
		`WITH del AS (
		     DELETE FROM %s WHERE listing_key = ANY($1)
		     RETURNING listing_key, standard_status
		 )
		 INSERT INTO %s (listing_key, event_type, old_value)
		 SELECT listing_key, 'delisted', standard_status FROM del`,
		s.table("property"), s.table("listing_event")), listingKeys)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// UpsertOpenHouses writes OpenHouse records in one transaction.
func (s *Store) UpsertOpenHouses(ctx context.Context, recs []mlsgrid.Record) (store.UpsertStats, error) {
	var stats store.UpsertStats
	type item struct {
		key  string
		args []any
	}
	items := make([]item, 0, len(recs))
	keys := make([]string, 0, len(recs))
	for _, rec := range recs {
		key := rec.String("OpenHouseKey")
		if _, okTS := rec.ModificationTimestamp(); key == "" || !okTS {
			stats.Skipped++
			continue
		}
		args := append([]any{key}, extractArgs(rec, s.aliases, openHouseCols)...)
		items = append(items, item{key: key, args: append(args, rec.Raw())})
		keys = append(keys, key)
	}
	if len(items) == 0 {
		return stats, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.UpsertStats{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	existing := make(map[string]bool)
	rows, err := tx.Query(ctx, fmt.Sprintf(
		"SELECT open_house_key FROM %s WHERE open_house_key = ANY($1)",
		s.table("open_house")), keys)
	if err != nil {
		return store.UpsertStats{}, err
	}
	var k string
	if _, err := pgx.ForEachRow(rows, []any{&k}, func() error {
		existing[k] = true
		return nil
	}); err != nil {
		return store.UpsertStats{}, err
	}

	b := &pgx.Batch{}
	for _, it := range items {
		b.Queue(s.sqlOpenHouseUpsert, it.args...)
		if existing[it.key] {
			stats.Updated++
		} else {
			stats.Inserted++
		}
	}
	if err := drainBatch(ctx, tx, b); err != nil {
		return store.UpsertStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.UpsertStats{}, err
	}
	return stats, nil
}

// DeleteOpenHouses removes open-house rows by key.
func (s *Store) DeleteOpenHouses(ctx context.Context, openHouseKeys []string) (int64, error) {
	if len(openHouseKeys) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE open_house_key = ANY($1)", s.table("open_house")), openHouseKeys)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
