// Package postgres implements store.Store on PostgreSQL via pgx/v5. The
// schema it manages is specified by docs/schema-contract.md and created by the
// embedded migrations; a golden-schema integration test keeps the two honest.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/piotrsenkow/mlsgrid-sync/internal/fieldscope"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// Store is the PostgreSQL implementation of store.Store.
type Store struct {
	pool    *pgxpool.Pool
	schema  string
	aliases *fieldscope.AliasMap

	// mediaStatus is the storage_status newly-discovered media rows get:
	// "pending" queues them for download (M7), "skipped" records
	// metadata-only mode.
	mediaStatus string

	// SQL is generated once from the field maps so column order has a single
	// source of truth.
	sqlPropertyUpsert  string
	sqlRoomInsert      string
	sqlUnitTypeInsert  string
	sqlMediaUpsert     string
	sqlOpenHouseUpsert string
	sqlEventInsert     string
}

var _ store.Store = (*Store)(nil)

// Options configures the store.
type Options struct {
	// Schema is the Postgres schema holding all objects (default "mlsgrid").
	Schema string
	// Aliases folds legacy per-MLS field spellings into core columns.
	Aliases *fieldscope.AliasMap
	// MediaDownload marks new media rows pending download instead of skipped.
	MediaDownload bool
}

// New connects to databaseURL and returns a ready store. It does not migrate;
// call Migrate (or the init-db command) first on fresh databases.
func New(ctx context.Context, databaseURL string, opts Options) (*Store, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("database.url is not set — set MLSGRID_DATABASE_URL or database.url in the config file")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}
	return NewWithPool(pool, opts), nil
}

// NewWithPool wraps an existing pool (used by tests).
func NewWithPool(pool *pgxpool.Pool, opts Options) *Store {
	if opts.Schema == "" {
		opts.Schema = "mlsgrid"
	}
	s := &Store{
		pool:        pool,
		schema:      opts.Schema,
		aliases:     opts.Aliases,
		mediaStatus: "skipped",
	}
	if opts.MediaDownload {
		s.mediaStatus = "pending"
	}
	s.sqlPropertyUpsert = propertyUpsertSQL(s.table("property"))
	s.sqlRoomInsert = roomInsertSQL(s.table("room"))
	s.sqlUnitTypeInsert = unitTypeInsertSQL(s.table("unit_type"))
	s.sqlMediaUpsert = mediaUpsertSQL(s.table("media"))
	s.sqlOpenHouseUpsert = openHouseUpsertSQL(s.table("open_house"))
	s.sqlEventInsert = fmt.Sprintf(
		`INSERT INTO %s (listing_key, event_type, old_value, new_value, source_modification_timestamp)
		 VALUES ($1, $2, $3, $4, $5)`, s.table("listing_event"))
	return s
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

func (s *Store) schemaIdent() string {
	return pgx.Identifier{s.schema}.Sanitize()
}

func (s *Store) table(name string) string {
	return pgx.Identifier{s.schema, name}.Sanitize()
}

// ContractVersion reads schema_meta's contract_version row.
func (s *Store) ContractVersion(ctx context.Context) (string, error) {
	var v string
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT value FROM %s WHERE key = 'contract_version'",
		s.table("schema_meta"))).Scan(&v)
	if err != nil {
		return "", fmt.Errorf("reading contract version (has init-db run?): %w", err)
	}
	return v, nil
}

// resourceTable maps a feed resource to its table and key column.
func resourceTable(resource string) (table, keyCol string, err error) {
	switch resource {
	case "Property":
		return "property", "listing_key", nil
	case "OpenHouse":
		return "open_house", "open_house_key", nil
	default:
		return "", "", fmt.Errorf("unknown resource %q", resource)
	}
}

// Count reports stored rows for a resource.
func (s *Store) Count(ctx context.Context, resource string) (int64, error) {
	table, _, err := resourceTable(resource)
	if err != nil {
		return 0, err
	}
	var n int64
	err = s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s", s.table(table))).Scan(&n)
	return n, err
}

// ListKeys returns every stored key with its local modification timestamp.
func (s *Store) ListKeys(ctx context.Context, resource string) (map[string]time.Time, error) {
	table, keyCol, err := resourceTable(resource)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		"SELECT %s, modification_timestamp FROM %s", keyCol, s.table(table)))
	if err != nil {
		return nil, err
	}
	keys := make(map[string]time.Time)
	var key string
	var ts time.Time
	if _, err := pgx.ForEachRow(rows, []any{&key, &ts}, func() error {
		keys[key] = ts
		return nil
	}); err != nil {
		return nil, err
	}
	return keys, nil
}
