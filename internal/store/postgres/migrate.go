package postgres

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

type migration struct {
	version  int
	name     string
	checksum string
	sql      string
}

// loadMigrations reads the embedded migration files, ordered by their numeric
// prefix (NNNN_name.sql). Duplicate versions are a programming error.
func loadMigrations() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("reading embedded migrations: %w", err)
	}
	seen := map[int]string{}
	var out []migration
	for _, e := range entries {
		name := e.Name()
		prefix, _, ok := strings.Cut(name, "_")
		version, convErr := strconv.Atoi(prefix)
		if !ok || convErr != nil || !strings.HasSuffix(name, ".sql") {
			return nil, fmt.Errorf("migration %q does not match NNNN_name.sql", name)
		}
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("duplicate migration version %d (%s and %s)", version, prev, name)
		}
		seen[version] = name
		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(body)
		out = append(out, migration{
			version:  version,
			name:     name,
			checksum: hex.EncodeToString(sum[:]),
			sql:      string(body),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// Migrate applies pending migrations in order, each in its own transaction
// with search_path set to the configured schema. Applied migrations are
// recorded with a checksum; editing an already-applied file is refused —
// schema changes require a new migration (and a contract version bump, per
// docs/schema-contract.md).
func (s *Store) Migrate(ctx context.Context) error {
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	// Serialize concurrent migrators (e.g. two init-db runs) per schema.
	lockKey := int64(fnvHash("mlsgrid-sync:" + s.schema))
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockKey); err != nil {
		return fmt.Errorf("acquiring migration lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockKey) }()

	if _, err := conn.Exec(ctx, fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s", s.schemaIdent())); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		version    integer PRIMARY KEY,
		name       text NOT NULL,
		checksum   text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`, s.table("schema_migrations"))); err != nil {
		return fmt.Errorf("creating schema_migrations: %w", err)
	}

	applied := map[int]string{}
	rows, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT version, checksum FROM %s", s.table("schema_migrations")))
	if err != nil {
		return err
	}
	var v int
	var sum string
	if _, err := pgx.ForEachRow(rows, []any{&v, &sum}, func() error {
		applied[v] = sum
		return nil
	}); err != nil {
		return err
	}

	for _, m := range migrations {
		if sum, ok := applied[m.version]; ok {
			if sum != m.checksum {
				return fmt.Errorf("migration %s was modified after being applied (checksum mismatch) — write a new migration instead", m.name)
			}
			continue
		}
		if err := s.applyMigration(ctx, conn.Conn(), m); err != nil {
			return fmt.Errorf("applying %s: %w", m.name, err)
		}
	}
	return nil
}

func (s *Store) applyMigration(ctx context.Context, conn *pgx.Conn, m migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Migration files create objects unqualified; scope them to our schema.
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"SET LOCAL search_path TO %s", s.schemaIdent())); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (version, name, checksum) VALUES ($1, $2, $3)",
		s.table("schema_migrations")), m.version, m.name, m.checksum); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// fnvHash is a stable 64-bit hash for the advisory lock key (FNV-1a).
func fnvHash(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}
