//go:build integration

package postgres

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

var updateGolden = flag.Bool("update", false, "rewrite the golden schema dump")

// TestGoldenSchema locks the migrated schema to testdata/golden_schema.txt.
// docs/schema-contract.md is the human-readable contract; this dump is its
// machine-checked shadow. If this test fails you either need a new migration
// plus a contract version bump, or you changed a migration that already
// shipped (never do that).
//
// Regenerate after an intentional change:
//
//	go test -tags integration ./internal/store/postgres -run TestGoldenSchema -update
func TestGoldenSchema(t *testing.T) {
	const schema = "t_golden"
	s := newTestStore(t, schema, Options{})
	dump := dumpSchema(t, s, schema)
	dump = strings.ReplaceAll(dump, schema+".", "<schema>.")

	goldenPath := filepath.Join("testdata", "golden_schema.txt")
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, []byte(dump), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Log("golden schema rewritten — remember the contract version bump if this was a real change")
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("reading golden file (run with -update to create it): %v", err)
	}
	if dump != string(want) {
		t.Errorf("schema drifted from golden dump.\nGot:\n%s\n\nDiff hint: re-run with -update and inspect git diff; schema changes need a migration + contract version bump.", dump)
	}
}

// dumpSchema renders a canonical text form of every contract object:
// columns, constraints, and indexes. schema_migrations is migrator
// bookkeeping, not contract surface, and is excluded.
func dumpSchema(t *testing.T, s *Store, schema string) string {
	t.Helper()
	ctx := context.Background()
	var b strings.Builder

	rows, err := s.pool.Query(ctx, `
		SELECT table_name, column_name, data_type, udt_name, is_nullable,
		       coalesce(column_default, ''), is_identity
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name <> 'schema_migrations'
		ORDER BY table_name, ordinal_position`, schema)
	if err != nil {
		t.Fatal(err)
	}
	var tbl, col, typ, udt, nullable, def, identity string
	if _, err := pgx.ForEachRow(rows, []any{&tbl, &col, &typ, &udt, &nullable, &def, &identity}, func() error {
		fmt.Fprintf(&b, "COLUMN %s.%s %s(%s) nullable=%s default=%s identity=%s\n",
			tbl, col, typ, udt, nullable, def, identity)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	rows, err = s.pool.Query(ctx, `
		SELECT rel.relname, con.conname, pg_get_constraintdef(con.oid)
		FROM pg_constraint con
		JOIN pg_class rel ON rel.oid = con.conrelid
		JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
		WHERE nsp.nspname = $1 AND rel.relname <> 'schema_migrations'
		ORDER BY rel.relname, con.conname`, schema)
	if err != nil {
		t.Fatal(err)
	}
	var rel, name, def2 string
	if _, err := pgx.ForEachRow(rows, []any{&rel, &name, &def2}, func() error {
		fmt.Fprintf(&b, "CONSTRAINT %s %s: %s\n", rel, name, def2)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	rows, err = s.pool.Query(ctx, `
		SELECT tablename, indexname, indexdef
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename <> 'schema_migrations'
		ORDER BY tablename, indexname`, schema)
	if err != nil {
		t.Fatal(err)
	}
	var itbl, iname, idef string
	if _, err := pgx.ForEachRow(rows, []any{&itbl, &iname, &idef}, func() error {
		fmt.Fprintf(&b, "INDEX %s %s: %s\n", itbl, iname, idef)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return b.String()
}
