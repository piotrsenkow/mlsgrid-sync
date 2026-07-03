package postgres

import (
	"strings"
	"testing"
)

func TestLoadMigrationsOrderedAndChecksummed(t *testing.T) {
	ms, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) == 0 {
		t.Fatal("no embedded migrations found")
	}
	if ms[0].version != 1 || ms[0].name != "0001_init.sql" {
		t.Errorf("first migration = %d %q", ms[0].version, ms[0].name)
	}
	prev := 0
	for _, m := range ms {
		if m.version <= prev {
			t.Errorf("migrations out of order at %q", m.name)
		}
		prev = m.version
		if len(m.checksum) != 64 {
			t.Errorf("%q checksum %q is not sha256 hex", m.name, m.checksum)
		}
		if strings.TrimSpace(m.sql) == "" {
			t.Errorf("%q is empty", m.name)
		}
	}
}

func TestInitMigrationIsSchemaAgnostic(t *testing.T) {
	// Migration files must create objects unqualified — the migrator scopes
	// them via search_path so the schema name stays configurable.
	ms, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		if strings.Contains(m.sql, "mlsgrid.") {
			t.Errorf("%q hardcodes the mlsgrid schema; objects must be unqualified", m.name)
		}
	}
}
