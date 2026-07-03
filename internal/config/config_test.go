package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mlsgrid-sync.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const validConfig = `
database:
  url: postgres://localhost/mls
profiles:
  mred:
    originating_system: mred
    token_env: MLSGRID_TOKEN_MRED
    field_scope: standard
    resources: [Property, OpenHouse]
`

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Schema != "mlsgrid" {
		t.Errorf("default schema = %q, want mlsgrid", cfg.Database.Schema)
	}
	if cfg.Sync.PageSize != 1000 {
		t.Errorf("default page_size = %d, want 1000", cfg.Sync.PageSize)
	}
	if cfg.RateLimit.RPS != 1.8 {
		t.Errorf("default rps = %v, want 1.8", cfg.RateLimit.RPS)
	}
	if cfg.Sync.Interval.Minutes() != 5 {
		t.Errorf("default interval = %v, want 5m", cfg.Sync.Interval)
	}
}

func TestRateLimitCapRefused(t *testing.T) {
	_, err := Load(writeConfig(t, validConfig+`
ratelimit:
  hourly: 9000
`))
	if err == nil || !strings.Contains(err.Error(), "exceeds MLS Grid published caps") {
		t.Fatalf("want cap-exceeded error, got %v", err)
	}
}

func TestProfileValidation(t *testing.T) {
	cases := []struct {
		name, yaml, wantErr string
	}{
		{"missing originating_system", `
profiles:
  bad:
    token_env: T
`, "originating_system is required"},
		{"missing token_env", `
profiles:
  bad:
    originating_system: mred
`, "token_env is required"},
		{"bad resource", `
profiles:
  bad:
    originating_system: mred
    token_env: T
    resources: [Member]
`, "unsupported resource"},
		{"bad media mode", `
profiles:
  bad:
    originating_system: mred
    token_env: T
    media: {mode: hotlink}
`, "media.mode must be"},
		{"download needs sink", `
profiles:
  bad:
    originating_system: mred
    token_env: T
    media: {mode: download, sink: {type: disk}}
`, "media.sink.path is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestProfileSelection(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig+`
  norstar:
    originating_system: northstar
    token_env: MLSGRID_TOKEN_NORTHSTAR
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Profile(""); err == nil || !strings.Contains(err.Error(), "--profile") {
		t.Errorf("ambiguous selection should demand --profile, got %v", err)
	}
	p, err := cfg.Profile("mred")
	if err != nil {
		t.Fatal(err)
	}
	if p.OriginatingSystem != "mred" {
		t.Errorf("got %q", p.OriginatingSystem)
	}
	if _, err := cfg.Profile("nope"); err == nil {
		t.Error("unknown profile should error")
	}
}

func TestSingleProfileImplicit(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	p, err := cfg.Profile("")
	if err != nil {
		t.Fatal(err)
	}
	if p.OriginatingSystem != "mred" {
		t.Errorf("got %q", p.OriginatingSystem)
	}
}

func TestTokenFromEnv(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := cfg.Profile("")
	if _, err := p.Token(); err == nil {
		t.Error("empty env var should error")
	}
	t.Setenv("MLSGRID_TOKEN_MRED", "tok-123")
	tok, err := p.Token()
	if err != nil || tok != "tok-123" {
		t.Errorf("got %q, %v", tok, err)
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("MLSGRID_DATABASE_URL", "postgres://override/db")
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.URL != "postgres://override/db" {
		t.Errorf("env override not applied, got %q", cfg.Database.URL)
	}
}

func TestEnvOnlyDatabaseURL(t *testing.T) {
	// The env var must work even when the config file has NO database
	// section at all — viper only surfaces env overrides for known keys,
	// so database.url needs a registered default.
	t.Setenv("MLSGRID_DATABASE_URL", "postgres://envonly/db")
	cfg, err := Load(writeConfig(t, `
profiles:
  mred:
    originating_system: mred
    token_env: T
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.URL != "postgres://envonly/db" {
		t.Errorf("env-only database.url not applied, got %q", cfg.Database.URL)
	}
}
