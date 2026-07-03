// Package config loads and validates mlsgrid-sync configuration from a YAML
// file with MLSGRID_-prefixed environment overrides. API tokens are never read
// from the file itself — profiles name an environment variable instead, so
// config files are always safe to commit.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Presets accepted for Profile.FieldScope; anything else is treated as a path
// to a custom scope YAML (validated when field scopes land in M8).
var fieldScopePresets = map[string]bool{
	"minimal": true, "standard": true, "analytics": true, "full": true,
}

var supportedResources = map[string]bool{
	"Property": true, "OpenHouse": true,
}

type Config struct {
	Database  Database           `mapstructure:"database"`
	Profiles  map[string]Profile `mapstructure:"profiles"`
	Sync      Sync               `mapstructure:"sync"`
	RateLimit RateLimit          `mapstructure:"ratelimit"`
}

type Database struct {
	// URL is a Postgres connection string. Prefer setting it via the
	// MLSGRID_DATABASE_URL environment variable over the config file.
	URL    string `mapstructure:"url"`
	Schema string `mapstructure:"schema"`
}

type Profile struct {
	// OriginatingSystem is the MLS Grid system slug (e.g. "mred").
	// Every API request is scoped to exactly one system.
	OriginatingSystem string `mapstructure:"originating_system"`
	// TokenEnv names the environment variable holding the bearer token.
	TokenEnv string `mapstructure:"token_env"`
	// FieldScope is a preset name (minimal|standard|analytics|full) or a
	// path to a custom scope YAML.
	FieldScope string `mapstructure:"field_scope"`
	// FieldAliases is "builtin:<system>" (e.g. builtin:mred) or a path to a
	// custom alias-map YAML. Empty means no alias normalization.
	FieldAliases string   `mapstructure:"field_aliases"`
	Resources    []string `mapstructure:"resources"`
	Media        Media    `mapstructure:"media"`
}

type Media struct {
	// Mode is metadata-only (store URLs and metadata) or download
	// (fetch files to the configured sink).
	Mode string `mapstructure:"mode"`
	Sink Sink   `mapstructure:"sink"`
}

type Sink struct {
	// Type is disk or s3 (any S3-compatible endpoint: AWS, R2, MinIO).
	Type     string `mapstructure:"type"`
	Path     string `mapstructure:"path"`
	Endpoint string `mapstructure:"endpoint"`
	Bucket   string `mapstructure:"bucket"`
	Prefix   string `mapstructure:"prefix"`
	Region   string `mapstructure:"region"`
}

type Sync struct {
	Interval       time.Duration `mapstructure:"interval"`
	ReconcileEvery time.Duration `mapstructure:"reconcile_every"`
	// PageSize is $top; the API caps it at 1000 when $expand is used.
	PageSize int `mapstructure:"page_size"`
	// HealthAddr is where the daemon serves GET /healthz; empty disables it.
	HealthAddr string `mapstructure:"health_addr"`
}

// RateLimit defaults sit deliberately under MLS Grid's published caps
// (2 rps, 7200/hr, 40000/day, 4 GB/hr). Raising them above the caps is
// refused at validation time.
type RateLimit struct {
	RPS           float64 `mapstructure:"rps"`
	Hourly        int     `mapstructure:"hourly"`
	Daily         int     `mapstructure:"daily"`
	BytesHourlyMB int     `mapstructure:"bytes_hourly_mb"`
}

// MLS Grid published hard caps; see docs/compliance.md.
const (
	capRPS           = 2.0
	capHourly        = 7200
	capDaily         = 40000
	capBytesHourlyMB = 4096
)

func setDefaults(v *viper.Viper) {
	v.SetDefault("database.schema", "mlsgrid")
	v.SetDefault("sync.interval", "5m")
	v.SetDefault("sync.reconcile_every", "24h")
	v.SetDefault("sync.page_size", 1000)
	v.SetDefault("sync.health_addr", "127.0.0.1:8322")
	v.SetDefault("ratelimit.rps", 1.8)
	v.SetDefault("ratelimit.hourly", 6800)
	v.SetDefault("ratelimit.daily", 38000)
	v.SetDefault("ratelimit.bytes_hourly_mb", 3500)
}

// Load reads configuration from path. If path is empty it searches
// ./mlsgrid-sync.yaml, then $XDG_CONFIG_HOME/mlsgrid-sync/config.yaml.
// Environment variables prefixed MLSGRID_ override file values
// (e.g. MLSGRID_DATABASE_URL overrides database.url).
func Load(path string) (*Config, error) {
	v := viper.New()
	setDefaults(v)
	v.SetEnvPrefix("MLSGRID")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	} else {
		v.SetConfigName("mlsgrid-sync")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		if xdg := configHome(); xdg != "" {
			v.SetConfigName("config")
			v.AddConfigPath(filepath.Join(xdg, "mlsgrid-sync"))
		}
		if err := v.ReadInConfig(); err != nil {
			var notFound viper.ConfigFileNotFoundError
			if !errors.As(err, &notFound) {
				return nil, err
			}
			// No file is fine: env vars + defaults may be enough for
			// commands like status; API commands validate per-profile.
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func configHome() string {
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		return x
	}
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, ".config")
	}
	return ""
}

func (c *Config) validate() error {
	if c.RateLimit.RPS > capRPS || c.RateLimit.Hourly > capHourly ||
		c.RateLimit.Daily > capDaily || c.RateLimit.BytesHourlyMB > capBytesHourlyMB {
		return fmt.Errorf("ratelimit config exceeds MLS Grid published caps (%.1f rps, %d/hr, %d/day, %d MB/hr) — refusing: exceeding them suspends your token", capRPS, capHourly, capDaily, capBytesHourlyMB)
	}
	if c.Sync.PageSize < 1 || c.Sync.PageSize > 1000 {
		return fmt.Errorf("sync.page_size must be 1-1000 (the API caps $top at 1000 with $expand), got %d", c.Sync.PageSize)
	}
	for name, p := range c.Profiles {
		if err := p.validate(); err != nil {
			return fmt.Errorf("profile %q: %w", name, err)
		}
	}
	return nil
}

func (p *Profile) validate() error {
	if p.OriginatingSystem == "" {
		return fmt.Errorf("originating_system is required")
	}
	if p.TokenEnv == "" {
		return fmt.Errorf("token_env is required (name of the environment variable holding your MLS Grid bearer token)")
	}
	if p.FieldScope != "" && !fieldScopePresets[p.FieldScope] && !strings.ContainsAny(p.FieldScope, "./") {
		return fmt.Errorf("field_scope must be a preset (minimal|standard|analytics|full) or a path to a scope YAML, got %q", p.FieldScope)
	}
	for _, r := range p.Resources {
		if !supportedResources[r] {
			return fmt.Errorf("unsupported resource %q (v1 supports Property and OpenHouse)", r)
		}
	}
	switch p.Media.Mode {
	case "", "metadata-only":
	case "download":
		switch p.Media.Sink.Type {
		case "disk":
			if p.Media.Sink.Path == "" {
				return fmt.Errorf("media.sink.path is required for disk sink")
			}
		case "s3":
			if p.Media.Sink.Bucket == "" {
				return fmt.Errorf("media.sink.bucket is required for s3 sink")
			}
		default:
			return fmt.Errorf("media.sink.type must be disk or s3, got %q", p.Media.Sink.Type)
		}
	default:
		return fmt.Errorf("media.mode must be metadata-only or download, got %q", p.Media.Mode)
	}
	return nil
}

// Token reads the profile's bearer token from its configured environment
// variable. It is resolved lazily so non-API commands work without it.
func (p *Profile) Token() (string, error) {
	t := os.Getenv(p.TokenEnv)
	if t == "" {
		return "", fmt.Errorf("environment variable %s is empty — set it to your MLS Grid bearer token", p.TokenEnv)
	}
	return t, nil
}

// Profile resolves a profile by name. With an empty name it returns the sole
// configured profile, or an error listing choices when several exist.
func (c *Config) Profile(name string) (*Profile, error) {
	if len(c.Profiles) == 0 {
		return nil, fmt.Errorf("no profiles configured — add one under profiles: in your config file")
	}
	if name == "" {
		if len(c.Profiles) == 1 {
			for _, p := range c.Profiles {
				return &p, nil
			}
		}
		return nil, fmt.Errorf("config defines %d profiles (%s) — pick one with --profile", len(c.Profiles), strings.Join(profileNames(c.Profiles), ", "))
	}
	p, ok := c.Profiles[name]
	if !ok {
		return nil, fmt.Errorf("unknown profile %q (configured: %s)", name, strings.Join(profileNames(c.Profiles), ", "))
	}
	return &p, nil
}

func profileNames(m map[string]Profile) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	return names
}
