package cli

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/piotrsenkow/mlsgrid-sync/internal/config"
	"github.com/piotrsenkow/mlsgrid-sync/internal/engine"
	"github.com/piotrsenkow/mlsgrid-sync/internal/fieldscope"
	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store/postgres"
)

// deps bundles what profile-scoped commands need. The caller owns Close on
// the store.
type deps struct {
	profile *config.Profile
	store   *postgres.Store
	pager   *mlsgrid.Pager
	limiter *ratelimit.Limiter
}

// resources returns the profile's configured resources (default Property).
func (d *deps) resources() []string {
	if len(d.profile.Resources) == 0 {
		return []string{"Property"}
	}
	return d.profile.Resources
}

func profileDeps(ctx context.Context, cmd *cobra.Command) (*deps, error) {
	p, err := selectedProfile(cmd)
	if err != nil {
		return nil, err
	}
	token, err := p.Token()
	if err != nil {
		return nil, err
	}
	aliases, err := fieldscope.Load(p.FieldAliases)
	if err != nil {
		return nil, err
	}
	st, err := postgres.New(ctx, cfg.Database.URL, postgres.Options{
		Schema:        cfg.Database.Schema,
		Aliases:       aliases,
		MediaDownload: p.Media.Mode == "download",
	})
	if err != nil {
		return nil, err
	}
	limiter := ratelimit.New(ratelimit.Config{
		RPS:         cfg.RateLimit.RPS,
		Hourly:      cfg.RateLimit.Hourly,
		Daily:       cfg.RateLimit.Daily,
		BytesHourly: int64(cfg.RateLimit.BytesHourlyMB) * 1024 * 1024,
	}, nil)
	pager := mlsgrid.NewPager(mlsgrid.NewClient(token), limiter, nil)
	return &deps{profile: p, store: st, pager: pager, limiter: limiter}, nil
}

// version is set via -ldflags "-X ...cli.version=v0.1.0" by goreleaser;
// falls back to VCS info for `go install` builds.
var version = "dev"

var initDBCmd = &cobra.Command{
	Use:   "init-db",
	Short: "Create the mlsgrid schema and run migrations",
	Long: `Connects to database.url (or MLSGRID_DATABASE_URL), creates the configured
schema if needed, and applies pending migrations. Idempotent — safe to re-run.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		st, err := postgres.New(ctx, cfg.Database.URL, postgres.Options{Schema: cfg.Database.Schema})
		if err != nil {
			return err
		}
		defer st.Close()
		if err := st.Migrate(ctx); err != nil {
			return err
		}
		version, err := st.ContractVersion(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("schema %q is up to date (contract version %s)\n", cfg.Database.Schema, version)
		return nil
	},
}

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "Run the initial full import (MlgCanView eq true)",
	Long: `Pages through the full feed for the selected profile and upserts every
viewable record. Resumable: an interrupted backfill continues from the
persisted @odata.nextLink. Re-running over a non-empty table requires --force.

--since bounds the import to records modified after a point in time. The
bounded import still completes the backfill: incremental sync then covers
everything from that point forward, and older records simply never load.
Combined with --max-pages this keeps a trial run to a handful of requests —
important when the token's rate budget is shared with another consumer.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		force, _ := cmd.Flags().GetBool("force")
		noExpand, _ := cmd.Flags().GetBool("no-expand")
		sinceStr, _ := cmd.Flags().GetString("since")
		maxPages, _ := cmd.Flags().GetInt("max-pages")
		since, err := parseSince(sinceStr, time.Now())
		if err != nil {
			return err
		}

		d, err := profileDeps(ctx, cmd)
		if err != nil {
			return err
		}
		defer d.store.Close()

		expand := []string{"Media", "Rooms", "UnitTypes"}
		if noExpand {
			expand = nil
		}
		for _, resource := range d.resources() {
			err := engine.NewBackfill(d.pager, d.store, engine.BackfillConfig{
				BaseURL:           mlsgrid.DefaultBaseURL,
				Resource:          resource,
				OriginatingSystem: d.profile.OriginatingSystem,
				PageSize:          cfg.Sync.PageSize,
				Expand:            expand,
				Since:             since,
				MaxPages:          maxPages,
				Force:             force,
				Limiter:           d.limiter,
			}).Run(ctx)
			if err != nil {
				return fmt.Errorf("backfilling %s: %w", resource, err)
			}
		}
		return nil
	},
}

// parseSince accepts a look-back duration ("24h") or an absolute time
// ("2026-07-01" or RFC3339).
func parseSince(s string, now time.Time) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d <= 0 {
			return nil, fmt.Errorf("--since duration must be positive, got %q", s)
		}
		t := now.UTC().Add(-d)
		return &t, nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			t = t.UTC()
			return &t, nil
		}
	}
	return nil, fmt.Errorf("--since must be a duration (e.g. 24h, 168h) or a time (2026-07-01 or RFC3339), got %q", s)
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Run incremental replication from the persisted cursor",
	Long: `Fetches records with ModificationTimestamp >= the persisted watermark,
upserting viewable records and hard-deleting MlgCanView=false ones.

Refuses to run before a completed backfill: an empty cursor must never
mean "fetch everything".`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := selectedProfile(cmd); err != nil {
			return err
		}
		once, _ := cmd.Flags().GetBool("once")
		daemon, _ := cmd.Flags().GetBool("daemon")
		if once == daemon {
			return fmt.Errorf("exactly one of --once or --daemon is required")
		}

		ctx := cmd.Context()
		d, err := profileDeps(ctx, cmd)
		if err != nil {
			return err
		}
		defer d.store.Close()

		expand := []string{"Media", "Rooms", "UnitTypes"}
		s := engine.NewSync(d.pager, d.store, engine.SyncConfig{
			BaseURL:           mlsgrid.DefaultBaseURL,
			Resources:         d.resources(),
			OriginatingSystem: d.profile.OriginatingSystem,
			PageSize:          cfg.Sync.PageSize,
			Expand:            expand,
			Interval:          cfg.Sync.Interval,
			HealthAddr:        cfg.Sync.HealthAddr,
			Limiter:           d.limiter,
		})
		s.SetReconciler(engine.NewReconcile(d.pager, d.store, engine.ReconcileConfig{
			BaseURL:           mlsgrid.DefaultBaseURL,
			Resources:         d.resources(),
			OriginatingSystem: d.profile.OriginatingSystem,
			PageSize:          cfg.Sync.PageSize,
			Expand:            expand,
			Limiter:           d.limiter,
		}), cfg.Sync.ReconcileEvery)
		if once {
			res, err := s.RunOnce(ctx)
			if err != nil {
				return err
			}
			slog.Info("caught up",
				"pages", res.Pages, "upserted", res.Upserted,
				"deleted", res.Deleted, "events", res.Events, "skipped", res.Skipped)
			return nil
		}
		return s.RunDaemon(ctx)
	},
}

var reconcileCmd = &cobra.Command{
	Use:   "reconcile",
	Short: "Full-feed key sweep: purge locally-stored records the feed no longer returns",
	Long: `MlgCanView=false records leave the feed after ~7 days; deletions that occur
while mlsgrid-sync is not running are only caught by this pass. The daemon
schedules it automatically per sync.reconcile_every.

The sweep fetches only keys and timestamps, then purges local records missing
remotely and re-fetches records whose remote timestamp is newer. Records that
exist remotely but not locally (normal after a bounded --since backfill) are
counted and logged, and only imported with --include-missing.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		includeMissing, _ := cmd.Flags().GetBool("include-missing")
		d, err := profileDeps(ctx, cmd)
		if err != nil {
			return err
		}
		defer d.store.Close()

		return engine.NewReconcile(d.pager, d.store, engine.ReconcileConfig{
			BaseURL:           mlsgrid.DefaultBaseURL,
			Resources:         d.resources(),
			OriginatingSystem: d.profile.OriginatingSystem,
			PageSize:          cfg.Sync.PageSize,
			Expand:            []string{"Media", "Rooms", "UnitTypes"},
			IncludeMissing:    includeMissing,
			Limiter:           d.limiter,
		}).Run(ctx)
	},
}

var mediaCmd = &cobra.Command{
	Use:   "media",
	Short: "Media download management",
}

var mediaRetryCmd = &cobra.Command{
	Use:   "retry",
	Short: "Re-queue media rows in storage_status=failed for download",
	RunE: func(cmd *cobra.Command, args []string) error {
		return notImplemented("M7")
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show sync cursors and record counts",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		st, err := postgres.New(ctx, cfg.Database.URL, postgres.Options{Schema: cfg.Database.Schema})
		if err != nil {
			return err
		}
		defer st.Close()

		version, err := st.ContractVersion(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("schema: %s (contract %s)\n", cfg.Database.Schema, version)

		names := make([]string, 0, len(cfg.Profiles))
		for n := range cfg.Profiles {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, name := range names {
			p := cfg.Profiles[name]
			resources := p.Resources
			if len(resources) == 0 {
				resources = []string{"Property"}
			}
			for _, r := range resources {
				count, err := st.Count(ctx, r)
				if err != nil {
					return err
				}
				state, err := st.SyncState(ctx, r, p.OriginatingSystem)
				if err != nil {
					return err
				}
				fmt.Printf("\nprofile %s — %s/%s\n", name, r, p.OriginatingSystem)
				fmt.Printf("  stored:     %d\n", count)
				if state == nil {
					fmt.Println("  cursor:     none (backfill has not run)")
					continue
				}
				fmt.Printf("  watermark:  %s\n", fmtTime(state.LastModificationTS))
				fmt.Printf("  backfill:   %s\n", fmtCompletion(state))
				fmt.Printf("  reconciled: %s\n", fmtTime(state.LastFullReconcileAt))
			}
		}

		usage, err := st.RateBudget(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("\nrate budget (persisted)\n")
		fmt.Printf("  hour window %s: %d requests, %.1f MB\n",
			fmtWindow(usage.HourStart), usage.HourRequests, float64(usage.HourBytes)/(1024*1024))
		fmt.Printf("  day window  %s: %d requests\n",
			fmtWindow(usage.DayStart), usage.DayRequests)
		return nil
	},
}

func fmtWindow(t time.Time) string {
	if t.IsZero() {
		return "none"
	}
	return t.UTC().Format(time.RFC3339)
}

func fmtTime(t *time.Time) string {
	if t == nil {
		return "none"
	}
	return t.UTC().Format(time.RFC3339)
}

func fmtCompletion(s *store.SyncState) string {
	if s.BackfillCompletedAt != nil {
		return "completed " + fmtTime(s.BackfillCompletedAt)
	}
	if s.InProgressURL != nil {
		return "in progress (re-run `backfill` to resume)"
	}
	return "not completed"
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the mlsgrid-sync version",
	Run: func(cmd *cobra.Command, args []string) {
		v := version
		if v == "dev" {
			if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
				v = info.Main.Version
			}
		}
		fmt.Println("mlsgrid-sync", v)
	},
}

func init() {
	backfillCmd.Flags().Bool("force", false, "allow re-running backfill over a non-empty table")
	backfillCmd.Flags().Bool("no-expand", false, "skip Rooms/UnitTypes/Media expansions (faster column backfill)")
	backfillCmd.Flags().String("since", "", "bound the import to records modified after a duration ago (24h) or a time (2026-07-01)")
	backfillCmd.Flags().Int("max-pages", 0, "stop after N pages, keeping the resume cursor (0 = unlimited)")
	reconcileCmd.Flags().Bool("include-missing", false, "also import remote records absent locally (turns a bounded import into a fuller one)")
	syncCmd.Flags().Bool("once", false, "run one catch-up pass and exit 0")
	syncCmd.Flags().Bool("daemon", false, "run continuously at sync.interval")
	mediaCmd.AddCommand(mediaRetryCmd)
	rootCmd.AddCommand(initDBCmd, backfillCmd, syncCmd, reconcileCmd, mediaCmd, statusCmd, versionCmd)
}
