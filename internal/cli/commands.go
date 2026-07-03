package cli

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

// version is set via -ldflags "-X ...cli.version=v0.1.0" by goreleaser;
// falls back to VCS info for `go install` builds.
var version = "dev"

var initDBCmd = &cobra.Command{
	Use:   "init-db",
	Short: "Create the mlsgrid schema and run migrations",
	RunE: func(cmd *cobra.Command, args []string) error {
		return notImplemented("M3")
	},
}

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "Run the initial full import (MlgCanView eq true)",
	Long: `Pages through the full feed for the selected profile and upserts every
viewable record. Resumable: an interrupted backfill continues from the
persisted @odata.nextLink. Re-running over a non-empty table requires --force.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := selectedProfile(cmd); err != nil {
			return err
		}
		return notImplemented("M4")
	},
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
		return notImplemented("M5")
	},
}

var reconcileCmd = &cobra.Command{
	Use:   "reconcile",
	Short: "Full-feed key sweep: purge locally-stored records the feed no longer returns",
	Long: `MlgCanView=false records leave the feed after ~7 days; deletions that occur
while mlsgrid-sync is not running are only caught by this pass. The daemon
schedules it automatically per sync.reconcile_every.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := selectedProfile(cmd); err != nil {
			return err
		}
		return notImplemented("M6")
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
	Short: "Show sync cursors, record counts, and rate-budget usage",
	RunE: func(cmd *cobra.Command, args []string) error {
		return notImplemented("M5")
	},
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
	syncCmd.Flags().Bool("once", false, "run one catch-up pass and exit 0")
	syncCmd.Flags().Bool("daemon", false, "run continuously at sync.interval")
	mediaCmd.AddCommand(mediaRetryCmd)
	rootCmd.AddCommand(initDBCmd, backfillCmd, syncCmd, reconcileCmd, mediaCmd, statusCmd, versionCmd)
}
