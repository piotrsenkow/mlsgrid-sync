// Package cli defines the mlsgrid-sync command tree. Commands are thin: they
// load config, wire dependencies, and delegate to internal/engine and friends.
package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/piotrsenkow/mlsgrid-sync/internal/config"
)

var (
	cfgFile string
	cfg     *config.Config
)

var rootCmd = &cobra.Command{
	Use:   "mlsgrid-sync",
	Short: "Replicate MLS Grid feeds into your own database",
	Long: `mlsgrid-sync replicates listing data from the MLS Grid API (RESO Web API /
OData) into PostgreSQL, honoring MLS Grid rate limits and delete semantics.

You must hold an executed MLS Grid Data License Agreement and per-MLS approval
for every feed you sync. See docs/compliance.md.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// version and help need no config
		if cmd.Name() == "version" || cmd.Name() == "help" {
			return nil
		}
		var err error
		cfg, err = config.Load(cfgFile)
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}
		return nil
	},
}

// Execute runs the root command. SIGINT/SIGTERM cancel the command context,
// letting long-running commands (backfill, sync --daemon) finish their
// current page cleanly before exiting.
func Execute() error {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return rootCmd.ExecuteContext(ctx)
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "path to config file (default: ./mlsgrid-sync.yaml, then $XDG_CONFIG_HOME/mlsgrid-sync/config.yaml)")
	rootCmd.PersistentFlags().StringP("profile", "p", "", "named MLS profile from config to operate on (required when config defines more than one)")
}

// notImplemented returns the standard error for commands whose milestone has
// not been built yet. The milestone reference keeps expectations honest.
func notImplemented(milestone string) error {
	return fmt.Errorf("not implemented yet — scheduled for %s (see docs/ROADMAP.md)", milestone)
}

// selectedProfile resolves the --profile flag against loaded config.
func selectedProfile(cmd *cobra.Command) (*config.Profile, error) {
	name, _ := cmd.Flags().GetString("profile")
	return cfg.Profile(name)
}
