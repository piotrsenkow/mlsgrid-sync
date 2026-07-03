// Package engine orchestrates replication: it drives the mlsgrid Pager
// against the store, owning cursor semantics (backfill here; incremental sync
// and reconcile arrive in later milestones).
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// Fetcher is the paging API the engine drives; *mlsgrid.Pager satisfies it.
type Fetcher interface {
	Fetch(ctx context.Context, url string) (*mlsgrid.PageResult, error)
}

// BackfillConfig parameterizes one backfill run.
type BackfillConfig struct {
	BaseURL           string
	Resource          string
	OriginatingSystem string
	// PageSize is $top (the API caps it at 1000 with $expand).
	PageSize int
	// Expand lists child resources; nil skips expansions entirely.
	Expand []string
	// Since bounds the import to records modified at or after this time.
	// A bounded import still counts as a completed backfill: incremental
	// sync will keep everything from Since forward up to date, and older
	// records simply never arrive. Useful for trials and for tokens shared
	// with other consumers, where a full-feed sweep would eat the shared
	// rate budget for hours.
	Since *time.Time
	// MaxPages stops the run after N pages, leaving the resume cursor in
	// place; 0 means unlimited. A later run continues where this one left
	// off. Combined with Since, this makes a cheap smoke test possible.
	MaxPages int
	// Force allows starting over when data already exists.
	Force bool
	Log   *slog.Logger
}

// Backfill runs the initial full import for one resource.
type Backfill struct {
	fetcher Fetcher
	store   store.Store
	cfg     BackfillConfig
	now     func() time.Time
}

// NewBackfill wires a backfill run.
func NewBackfill(f Fetcher, st store.Store, cfg BackfillConfig) *Backfill {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Backfill{fetcher: f, store: st, cfg: cfg, now: time.Now}
}

// queryURL builds the filtered feed URL from scratch. Backfill always filters
// MlgCanView eq true; ge is used for the timestamp bound so boundary records
// re-process idempotently instead of being skipped.
func (b *Backfill) queryURL(since *time.Time) (string, error) {
	return mlsgrid.Query{
		Resource:                b.cfg.Resource,
		OriginatingSystem:       b.cfg.OriginatingSystem,
		MlgCanViewTrue:          true,
		ModificationTimestampGE: since,
		Expand:                  b.cfg.Expand,
		Top:                     b.cfg.PageSize,
	}.URL(b.cfg.BaseURL)
}

// Run executes the backfill: fresh, resumed, or refused.
//
//   - A persisted in_progress_url resumes automatically.
//   - Existing data without a resume cursor requires Force.
//   - The watermark and resume cursor persist after every page transaction,
//     so a crash or MaxPages stop loses at most one page of work.
//   - A 400 response mid-run (stale nextLink, or the API's deep-$skip limit)
//     rebuilds the URL from the watermark and continues.
func (b *Backfill) Run(ctx context.Context) error {
	log := b.cfg.Log
	started := b.now().UTC()

	st, err := b.store.SyncState(ctx, b.cfg.Resource, b.cfg.OriginatingSystem)
	if err != nil {
		return fmt.Errorf("reading sync state: %w", err)
	}

	var watermark, reconcileAt *time.Time
	var url string
	resuming := false
	if st != nil {
		watermark = st.LastModificationTS
		reconcileAt = st.LastFullReconcileAt
		if st.InProgressURL != nil && *st.InProgressURL != "" {
			url = *st.InProgressURL
			resuming = true
		}
	}

	if !resuming {
		if st != nil && st.BackfillCompletedAt != nil && !b.cfg.Force {
			return fmt.Errorf("backfill for %s/%s already completed at %s — run `sync` for updates, or re-run with --force to start over",
				b.cfg.Resource, b.cfg.OriginatingSystem, st.BackfillCompletedAt.UTC().Format(time.RFC3339))
		}
		n, err := b.store.PropertyCount(ctx)
		if err != nil {
			return fmt.Errorf("counting stored properties: %w", err)
		}
		if n > 0 && !b.cfg.Force {
			return fmt.Errorf("%d properties already stored and no backfill is in progress — use --force to re-import over them", n)
		}
		watermark = b.cfg.Since
		if url, err = b.queryURL(b.cfg.Since); err != nil {
			return err
		}
	}

	log.Info("backfill starting",
		"resource", b.cfg.Resource,
		"originating_system", b.cfg.OriginatingSystem,
		"resuming", resuming,
		"since", timeOrNone(b.cfg.Since))

	var pages, upserted, deleted, skipped, events int
	rebuiltAfter400 := false
	for url != "" {
		if b.cfg.MaxPages > 0 && pages >= b.cfg.MaxPages {
			log.Info("page cap reached — resume cursor persisted; re-run backfill to continue",
				"pages", pages, "max_pages", b.cfg.MaxPages)
			return nil
		}

		page, err := b.fetcher.Fetch(ctx, url)
		if err != nil {
			var httpErr *mlsgrid.HTTPError
			if errors.As(err, &httpErr) && httpErr.StatusCode == 400 && !rebuiltAfter400 && watermark != nil {
				// Stale nextLink or the API's deep-paging limit: rebuild a
				// timestamp-filtered URL from the watermark. The feed is
				// ordered by ModificationTimestamp, so everything before the
				// watermark is already stored.
				rebuiltAfter400 = true
				if url, err = b.queryURL(watermark); err != nil {
					return err
				}
				log.Warn("page URL rejected (400) — rebuilt from watermark",
					"watermark", watermark.UTC().Format(time.RFC3339Nano))
				continue
			}
			return fmt.Errorf("fetching page %d (progress is persisted; re-running resumes): %w", pages+1, err)
		}
		rebuiltAfter400 = false
		pages++

		viewable, revokedKeys := splitViewable(page.Records)
		stats, err := b.store.UpsertProperties(ctx, viewable)
		if err != nil {
			return fmt.Errorf("storing page %d: %w", pages, err)
		}
		if len(revokedKeys) > 0 {
			// The MlgCanView filter should exclude these, but a revocation
			// mid-pagination can slip through; honor it either way.
			if _, err := b.store.DeleteProperties(ctx, revokedKeys); err != nil {
				return fmt.Errorf("deleting revoked records on page %d: %w", pages, err)
			}
			deleted += len(revokedKeys)
		}
		upserted += stats.Inserted + stats.Updated
		skipped += stats.Skipped
		events += stats.Events

		watermark = maxModificationTS(watermark, page.Records)
		next := page.NextLink
		state := store.SyncState{
			Resource:            b.cfg.Resource,
			OriginatingSystem:   b.cfg.OriginatingSystem,
			LastModificationTS:  watermark,
			LastFullReconcileAt: reconcileAt,
		}
		if next != "" {
			state.InProgressURL = &next
		}
		if err := b.store.SetSyncState(ctx, state); err != nil {
			return fmt.Errorf("persisting cursor after page %d: %w", pages, err)
		}

		log.Info("page stored",
			"page", pages,
			"records", len(page.Records),
			"skipped", stats.Skipped,
			"wire_bytes", page.WireBytes,
			"watermark", timeOrNone(watermark))
		url = next
	}

	// An empty feed yields no watermark; use the run start so incremental
	// sync has a valid cursor covering everything after this backfill.
	if watermark == nil {
		watermark = &started
	}
	done := b.now().UTC()
	if err := b.store.SetSyncState(ctx, store.SyncState{
		Resource:            b.cfg.Resource,
		OriginatingSystem:   b.cfg.OriginatingSystem,
		LastModificationTS:  watermark,
		BackfillCompletedAt: &done,
		LastFullReconcileAt: reconcileAt,
	}); err != nil {
		return fmt.Errorf("marking backfill complete: %w", err)
	}

	log.Info("backfill complete",
		"pages", pages,
		"upserted", upserted,
		"deleted", deleted,
		"skipped", skipped,
		"events", events,
		"duration", done.Sub(started).Round(time.Second).String(),
		"watermark", timeOrNone(watermark))
	return nil
}

// splitViewable partitions a page into storable records and revoked keys.
func splitViewable(recs []mlsgrid.Record) (viewable []mlsgrid.Record, revokedKeys []string) {
	for _, rec := range recs {
		if rec.CanView() {
			viewable = append(viewable, rec)
			continue
		}
		if key := rec.ListingKey(); key != "" {
			revokedKeys = append(revokedKeys, key)
		}
	}
	return viewable, revokedKeys
}

// maxModificationTS advances the watermark over a page. The feed is ordered
// by ModificationTimestamp, but scanning every record costs nothing and does
// not depend on that guarantee.
func maxModificationTS(current *time.Time, recs []mlsgrid.Record) *time.Time {
	max := current
	for _, rec := range recs {
		if ts, ok := rec.ModificationTimestamp(); ok {
			if max == nil || ts.After(*max) {
				t := ts
				max = &t
			}
		}
	}
	return max
}

func timeOrNone(t *time.Time) string {
	if t == nil {
		return "none"
	}
	return t.UTC().Format(time.RFC3339Nano)
}
