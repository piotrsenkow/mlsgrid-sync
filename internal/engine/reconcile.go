package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// ReconcileConfig parameterizes a full-feed consistency pass.
type ReconcileConfig struct {
	BaseURL           string
	Resources         []string
	OriginatingSystem string
	PageSize          int
	Expand            []string
	// IncludeMissing re-fetches remote records absent locally. Off by
	// default: after a bounded backfill (--since) most of the feed is
	// deliberately not stored, and pulling it in would silently turn a
	// bounded import into a full one. Purging and stale-refresh are
	// unaffected either way.
	IncludeMissing bool
	// ChunkSize bounds keys per re-fetch request (URL length); default 50.
	ChunkSize int
	// PropertySelect narrows Property re-fetches via $select (see
	// BackfillConfig.PropertySelect). The key sweep always uses its own,
	// narrower $select regardless.
	PropertySelect []string
	Limiter        *ratelimit.Limiter
	Log            *slog.Logger
}

// ReconcileResult summarizes one resource's pass.
type ReconcileResult struct {
	RemoteKeys   int
	LocalKeys    int
	Purged       int64
	Refetched    int
	MissingLocal int
}

// Reconcile diffs the full remote key set against local storage. It exists
// because MlgCanView=false records leave the feed after ~7 days: any deletion
// that happens while sync is not running is only ever caught here. The sweep
// uses $select to fetch just keys and timestamps, so a full pass costs a
// fraction of a backfill's bandwidth.
type Reconcile struct {
	fetcher Fetcher
	store   store.Store
	cfg     ReconcileConfig
	now     func() time.Time
}

// NewReconcile wires a reconcile pass.
func NewReconcile(f Fetcher, st store.Store, cfg ReconcileConfig) *Reconcile {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 50
	}
	return &Reconcile{fetcher: f, store: st, cfg: cfg, now: time.Now}
}

// Run reconciles every configured resource.
func (r *Reconcile) Run(ctx context.Context) error {
	bud := newBudget(r.cfg.Limiter, r.store, r.cfg.Log)
	bud.restore(ctx)
	for _, resource := range r.cfg.Resources {
		res, err := r.runResource(ctx, resource, bud)
		if err != nil {
			return fmt.Errorf("reconciling %s: %w", resource, err)
		}
		r.cfg.Log.Info("reconcile complete",
			"resource", resource,
			"remote_keys", res.RemoteKeys,
			"local_keys", res.LocalKeys,
			"purged", res.Purged,
			"refetched", res.Refetched,
			"missing_local", res.MissingLocal,
			"include_missing", r.cfg.IncludeMissing)
	}
	return nil
}

func (r *Reconcile) runResource(ctx context.Context, resource string, bud *budget) (ReconcileResult, error) {
	var res ReconcileResult
	ops, err := opsFor(resource)
	if err != nil {
		return res, err
	}

	st, err := r.store.SyncState(ctx, resource, r.cfg.OriginatingSystem)
	if err != nil {
		return res, fmt.Errorf("reading sync state: %w", err)
	}
	if st == nil || st.BackfillCompletedAt == nil {
		return res, fmt.Errorf("%w (reconcile diffs against a complete local set)", ErrBackfillRequired)
	}

	// 1. Sweep the remote key set: keys + timestamps only, MlgCanView true.
	remote, err := r.sweep(ctx, resource, ops, bud)
	if err != nil {
		return res, err
	}
	res.RemoteKeys = len(remote)

	// 2. Load the local key set.
	local, err := r.store.ListKeys(ctx, resource)
	if err != nil {
		return res, fmt.Errorf("listing local keys: %w", err)
	}
	res.LocalKeys = len(local)

	// 3. Diff.
	var purge, refetch []string
	for key := range local {
		if _, ok := remote[key]; !ok {
			purge = append(purge, key)
		}
	}
	for key, remoteTS := range remote {
		localTS, stored := local[key]
		switch {
		case !stored:
			res.MissingLocal++
			if r.cfg.IncludeMissing {
				refetch = append(refetch, key)
			}
		case remoteTS.After(localTS):
			refetch = append(refetch, key)
		}
	}

	// 4. Purge local records the feed no longer returns — deletions missed
	// while sync was down. This is the license-safety half of reconcile.
	for _, chunk := range chunks(purge, 500) {
		n, err := ops.del(ctx, r.store, chunk)
		if err != nil {
			return res, fmt.Errorf("purging: %w", err)
		}
		res.Purged += n
	}

	// 5. Re-fetch stale (and optionally missing) records with expansions.
	for _, chunk := range chunks(refetch, r.cfg.ChunkSize) {
		q := mlsgrid.Query{
			Resource:          resource,
			OriginatingSystem: r.cfg.OriginatingSystem,
			MlgCanViewTrue:    true,
			KeyField:          ops.keyField,
			Keys:              chunk,
			Top:               r.cfg.PageSize,
		}
		if ops.expandable {
			q.Expand = r.cfg.Expand
		}
		if resource == "Property" {
			q.Select = r.cfg.PropertySelect
		}
		url, err := q.URL(r.cfg.BaseURL)
		if err != nil {
			return res, err
		}
		for url != "" {
			page, err := r.fetcher.Fetch(ctx, url)
			if err != nil {
				return res, fmt.Errorf("re-fetching %d records: %w", len(chunk), err)
			}
			viewable, revoked := ops.split(page.Records)
			stats, err := ops.upsert(ctx, r.store, viewable)
			if err != nil {
				return res, err
			}
			if len(revoked) > 0 {
				if _, err := ops.del(ctx, r.store, revoked); err != nil {
					return res, err
				}
			}
			res.Refetched += stats.Inserted + stats.Updated
			bud.persist(ctx)
			url = page.NextLink
		}
	}

	// 6. Stamp completion, preserving the sync cursor.
	done := r.now().UTC()
	st.LastFullReconcileAt = &done
	if err := r.store.SetSyncState(ctx, *st); err != nil {
		return res, fmt.Errorf("stamping reconcile completion: %w", err)
	}
	bud.persist(ctx)
	return res, nil
}

// sweep pages the full viewable feed fetching only the key and timestamp.
//
// Known limitation: feeds beyond the API's deep-paging limit (~500K records)
// may 400 mid-sweep; windowed sweeps are a roadmap item. The error message
// makes the cause visible rather than retrying into it.
func (r *Reconcile) sweep(ctx context.Context, resource string, ops resourceOps, bud *budget) (map[string]time.Time, error) {
	url, err := mlsgrid.Query{
		Resource:          resource,
		OriginatingSystem: r.cfg.OriginatingSystem,
		MlgCanViewTrue:    true,
		Select:            []string{ops.keyField, "ModificationTimestamp"},
		Top:               r.cfg.PageSize,
	}.URL(r.cfg.BaseURL)
	if err != nil {
		return nil, err
	}

	remote := make(map[string]time.Time)
	pages := 0
	for url != "" {
		page, err := r.fetcher.Fetch(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("sweep page %d (very large feeds can exceed the API's deep-paging limit; windowed sweeps are planned): %w", pages+1, err)
		}
		pages++
		for _, rec := range page.Records {
			key := rec.String(ops.keyField)
			ts, ok := rec.ModificationTimestamp()
			if key == "" || !ok {
				continue
			}
			remote[key] = ts
		}
		bud.persist(ctx)
		url = page.NextLink
	}
	return remote, nil
}

// chunks splits keys into bounded slices.
func chunks(keys []string, size int) [][]string {
	if len(keys) == 0 {
		return nil
	}
	var out [][]string
	for start := 0; start < len(keys); start += size {
		end := min(start+size, len(keys))
		out = append(out, keys[start:end])
	}
	return out
}
