package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// ErrBackfillRequired means the replication cursor is missing or incomplete.
// Incremental sync refuses to run in that state: an empty cursor must never
// mean "fetch everything" — that mistake once re-downloaded an entire feed.
var ErrBackfillRequired = errors.New("incremental sync requires a completed backfill — run `mlsgrid-sync backfill` first")

// SyncConfig parameterizes incremental replication.
type SyncConfig struct {
	BaseURL string
	// Resources are synced in order each pass (e.g. Property, OpenHouse);
	// each keeps its own cursor.
	Resources         []string
	OriginatingSystem string
	PageSize          int
	Expand            []string
	// PropertySelect narrows Property requests via $select (see
	// BackfillConfig.PropertySelect).
	PropertySelect []string
	// Interval paces daemon passes; each sleep gets up to 10% jitter so
	// multiple daemons never synchronize their load.
	Interval time.Duration
	// HealthAddr serves GET /healthz in daemon mode; empty disables it.
	HealthAddr string
	// Limiter, when set, has its window counters restored from and
	// persisted to the store, so restarts cannot launder rate usage.
	Limiter *ratelimit.Limiter
	Log     *slog.Logger
}

// SyncResult summarizes one catch-up pass.
type SyncResult struct {
	Pages    int
	Upserted int
	Deleted  int
	Skipped  int
	Events   int
}

// Sync runs incremental replication from the persisted watermark.
type Sync struct {
	fetcher Fetcher
	store   store.Store
	cfg     SyncConfig
	now     func() time.Time

	// reconciler, when set, runs during daemon passes once the last full
	// reconcile is older than reconcileEvery.
	reconciler     *Reconcile
	reconcileEvery time.Duration
}

// NewSync wires an incremental sync.
func NewSync(f Fetcher, st store.Store, cfg SyncConfig) *Sync {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Sync{fetcher: f, store: st, cfg: cfg, now: time.Now}
}

// SetReconciler schedules periodic reconcile passes in daemon mode.
func (s *Sync) SetReconciler(r *Reconcile, every time.Duration) {
	s.reconciler = r
	s.reconcileEvery = every
}

// queryURL builds the incremental feed URL. Unlike backfill it must NOT
// filter MlgCanView: revoked records (MlgCanView=false) have to arrive so
// they can be hard-deleted locally — a license obligation.
func (s *Sync) queryURL(resource string, expandable bool, watermark *time.Time) (string, error) {
	q := mlsgrid.Query{
		Resource:                resource,
		OriginatingSystem:       s.cfg.OriginatingSystem,
		ModificationTimestampGE: watermark,
		Top:                     s.cfg.PageSize,
	}
	if expandable {
		q.Expand = s.cfg.Expand
	}
	if resource == "Property" {
		q.Select = s.cfg.PropertySelect
	}
	return q.URL(s.cfg.BaseURL)
}

// RunOnce performs one catch-up pass over every configured resource:
// everything modified at or after each resource's watermark, ge so boundary
// records re-process idempotently. Watermarks advance after each page's
// transaction commits.
func (s *Sync) RunOnce(ctx context.Context) (SyncResult, error) {
	var total SyncResult
	bud := newBudget(s.cfg.Limiter, s.store, s.cfg.Log)
	bud.restore(ctx)
	for _, resource := range s.cfg.Resources {
		res, err := s.runResource(ctx, resource, bud)
		total.Pages += res.Pages
		total.Upserted += res.Upserted
		total.Deleted += res.Deleted
		total.Skipped += res.Skipped
		total.Events += res.Events
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (s *Sync) runResource(ctx context.Context, resource string, bud *budget) (SyncResult, error) {
	var res SyncResult
	log := s.cfg.Log

	ops, err := opsFor(resource)
	if err != nil {
		return res, err
	}
	st, err := s.store.SyncState(ctx, resource, s.cfg.OriginatingSystem)
	if err != nil {
		return res, fmt.Errorf("reading sync state: %w", err)
	}
	switch {
	case st == nil:
		return res, fmt.Errorf("%w (no cursor exists for %s/%s)", ErrBackfillRequired, resource, s.cfg.OriginatingSystem)
	case st.BackfillCompletedAt == nil:
		return res, fmt.Errorf("%w (the %s backfill is incomplete — re-run `backfill` to resume it)", ErrBackfillRequired, resource)
	case st.LastModificationTS == nil:
		return res, fmt.Errorf("%w (the %s cursor watermark is NULL)", ErrBackfillRequired, resource)
	}
	watermark := st.LastModificationTS

	url, err := s.queryURL(resource, ops.expandable, watermark)
	if err != nil {
		return res, err
	}

	rebuiltAfter400 := false
	for url != "" {
		page, err := s.fetcher.Fetch(ctx, url)
		if err != nil {
			var httpErr *mlsgrid.HTTPError
			if errors.As(err, &httpErr) && httpErr.StatusCode == 400 && !rebuiltAfter400 {
				rebuiltAfter400 = true
				if url, err = s.queryURL(resource, ops.expandable, watermark); err != nil {
					return res, err
				}
				log.Warn("page URL rejected (400) — rebuilt from watermark",
					"resource", resource, "watermark", timeOrNone(watermark))
				continue
			}
			return res, fmt.Errorf("fetching %s page %d: %w", resource, res.Pages+1, err)
		}
		rebuiltAfter400 = false
		res.Pages++

		viewable, revokedKeys := ops.split(page.Records)
		stats, err := ops.upsert(ctx, s.store, viewable)
		if err != nil {
			return res, fmt.Errorf("storing %s page %d: %w", resource, res.Pages, err)
		}
		if len(revokedKeys) > 0 {
			// Hard delete (+ delisted event for listings): removing revoked
			// records is a license obligation, not housekeeping.
			if _, err := ops.del(ctx, s.store, revokedKeys); err != nil {
				return res, fmt.Errorf("deleting revoked %s records: %w", resource, err)
			}
			res.Deleted += len(revokedKeys)
		}
		res.Upserted += stats.Inserted + stats.Updated
		res.Skipped += stats.Skipped
		res.Events += stats.Events

		watermark = maxModificationTS(watermark, page.Records)
		if err := s.store.SetSyncState(ctx, store.SyncState{
			Resource:            resource,
			OriginatingSystem:   s.cfg.OriginatingSystem,
			LastModificationTS:  watermark,
			BackfillCompletedAt: st.BackfillCompletedAt,
			LastFullReconcileAt: st.LastFullReconcileAt,
		}); err != nil {
			return res, fmt.Errorf("persisting %s cursor after page %d: %w", resource, res.Pages, err)
		}
		bud.persist(ctx)
		url = page.NextLink
	}
	return res, nil
}

// reconcileDue reports whether any resource's last full reconcile is missing
// or older than the configured cadence.
func (s *Sync) reconcileDue(ctx context.Context) bool {
	if s.reconciler == nil || s.reconcileEvery <= 0 {
		return false
	}
	for _, resource := range s.cfg.Resources {
		st, err := s.store.SyncState(ctx, resource, s.cfg.OriginatingSystem)
		if err != nil || st == nil {
			continue
		}
		if st.LastFullReconcileAt == nil || s.now().Sub(*st.LastFullReconcileAt) >= s.reconcileEvery {
			return true
		}
	}
	return false
}

// RunDaemon loops RunOnce at the configured interval until the context ends.
// A caught-up pass is a normal, quiet outcome — the daemon never exits on
// "nothing to do" (an exit-on-catch-up loop under Restart=on-failure once
// stalled a production sync silently). It does exit, loudly, when the rate
// limiter's circuit opens: the token may be suspended and retrying through a
// suspension only extends it. Cursor-invariant failures also exit — no number
// of retries fixes a missing backfill.
func (s *Sync) RunDaemon(ctx context.Context) error {
	log := s.cfg.Log
	health := &daemonHealth{}

	if s.cfg.HealthAddr != "" {
		srv, addr, err := startHealthServer(s.cfg.HealthAddr, health)
		if err != nil {
			return fmt.Errorf("starting health endpoint: %w", err)
		}
		defer func() { _ = srv.Close() }()
		log.Info("health endpoint listening", "addr", addr)
	}

	for {
		res, err := s.RunOnce(ctx)
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil
		case errors.Is(err, ratelimit.ErrCircuitOpen):
			health.recordFailure(err)
			return fmt.Errorf("halting daemon: %w", err)
		case errors.Is(err, ErrBackfillRequired):
			health.recordFailure(err)
			return err
		case err != nil:
			health.recordFailure(err)
			log.Error("sync pass failed — retrying next interval", "error", err)
		default:
			health.recordSuccess(s.now())
			log.Info("sync pass complete",
				"pages", res.Pages, "upserted", res.Upserted,
				"deleted", res.Deleted, "events", res.Events)
			if s.reconcileDue(ctx) {
				log.Info("running scheduled reconcile pass")
				if rerr := s.reconciler.Run(ctx); rerr != nil {
					if errors.Is(rerr, ratelimit.ErrCircuitOpen) {
						health.recordFailure(rerr)
						return fmt.Errorf("halting daemon: %w", rerr)
					}
					health.recordFailure(rerr)
					log.Error("reconcile pass failed — will retry when next due", "error", rerr)
				}
			}
		}

		jitter := time.Duration(rand.Int64N(int64(s.cfg.Interval)/10 + 1))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.cfg.Interval + jitter):
		}
	}
}

// daemonHealth tracks pass outcomes for the /healthz endpoint.
type daemonHealth struct {
	mu          sync.Mutex
	lastSuccess time.Time
	lastError   string
	lastErrorAt time.Time
	passes      int
	failures    int
}

func (h *daemonHealth) recordSuccess(t time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastSuccess = t.UTC()
	h.passes++
}

func (h *daemonHealth) recordFailure(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastError = err.Error()
	h.lastErrorAt = time.Now().UTC()
	h.failures++
}

// handler serves /healthz: 200 while the most recent pass succeeded (or none
// has finished yet), 503 once the latest outcome is a failure.
func (h *daemonHealth) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		type payload struct {
			Healthy     bool   `json:"healthy"`
			LastSuccess string `json:"last_success,omitempty"`
			LastError   string `json:"last_error,omitempty"`
			LastErrorAt string `json:"last_error_at,omitempty"`
			Passes      int    `json:"passes"`
			Failures    int    `json:"failures"`
		}
		p := payload{
			Healthy:  h.lastErrorAt.IsZero() || h.lastErrorAt.Before(h.lastSuccess),
			Passes:   h.passes,
			Failures: h.failures,
		}
		if !h.lastSuccess.IsZero() {
			p.LastSuccess = h.lastSuccess.Format(time.RFC3339)
		}
		if !h.lastErrorAt.IsZero() {
			p.LastError = h.lastError
			p.LastErrorAt = h.lastErrorAt.Format(time.RFC3339)
		}
		h.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if !p.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(p)
	})
	return mux
}

// startHealthServer listens on addr (host:0 picks a free port) and serves the
// health handler. The returned address reports the actual port.
func startHealthServer(addr string, h *daemonHealth) (*http.Server, string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", err
	}
	srv := &http.Server{Handler: h.handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return srv, ln.Addr().String(), nil
}
