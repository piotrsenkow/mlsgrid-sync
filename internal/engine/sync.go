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
	BaseURL           string
	Resource          string
	OriginatingSystem string
	PageSize          int
	Expand            []string
	// Interval paces daemon passes; each sleep gets up to 10% jitter so
	// multiple daemons never synchronize their load.
	Interval time.Duration
	// HealthAddr serves GET /healthz in daemon mode; empty disables it.
	HealthAddr string
	Log        *slog.Logger
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
}

// NewSync wires an incremental sync.
func NewSync(f Fetcher, st store.Store, cfg SyncConfig) *Sync {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Sync{fetcher: f, store: st, cfg: cfg, now: time.Now}
}

// queryURL builds the incremental feed URL. Unlike backfill it must NOT
// filter MlgCanView: revoked records (MlgCanView=false) have to arrive so
// they can be hard-deleted locally — a license obligation.
func (s *Sync) queryURL(watermark *time.Time) (string, error) {
	return mlsgrid.Query{
		Resource:                s.cfg.Resource,
		OriginatingSystem:       s.cfg.OriginatingSystem,
		ModificationTimestampGE: watermark,
		Expand:                  s.cfg.Expand,
		Top:                     s.cfg.PageSize,
	}.URL(s.cfg.BaseURL)
}

// RunOnce performs one catch-up pass: everything modified at or after the
// watermark, ge so boundary records re-process idempotently. The watermark
// advances after each page's transaction commits.
func (s *Sync) RunOnce(ctx context.Context) (SyncResult, error) {
	var res SyncResult
	log := s.cfg.Log

	st, err := s.store.SyncState(ctx, s.cfg.Resource, s.cfg.OriginatingSystem)
	if err != nil {
		return res, fmt.Errorf("reading sync state: %w", err)
	}
	switch {
	case st == nil:
		return res, fmt.Errorf("%w (no cursor exists for %s/%s)", ErrBackfillRequired, s.cfg.Resource, s.cfg.OriginatingSystem)
	case st.BackfillCompletedAt == nil:
		return res, fmt.Errorf("%w (a backfill is incomplete — re-run `backfill` to resume it)", ErrBackfillRequired)
	case st.LastModificationTS == nil:
		return res, fmt.Errorf("%w (cursor watermark is NULL)", ErrBackfillRequired)
	}
	watermark := st.LastModificationTS

	url, err := s.queryURL(watermark)
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
				if url, err = s.queryURL(watermark); err != nil {
					return res, err
				}
				log.Warn("page URL rejected (400) — rebuilt from watermark",
					"watermark", timeOrNone(watermark))
				continue
			}
			return res, fmt.Errorf("fetching page %d: %w", res.Pages+1, err)
		}
		rebuiltAfter400 = false
		res.Pages++

		viewable, revokedKeys := splitViewable(page.Records)
		stats, err := s.store.UpsertProperties(ctx, viewable)
		if err != nil {
			return res, fmt.Errorf("storing page %d: %w", res.Pages, err)
		}
		if len(revokedKeys) > 0 {
			// Hard delete + delisted event: removing revoked records is a
			// license obligation, not housekeeping.
			if _, err := s.store.DeleteProperties(ctx, revokedKeys); err != nil {
				return res, fmt.Errorf("deleting revoked records: %w", err)
			}
			res.Deleted += len(revokedKeys)
		}
		res.Upserted += stats.Inserted + stats.Updated
		res.Skipped += stats.Skipped
		res.Events += stats.Events

		watermark = maxModificationTS(watermark, page.Records)
		if err := s.store.SetSyncState(ctx, store.SyncState{
			Resource:            s.cfg.Resource,
			OriginatingSystem:   s.cfg.OriginatingSystem,
			LastModificationTS:  watermark,
			BackfillCompletedAt: st.BackfillCompletedAt,
			LastFullReconcileAt: st.LastFullReconcileAt,
		}); err != nil {
			return res, fmt.Errorf("persisting cursor after page %d: %w", res.Pages, err)
		}
		url = page.NextLink
	}
	return res, nil
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
