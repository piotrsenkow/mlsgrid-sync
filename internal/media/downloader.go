package media

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// Store is the slice of the store the downloader needs: the pending queue,
// outcome marking, and the persisted rate budget it shares with the feed.
type Store interface {
	PendingMedia(ctx context.Context, afterKey string, limit int) ([]store.MediaItem, error)
	MarkMediaDownloaded(ctx context.Context, mediaKey, localPath, contentType string, bytes int64) error
	MarkMediaFailed(ctx context.Context, mediaKey string, permanent bool) error
	RateBudget(ctx context.Context) (ratelimit.Usage, error)
	SetRateBudget(ctx context.Context, u ratelimit.Usage) error
}

// Config parameterizes a download run.
type Config struct {
	// Token is the MLS Grid access token, sent as the User-Agent header —
	// mandatory on media downloads since 2026-06-01. It is NOT sent as an
	// Authorization header; media URLs authenticate solely via User-Agent.
	Token string
	Sink  Sink
	// Workers bounds concurrent downloads (default 4). The rate limiter is
	// still the real governor; extra workers just hide per-file latency.
	Workers int
	// MaxFiles stops the run after N attempts (0 = drain the queue). Use it
	// to bound runs when the token's budget is shared with another consumer.
	MaxFiles int
	// MaxFileBytes rejects absurdly large responses (default 64 MiB).
	MaxFileBytes int64
	// MaxFailures is the failure_count at which a row is parked as 'failed'
	// (default 3, matching the schema contract).
	MaxFailures int
	Limiter     *ratelimit.Limiter
	HTTPClient  *http.Client
	Log         *slog.Logger
}

// Stats summarizes a download run.
type Stats struct {
	Downloaded int
	Failed     int
	Bytes      int64
}

// Downloader drains the pending-media queue through a bounded worker pool.
// Per-URL failures are tolerated (the production feed routinely references
// dead media hosts): a failed file is retried on later runs until it parks
// at 'failed', and never blocks the rest of the queue. Only systemic
// problems — an open rate-limit circuit, a failing sink or database — abort
// the run.
type Downloader struct {
	st  Store
	cfg Config

	mu    sync.Mutex
	stats Stats
}

// NewDownloader applies defaults and returns a ready Downloader.
func NewDownloader(st Store, cfg Config) *Downloader {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.MaxFileBytes <= 0 {
		cfg.MaxFileBytes = 64 << 20
	}
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = 3
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 2 * time.Minute}
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Downloader{st: st, cfg: cfg}
}

// claimBatch keeps queue queries small while amortizing them across many
// downloads; budget snapshots persist at this cadence too.
const claimBatch = 100

// Run makes one pass over the currently-pending queue. The keyset cursor
// only moves forward, so rows whose failed attempt flips them back to
// 'pending' behind it are not retried until the next run.
func (d *Downloader) Run(ctx context.Context) (Stats, error) {
	usage, err := d.st.RateBudget(ctx)
	if err != nil {
		return Stats{}, fmt.Errorf("media: restoring rate budget: %w", err)
	}
	d.cfg.Limiter.Restore(usage)

	after := ""
	attempted := 0
	for {
		limit := claimBatch
		if d.cfg.MaxFiles > 0 {
			if remaining := d.cfg.MaxFiles - attempted; remaining < limit {
				limit = remaining
			}
		}
		if limit <= 0 {
			d.cfg.Log.Info("media: max-files reached", "attempted", attempted)
			break
		}
		items, err := d.st.PendingMedia(ctx, after, limit)
		if err != nil {
			return d.snapshot(), fmt.Errorf("media: claiming pending rows: %w", err)
		}
		if len(items) == 0 {
			break
		}
		after = items[len(items)-1].MediaKey
		attempted += len(items)

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(d.cfg.Workers)
		for _, it := range items {
			g.Go(func() error { return d.one(gctx, it) })
		}
		werr := g.Wait()
		if err := d.persistBudget(ctx); err != nil {
			return d.snapshot(), err
		}
		if werr != nil {
			return d.snapshot(), werr
		}
		s := d.snapshot()
		d.cfg.Log.Info("media: progress",
			"downloaded", s.Downloaded, "failed", s.Failed,
			"mb", fmt.Sprintf("%.1f", float64(s.Bytes)/(1024*1024)))
	}
	return d.snapshot(), nil
}

// persistBudget snapshots limiter windows to the database, so an interrupted
// run cannot launder the bytes it already spent.
func (d *Downloader) persistBudget(ctx context.Context) error {
	// context.WithoutCancel: the final persist must succeed even when the
	// run is aborting on ctx cancellation.
	if err := d.st.SetRateBudget(context.WithoutCancel(ctx), d.cfg.Limiter.Snapshot()); err != nil {
		return fmt.Errorf("media: persisting rate budget: %w", err)
	}
	return nil
}

// one downloads a single file. It returns nil for per-file failures (marked
// in the store) and an error only for systemic conditions that must abort
// the whole run.
func (d *Downloader) one(ctx context.Context, it store.MediaItem) error {
	// A file that keeps failing parks at 'failed' once its budget is spent.
	permanent := it.FailureCount+1 >= d.cfg.MaxFailures

	if it.MediaURL == "" {
		// Nothing to fetch, ever.
		return d.fail(ctx, it, true, "no MediaURL")
	}
	if err := d.cfg.Limiter.Wait(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, it.MediaURL, nil)
	if err != nil {
		return d.fail(ctx, it, true, fmt.Sprintf("bad URL: %v", err))
	}
	req.Header.Set("User-Agent", d.cfg.Token)

	resp, err := d.cfg.HTTPClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return d.fail(ctx, it, permanent, fmt.Sprintf("request failed: %v", err))
	}
	defer func() { _ = resp.Body.Close() }()

	data, readErr := io.ReadAll(io.LimitReader(resp.Body, d.cfg.MaxFileBytes+1))
	d.cfg.Limiter.RecordResponse(int64(len(data)), resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		if d.cfg.Limiter.CircuitOpen() {
			return fmt.Errorf("media: %w", ratelimit.ErrCircuitOpen)
		}
		return d.fail(ctx, it, permanent, fmt.Sprintf("HTTP %d", resp.StatusCode))
	}
	if readErr != nil {
		return d.fail(ctx, it, permanent, fmt.Sprintf("reading body: %v", readErr))
	}
	if int64(len(data)) > d.cfg.MaxFileBytes {
		// Deterministically too large — retrying cannot help.
		return d.fail(ctx, it, true, fmt.Sprintf("larger than %d bytes", d.cfg.MaxFileBytes))
	}

	contentType := resp.Header.Get("Content-Type")
	localPath, err := d.cfg.Sink.Store(ctx, it.MediaKey, contentType, data)
	if err != nil {
		// A failing sink (disk full, object store down) fails everything —
		// abort instead of burning the queue's failure budgets.
		return fmt.Errorf("media: sink store: %w", err)
	}
	if err := d.st.MarkMediaDownloaded(ctx, it.MediaKey, localPath, contentType, int64(len(data))); err != nil {
		return fmt.Errorf("media: marking %s downloaded: %w", it.MediaKey, err)
	}
	d.mu.Lock()
	d.stats.Downloaded++
	d.stats.Bytes += int64(len(data))
	d.mu.Unlock()
	return nil
}

// fail records a per-file failure; only a store error escalates it.
func (d *Downloader) fail(ctx context.Context, it store.MediaItem, permanent bool, reason string) error {
	d.cfg.Log.Warn("media: download failed",
		"media_key", it.MediaKey, "listing_key", it.ListingKey,
		"permanent", permanent, "reason", reason)
	if err := d.st.MarkMediaFailed(ctx, it.MediaKey, permanent); err != nil {
		return fmt.Errorf("media: marking %s failed: %w", it.MediaKey, err)
	}
	d.mu.Lock()
	d.stats.Failed++
	d.mu.Unlock()
	return nil
}

func (d *Downloader) snapshot() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats
}
