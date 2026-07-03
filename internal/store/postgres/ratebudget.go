package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
)

// RateBudget loads the most recent hour and day window counters. Windows that
// have already passed are still returned — ratelimit.Restore ignores them —
// so this needs no clock of its own.
func (s *Store) RateBudget(ctx context.Context) (ratelimit.Usage, error) {
	var u ratelimit.Usage
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT DISTINCT ON (window_kind) window_kind, window_start, requests, bytes_downloaded
		 FROM %s ORDER BY window_kind, window_start DESC`, s.table("rate_budget")))
	if err != nil {
		return u, err
	}
	var kind string
	var start time.Time
	var requests int
	var bytes int64
	if _, err := pgx.ForEachRow(rows, []any{&kind, &start, &requests, &bytes}, func() error {
		switch kind {
		case "hour":
			u.HourStart = start
			u.HourRequests = requests
			u.HourBytes = bytes
		case "day":
			u.DayStart = start
			u.DayRequests = requests
		}
		return nil
	}); err != nil {
		return u, err
	}
	return u, nil
}

// SetRateBudget upserts the current window counters and prunes stale windows.
func (s *Store) SetRateBudget(ctx context.Context, u ratelimit.Usage) error {
	b := &pgx.Batch{}
	upsert := fmt.Sprintf(
		`INSERT INTO %s (window_kind, window_start, requests, bytes_downloaded)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (window_kind, window_start) DO UPDATE SET
		     requests = EXCLUDED.requests,
		     bytes_downloaded = EXCLUDED.bytes_downloaded,
		     updated_at = now()`, s.table("rate_budget"))
	if !u.HourStart.IsZero() {
		b.Queue(upsert, "hour", u.HourStart, u.HourRequests, u.HourBytes)
	}
	if !u.DayStart.IsZero() {
		b.Queue(upsert, "day", u.DayStart, u.DayRequests, int64(0))
	}
	b.Queue(fmt.Sprintf(
		"DELETE FROM %s WHERE window_start < now() - interval '48 hours'",
		s.table("rate_budget")))

	br := s.pool.SendBatch(ctx, b)
	for i := 0; i < b.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("persisting rate budget: %w", err)
		}
	}
	return br.Close()
}
