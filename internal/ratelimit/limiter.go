// Package ratelimit enforces the MLS Grid usage limits client-side: a
// per-second token bucket, hourly/daily request windows and an hourly byte
// budget aligned to UTC wall-clock boundaries, and a circuit breaker that
// halts the process rather than hammering a suspended token.
//
// The clock is injectable so every behavior is testable without real time.
// Window state is exposed via Snapshot/Restore so a crash-looping process
// cannot launder its budget (persisted to the rate_budget table).
package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ErrCircuitOpen means repeated 429s were observed: the token is likely
// suspended, and continuing would only extend the suspension. The caller must
// halt loudly, not retry.
var ErrCircuitOpen = errors.New("ratelimit: circuit open after repeated 429s — token may be suspended; halting instead of retrying (see docs/compliance.md)")

// Clock abstracts time for testability.
type Clock interface {
	Now() time.Time
	// Sleep blocks for d or until ctx is done.
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RealClock returns the wall clock.
func RealClock() Clock { return realClock{} }

// Config caps requests and bytes. Zero values disable the corresponding
// check (used in tests; production config always sets all of them).
type Config struct {
	RPS         float64
	Burst       int
	Hourly      int   // requests per UTC hour
	Daily       int   // requests per UTC day
	BytesHourly int64 // downloaded bytes per UTC hour
	// CircuitThreshold is the consecutive-429 count that opens the circuit
	// (default 3).
	CircuitThreshold int
}

type window struct {
	start    time.Time
	requests int
	bytes    int64
}

// Limiter is safe for concurrent use.
type Limiter struct {
	mu             sync.Mutex
	cfg            Config
	clock          Clock
	rps            *rate.Limiter
	hour           window
	day            window
	consecutive429 int
}

// New builds a Limiter. A nil clock means the wall clock.
func New(cfg Config, clock Clock) *Limiter {
	if clock == nil {
		clock = RealClock()
	}
	if cfg.CircuitThreshold <= 0 {
		cfg.CircuitThreshold = 3
	}
	if cfg.Burst <= 0 {
		cfg.Burst = 1
	}
	rps := rate.Inf
	if cfg.RPS > 0 {
		rps = rate.Limit(cfg.RPS)
	}
	return &Limiter{
		cfg:   cfg,
		clock: clock,
		rps:   rate.NewLimiter(rps, cfg.Burst),
	}
}

// roll resets windows when the clock crosses a UTC boundary. Alignment to
// wall-clock hours/days (not process start) matches how MLS Grid accounts
// usage.
func (l *Limiter) roll(now time.Time) {
	hourStart := now.UTC().Truncate(time.Hour)
	if !hourStart.Equal(l.hour.start) {
		l.hour = window{start: hourStart}
	}
	dayStart := now.UTC().Truncate(24 * time.Hour)
	if !dayStart.Equal(l.day.start) {
		l.day = window{start: dayStart}
	}
}

// Wait blocks until a request is allowed to start, sleeping across window
// boundaries when a budget is exhausted. It returns ErrCircuitOpen without
// blocking when the circuit is open, and the context error if ctx ends first.
// The request is counted against the hour/day windows when Wait returns nil.
func (l *Limiter) Wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		if l.consecutive429 >= l.cfg.CircuitThreshold {
			l.mu.Unlock()
			return ErrCircuitOpen
		}
		now := l.clock.Now()
		l.roll(now)

		var wait time.Duration
		switch {
		case l.cfg.Daily > 0 && l.day.requests >= l.cfg.Daily:
			wait = l.day.start.Add(24 * time.Hour).Sub(now)
		case l.cfg.Hourly > 0 && l.hour.requests >= l.cfg.Hourly:
			wait = l.hour.start.Add(time.Hour).Sub(now)
		case l.cfg.BytesHourly > 0 && l.hour.bytes >= l.cfg.BytesHourly:
			wait = l.hour.start.Add(time.Hour).Sub(now)
		}
		if wait > 0 {
			l.mu.Unlock()
			if err := l.clock.Sleep(ctx, wait); err != nil {
				return err
			}
			continue
		}

		res := l.rps.ReserveN(now, 1)
		if !res.OK() {
			l.mu.Unlock()
			return errors.New("ratelimit: rps reservation impossible (burst misconfigured)")
		}
		delay := res.DelayFrom(now)
		l.hour.requests++
		l.day.requests++
		l.mu.Unlock()

		if delay > 0 {
			if err := l.clock.Sleep(ctx, delay); err != nil {
				return err
			}
		}
		return nil
	}
}

// RecordResponse accounts a completed request: bytes against the hourly
// download budget, and the status against the circuit breaker (429 increments
// it, any 2xx closes it).
func (l *Limiter) RecordResponse(bytes int64, status int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.roll(l.clock.Now())
	l.hour.bytes += bytes
	switch {
	case status == 429:
		l.consecutive429++
	case status >= 200 && status < 300:
		l.consecutive429 = 0
	}
}

// CircuitOpen reports whether the breaker has tripped.
func (l *Limiter) CircuitOpen() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.consecutive429 >= l.cfg.CircuitThreshold
}

// Usage is a persistable snapshot of window state.
type Usage struct {
	HourStart    time.Time
	HourRequests int
	HourBytes    int64
	DayStart     time.Time
	DayRequests  int
}

// Snapshot returns current window counters for persistence.
func (l *Limiter) Snapshot() Usage {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.roll(l.clock.Now())
	return Usage{
		HourStart:    l.hour.start,
		HourRequests: l.hour.requests,
		HourBytes:    l.hour.bytes,
		DayStart:     l.day.start,
		DayRequests:  l.day.requests,
	}
}

// Restore seeds window counters from persisted state, ignoring windows that
// have already passed. Call once at startup before any Wait.
func (l *Limiter) Restore(u Usage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	if u.HourStart.Equal(now.UTC().Truncate(time.Hour)) {
		l.hour = window{start: u.HourStart, requests: u.HourRequests, bytes: u.HourBytes}
	}
	if u.DayStart.Equal(now.UTC().Truncate(24 * time.Hour)) {
		l.day = window{start: u.DayStart, requests: u.DayRequests}
	}
}
