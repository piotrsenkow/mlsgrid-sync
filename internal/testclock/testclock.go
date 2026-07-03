// Package testclock provides a deterministic Clock for tests: Sleep advances
// virtual time instantly and records every requested duration.
package testclock

import (
	"context"
	"sync"
	"time"
)

// Clock implements ratelimit.Clock with manually controlled time.
type Clock struct {
	mu    sync.Mutex
	now   time.Time
	slept []time.Duration
}

// At starts a clock at the given instant.
func At(t time.Time) *Clock { return &Clock{now: t} }

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Sleep advances time by d immediately (no real blocking) and records d.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	c.slept = append(c.slept, d)
	return nil
}

// Advance moves time forward without recording a sleep.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Slept returns a copy of all recorded sleep durations.
func (c *Clock) Slept() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.slept...)
}
