package mlsgrid

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
)

// Pager wraps Client with the retry policy the API demands:
//
//   - every request first clears the rate limiter (which may sleep across a
//     window boundary or refuse with an open circuit)
//   - 429: honor Retry-After, else exponential backoff 30s→16m; repeated 429s
//     trip the limiter's circuit breaker and the run halts
//   - 5xx and transport errors: jittered exponential backoff, bounded retries
//   - other 4xx (e.g. a stale @odata.nextLink): returned to the caller, which
//     knows how to rebuild the URL from its cursor
type Pager struct {
	client  *Client
	limiter *ratelimit.Limiter
	clock   ratelimit.Clock
	// max5xx bounds retries for server/transport errors (default 5).
	max5xx int
}

// NewPager assembles the retrying fetcher. A nil clock means the wall clock.
func NewPager(client *Client, limiter *ratelimit.Limiter, clock ratelimit.Clock) *Pager {
	if clock == nil {
		clock = ratelimit.RealClock()
	}
	return &Pager{client: client, limiter: limiter, clock: clock, max5xx: 5}
}

const (
	backoff429Base = 30 * time.Second
	backoff429Cap  = 16 * time.Minute
	backoff5xxBase = 2 * time.Second
)

// Fetch retrieves one page, retrying per policy. It returns ErrCircuitOpen
// (wrapped) when repeated 429s indicate a suspended token.
func (p *Pager) Fetch(ctx context.Context, url string) (*PageResult, error) {
	var tries429, tries5xx int
	for {
		if err := p.limiter.Wait(ctx); err != nil {
			return nil, err
		}

		page, err := p.client.FetchPage(ctx, url)
		if err == nil {
			p.limiter.RecordResponse(page.WireBytes, http.StatusOK)
			return page, nil
		}

		var httpErr *HTTPError
		if !errors.As(err, &httpErr) {
			// Transport-level failure: retry like a 5xx, without a
			// status to record.
			tries5xx++
			if tries5xx > p.max5xx {
				return nil, fmt.Errorf("mlsgrid: giving up after %d transport errors: %w", p.max5xx, err)
			}
			if err := p.clock.Sleep(ctx, jittered(backoff5xxBase, tries5xx)); err != nil {
				return nil, err
			}
			continue
		}

		p.limiter.RecordResponse(httpErr.WireBytes, httpErr.StatusCode)
		switch {
		case httpErr.StatusCode == http.StatusTooManyRequests:
			if p.limiter.CircuitOpen() {
				return nil, fmt.Errorf("%w (last response: %s)", ratelimit.ErrCircuitOpen, httpErr.Body)
			}
			tries429++
			d := httpErr.RetryAfter
			if d <= 0 {
				d = expBackoff(backoff429Base, backoff429Cap, tries429)
			}
			if err := p.clock.Sleep(ctx, d); err != nil {
				return nil, err
			}
		case httpErr.StatusCode >= 500:
			tries5xx++
			if tries5xx > p.max5xx {
				return nil, fmt.Errorf("mlsgrid: giving up after %d server errors: %w", p.max5xx, httpErr)
			}
			if err := p.clock.Sleep(ctx, jittered(backoff5xxBase, tries5xx)); err != nil {
				return nil, err
			}
		default:
			return nil, httpErr
		}
	}
}

// expBackoff returns base·2^(attempt-1) capped.
func expBackoff(base, cap_ time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= cap_ {
			return cap_
		}
	}
	if d > cap_ {
		return cap_
	}
	return d
}

// jittered returns exponential backoff plus up to 25% random jitter.
func jittered(base time.Duration, attempt int) time.Duration {
	d := expBackoff(base, 2*time.Minute, attempt)
	return d + time.Duration(rand.Int64N(int64(d)/4+1))
}
