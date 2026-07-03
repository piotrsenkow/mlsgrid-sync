package mlsgrid

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
	"github.com/piotrsenkow/mlsgrid-sync/internal/testclock"
)

// scriptedServer returns each status in sequence, then 200 with a fixture page.
func scriptedServer(t *testing.T, statuses []int, retryAfter string) (*httptest.Server, *int) {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { calls++ }()
		if calls < len(statuses) {
			if retryAfter != "" && statuses[calls] == 429 {
				w.Header().Set("Retry-After", retryAfter)
			}
			w.WriteHeader(statuses[calls])
			_, _ = w.Write([]byte(`{"error":"scripted"}`))
			return
		}
		_, _ = w.Write(loadFixture(t, "property_page2.json", "http://ignored.invalid"))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func newTestPager(clock ratelimit.Clock, srvURL string) *Pager {
	// Generous limits: these tests exercise retry policy, not budgets.
	limiter := ratelimit.New(ratelimit.Config{RPS: 1000, Burst: 10, CircuitThreshold: 3}, clock)
	return NewPager(NewClient("t"), limiter, clock)
}

func TestPagerHonorsRetryAfter(t *testing.T) {
	clock := testclock.At(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	srv, calls := scriptedServer(t, []int{429}, "45")
	p := newTestPager(clock, srv.URL)

	page, err := p.Fetch(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || *calls != 2 {
		t.Fatalf("records=%d calls=%d", len(page.Records), *calls)
	}
	var sawRetryAfter bool
	for _, d := range clock.Slept() {
		if d == 45*time.Second {
			sawRetryAfter = true
		}
	}
	if !sawRetryAfter {
		t.Errorf("expected a 45s sleep honoring Retry-After, slept %v", clock.Slept())
	}
}

func TestPager429BackoffWithoutHeader(t *testing.T) {
	clock := testclock.At(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	srv, _ := scriptedServer(t, []int{429, 429}, "")
	p := newTestPager(clock, srv.URL)

	if _, err := p.Fetch(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}
	var backoffs []time.Duration
	for _, d := range clock.Slept() {
		if d >= backoff429Base {
			backoffs = append(backoffs, d)
		}
	}
	if len(backoffs) != 2 || backoffs[0] != 30*time.Second || backoffs[1] != 60*time.Second {
		t.Errorf("429 backoffs = %v, want [30s 60s]", backoffs)
	}
}

func TestPagerCircuitBreakerHalts(t *testing.T) {
	clock := testclock.At(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	srv, calls := scriptedServer(t, []int{429, 429, 429, 429, 429}, "")
	p := newTestPager(clock, srv.URL)

	_, err := p.Fetch(context.Background(), srv.URL)
	if !errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
	// Threshold 3: halt after the third consecutive 429, never hammering on.
	if *calls != 3 {
		t.Errorf("made %d requests before halting, want 3", *calls)
	}
}

func TestPagerRetries5xxThenGivesUp(t *testing.T) {
	clock := testclock.At(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	srv, calls := scriptedServer(t, []int{503, 502}, "")
	p := newTestPager(clock, srv.URL)
	if _, err := p.Fetch(context.Background(), srv.URL); err != nil {
		t.Fatalf("2 transient 5xx should be survivable: %v", err)
	}
	if *calls != 3 {
		t.Errorf("calls = %d, want 3", *calls)
	}

	srv2, _ := scriptedServer(t, []int{500, 500, 500, 500, 500, 500, 500}, "")
	p2 := newTestPager(clock, srv2.URL)
	if _, err := p2.Fetch(context.Background(), srv2.URL); err == nil {
		t.Error("persistent 5xx must eventually give up")
	}
}

func TestPagerPassesThrough400(t *testing.T) {
	clock := testclock.At(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	srv, calls := scriptedServer(t, []int{400}, "")
	p := newTestPager(clock, srv.URL)

	_, err := p.Fetch(context.Background(), srv.URL)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 400 {
		t.Fatalf("want HTTPError 400 passed through, got %v", err)
	}
	if *calls != 1 {
		t.Errorf("400 must not be retried (caller rebuilds URL), calls = %d", *calls)
	}
}

func TestExpBackoffCap(t *testing.T) {
	if d := expBackoff(backoff429Base, backoff429Cap, 10); d != backoff429Cap {
		t.Errorf("attempt 10 = %v, want cap %v", d, backoff429Cap)
	}
	if d := expBackoff(backoff429Base, backoff429Cap, 1); d != 30*time.Second {
		t.Errorf("attempt 1 = %v, want 30s", d)
	}
}
