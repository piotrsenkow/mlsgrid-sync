package mlsgrid

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// DefaultBaseURL is the production API root. Tests must never use it; they
// point the client at an httptest server instead.
const DefaultBaseURL = "https://api.mlsgrid.com/v2"

// Client performs single HTTP requests against the MLS Grid API. It does no
// rate limiting or retrying itself — Pager owns that policy.
type Client struct {
	httpClient *http.Client
	token      string
}

// NewClient returns a client authenticating with the given bearer token.
func NewClient(token string, opts ...ClientOption) *Client {
	c := &Client{
		httpClient: &http.Client{Timeout: 60 * time.Second},
		token:      token,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

type ClientOption func(*Client)

// WithHTTPClient overrides the underlying HTTP client (tests, custom timeouts).
func WithHTTPClient(h *http.Client) ClientOption {
	return func(c *Client) { c.httpClient = h }
}

// PageResult is one page of feed records.
type PageResult struct {
	Records  []Record
	NextLink string
	// WireBytes counts compressed bytes read off the network — the unit the
	// MLS Grid hourly download budget is measured in.
	WireBytes int64
}

// HTTPError is a non-200 API response.
type HTTPError struct {
	StatusCode int
	// RetryAfter is the parsed Retry-After header (0 when absent).
	RetryAfter time.Duration
	Body       string
	// WireBytes still counts against the download budget.
	WireBytes int64
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("mlsgrid: HTTP %d: %s", e.StatusCode, e.Body)
}

// Temporary reports whether the request may be retried (throttling or
// server-side failure).
func (e *HTTPError) Temporary() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

type envelope struct {
	Value    []json.RawMessage `json:"value"`
	NextLink string            `json:"@odata.nextLink"`
}

type countingReader struct {
	r io.Reader
	n int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n += int64(n)
	return n, err
}

// FetchPage GETs one feed page. Gzip is requested and decompressed manually
// (rather than via the transport) so WireBytes reflects what actually crossed
// the network.
func (c *Client) FetchPage(ctx context.Context, url string) (*PageResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mlsgrid: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	counter := &countingReader{r: resp.Body}
	var body io.Reader = counter
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(counter)
		if err != nil {
			return nil, fmt.Errorf("mlsgrid: bad gzip response: %w", err)
		}
		defer func() { _ = gz.Close() }()
		body = gz
	}

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(body, 4096))
		_, _ = io.Copy(io.Discard, body)
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header, time.Now()),
			Body:       string(snippet),
			WireBytes:  counter.n,
		}
	}

	var env envelope
	if err := json.NewDecoder(body).Decode(&env); err != nil {
		return nil, fmt.Errorf("mlsgrid: decoding page: %w", err)
	}
	// Drain so WireBytes covers the full response.
	_, _ = io.Copy(io.Discard, body)

	records := make([]Record, 0, len(env.Value))
	for i, raw := range env.Value {
		rec, err := ParseRecord(raw)
		if err != nil {
			return nil, fmt.Errorf("mlsgrid: record %d: %w", i, err)
		}
		records = append(records, rec)
	}
	return &PageResult{Records: records, NextLink: env.NextLink, WireBytes: counter.n}, nil
}

// parseRetryAfter handles both delta-seconds and HTTP-date forms.
func parseRetryAfter(h http.Header, now time.Time) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}
