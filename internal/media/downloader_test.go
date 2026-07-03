package media

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// fakeQueue implements the downloader's Store interface over a static queue.
type fakeQueue struct {
	mu           sync.Mutex
	items        []store.MediaItem // must be sorted by MediaKey
	downloaded   map[string]struct{ path, contentType string }
	failed       map[string]bool // key -> permanent
	budget       ratelimit.Usage
	budgetReads  int
	budgetWrites int
}

func newFakeQueue(items ...store.MediaItem) *fakeQueue {
	sort.Slice(items, func(i, j int) bool { return items[i].MediaKey < items[j].MediaKey })
	return &fakeQueue{
		items:      items,
		downloaded: map[string]struct{ path, contentType string }{},
		failed:     map[string]bool{},
	}
}

func (f *fakeQueue) PendingMedia(ctx context.Context, afterKey string, limit int) ([]store.MediaItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.MediaItem
	for _, it := range f.items {
		if it.MediaKey > afterKey && len(out) < limit {
			out = append(out, it)
		}
	}
	return out, nil
}

func (f *fakeQueue) MarkMediaDownloaded(ctx context.Context, mediaKey, localPath, contentType string, bytes int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downloaded[mediaKey] = struct{ path, contentType string }{localPath, contentType}
	return nil
}

func (f *fakeQueue) MarkMediaFailed(ctx context.Context, mediaKey string, permanent bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed[mediaKey] = permanent
	return nil
}

func (f *fakeQueue) RateBudget(ctx context.Context) (ratelimit.Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.budgetReads++
	return f.budget, nil
}

func (f *fakeQueue) SetRateBudget(ctx context.Context, u ratelimit.Usage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.budget = u
	f.budgetWrites++
	return nil
}

// fakeSink records stored objects in memory.
type fakeSink struct {
	mu      sync.Mutex
	stored  map[string][]byte
	removed []string
	err     error
}

func newFakeSink() *fakeSink { return &fakeSink{stored: map[string][]byte{}} }

func (s *fakeSink) Store(ctx context.Context, mediaKey, contentType string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return "", s.err
	}
	path := objectPath(mediaKey, contentType)
	s.stored[path] = append([]byte(nil), data...)
	return path, nil
}

func (s *fakeSink) Remove(ctx context.Context, localPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed = append(s.removed, localPath)
	return nil
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testLimiter(cfg ratelimit.Config) *ratelimit.Limiter { return ratelimit.New(cfg, nil) }

// mediaServer serves canned bodies by path and asserts the mandatory
// User-Agent access-token header on every request.
func mediaServer(t *testing.T, token string, bodies map[string]struct {
	status int
	body   string
	ctype  string
}) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if got := r.Header.Get("User-Agent"); got != token {
			t.Errorf("User-Agent = %q, want the access token %q — MLS Grid requires it on media downloads", got, token)
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("media downloads must not send Authorization, got %q", got)
		}
		resp, ok := bodies[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if resp.ctype != "" {
			w.Header().Set("Content-Type", resp.ctype)
		}
		w.WriteHeader(resp.status)
		_, _ = w.Write([]byte(resp.body))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

type cannedResp = struct {
	status int
	body   string
	ctype  string
}

func TestDownloaderSendsTokenUserAgent(t *testing.T) {
	const token = "test-access-token"
	srv, _ := mediaServer(t, token, map[string]cannedResp{
		"/a.jpg": {200, "jpegdata", "image/jpeg"},
	})
	st := newFakeQueue(store.MediaItem{MediaKey: "TSTM1", ListingKey: "TSTL1", MediaURL: srv.URL + "/a.jpg"})
	sink := newFakeSink()

	stats, err := NewDownloader(st, Config{
		Token: token, Sink: sink, Limiter: testLimiter(ratelimit.Config{}), Log: discardLog(),
	}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Downloaded != 1 || stats.Bytes != 8 {
		t.Errorf("stats = %+v, want 1 download of 8 bytes", stats)
	}
	rec, ok := st.downloaded["TSTM1"]
	if !ok {
		t.Fatal("TSTM1 not marked downloaded")
	}
	if rec.contentType != "image/jpeg" {
		t.Errorf("content type = %q", rec.contentType)
	}
	if _, ok := sink.stored[rec.path]; !ok {
		t.Errorf("sink has %v, no object at recorded path %q", sink.stored, rec.path)
	}
	if st.budgetReads == 0 || st.budgetWrites == 0 {
		t.Error("downloader must restore and persist the shared rate budget")
	}
}

func TestDownloaderPerURLFailureTolerance(t *testing.T) {
	const token = "tok"
	srv, _ := mediaServer(t, token, map[string]cannedResp{
		"/ok.jpg": {200, "data", "image/jpeg"},
		"/dead":   {500, "boom", ""},
	})
	st := newFakeQueue(
		store.MediaItem{MediaKey: "TSTM1", MediaURL: srv.URL + "/dead"},
		store.MediaItem{MediaKey: "TSTM2", MediaURL: srv.URL + "/ok.jpg"},
		store.MediaItem{MediaKey: "TSTM3", MediaURL: srv.URL + "/dead", FailureCount: 2},
	)
	stats, err := NewDownloader(st, Config{
		Token: token, Sink: newFakeSink(), Limiter: testLimiter(ratelimit.Config{}), Log: discardLog(),
	}).Run(context.Background())
	if err != nil {
		t.Fatalf("per-URL failures must not abort the run: %v", err)
	}
	if stats.Downloaded != 1 || stats.Failed != 2 {
		t.Errorf("stats = %+v, want 1 downloaded / 2 failed", stats)
	}
	if perm := st.failed["TSTM1"]; perm {
		t.Error("first failure must stay retryable (pending), not permanent")
	}
	if perm, ok := st.failed["TSTM3"]; !ok || !perm {
		t.Error("third failure must park the row as failed")
	}
}

func TestDownloaderEmptyURLFailsWithoutRequest(t *testing.T) {
	srv, hits := mediaServer(t, "tok", nil)
	_ = srv
	st := newFakeQueue(store.MediaItem{MediaKey: "TSTM1", MediaURL: ""})
	stats, err := NewDownloader(st, Config{
		Token: "tok", Sink: newFakeSink(), Limiter: testLimiter(ratelimit.Config{}), Log: discardLog(),
	}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Failed != 1 || !st.failed["TSTM1"] {
		t.Errorf("empty URL must fail permanently: stats=%+v failed=%v", stats, st.failed)
	}
	if hits.Load() != 0 {
		t.Errorf("no HTTP request should be made for an empty URL, got %d", hits.Load())
	}
}

func TestDownloaderMaxFiles(t *testing.T) {
	srv, hits := mediaServer(t, "tok", map[string]cannedResp{
		"/a.jpg": {200, "data", "image/jpeg"},
	})
	var items []store.MediaItem
	for _, k := range []string{"TSTM1", "TSTM2", "TSTM3", "TSTM4", "TSTM5"} {
		items = append(items, store.MediaItem{MediaKey: k, MediaURL: srv.URL + "/a.jpg"})
	}
	stats, err := NewDownloader(newFakeQueue(items...), Config{
		Token: "tok", Sink: newFakeSink(), MaxFiles: 2,
		Limiter: testLimiter(ratelimit.Config{}), Log: discardLog(),
	}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Downloaded != 2 || hits.Load() != 2 {
		t.Errorf("downloaded=%d hits=%d, want exactly 2 (bounded run on a shared token)", stats.Downloaded, hits.Load())
	}
}

func TestDownloaderOversizedFileFailsPermanently(t *testing.T) {
	srv, _ := mediaServer(t, "tok", map[string]cannedResp{
		"/big.jpg": {200, "0123456789ABCDEF", "image/jpeg"},
	})
	st := newFakeQueue(store.MediaItem{MediaKey: "TSTM1", MediaURL: srv.URL + "/big.jpg"})
	sink := newFakeSink()
	stats, err := NewDownloader(st, Config{
		Token: "tok", Sink: sink, MaxFileBytes: 8,
		Limiter: testLimiter(ratelimit.Config{}), Log: discardLog(),
	}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Failed != 1 || !st.failed["TSTM1"] {
		t.Errorf("oversized file must fail permanently, stats=%+v failed=%v", stats, st.failed)
	}
	if len(sink.stored) != 0 {
		t.Error("oversized file must not reach the sink")
	}
}

func TestDownloaderHaltsOnOpenCircuit(t *testing.T) {
	srv, _ := mediaServer(t, "tok", map[string]cannedResp{
		"/throttled": {429, "slow down", ""},
	})
	var items []store.MediaItem
	for _, k := range []string{"TSTM1", "TSTM2", "TSTM3", "TSTM4", "TSTM5"} {
		items = append(items, store.MediaItem{MediaKey: k, MediaURL: srv.URL + "/throttled"})
	}
	st := newFakeQueue(items...)
	_, err := NewDownloader(st, Config{
		Token: "tok", Sink: newFakeSink(), Workers: 1,
		Limiter: testLimiter(ratelimit.Config{CircuitThreshold: 3}), Log: discardLog(),
	}).Run(context.Background())
	if !errors.Is(err, ratelimit.ErrCircuitOpen) {
		t.Fatalf("repeated 429s must halt the run with ErrCircuitOpen, got %v", err)
	}
	if st.budgetWrites == 0 {
		t.Error("budget must persist even when the run aborts")
	}
}

func TestDownloaderSinkFailureAborts(t *testing.T) {
	srv, _ := mediaServer(t, "tok", map[string]cannedResp{
		"/a.jpg": {200, "data", "image/jpeg"},
	})
	sink := newFakeSink()
	sink.err = errors.New("disk full")
	st := newFakeQueue(store.MediaItem{MediaKey: "TSTM1", MediaURL: srv.URL + "/a.jpg"})
	_, err := NewDownloader(st, Config{
		Token: "tok", Sink: sink, Limiter: testLimiter(ratelimit.Config{}), Log: discardLog(),
	}).Run(context.Background())
	if err == nil {
		t.Fatal("a failing sink is systemic and must abort the run")
	}
	if len(st.failed) != 0 {
		t.Errorf("sink failures must not burn per-file failure budgets, got %v", st.failed)
	}
}

func TestDownloaderRecordsByteBudget(t *testing.T) {
	srv, _ := mediaServer(t, "tok", map[string]cannedResp{
		"/a.jpg": {200, "12345678", "image/jpeg"},
	})
	lim := testLimiter(ratelimit.Config{})
	_, err := NewDownloader(
		newFakeQueue(store.MediaItem{MediaKey: "TSTM1", MediaURL: srv.URL + "/a.jpg"}),
		Config{Token: "tok", Sink: newFakeSink(), Limiter: lim, Log: discardLog()},
	).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := lim.Snapshot().HourBytes; got != 8 {
		t.Errorf("hourly byte budget recorded %d bytes, want 8 — media shares the feed's 4 GB/hr budget", got)
	}
}
