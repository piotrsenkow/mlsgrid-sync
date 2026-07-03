package mlsgrid

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFetchPageFollowsNextLink(t *testing.T) {
	var gotAuth []string
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/Property", func(w http.ResponseWriter, r *http.Request) {
		gotAuth = append(gotAuth, r.Header.Get("Authorization"))
		name := "property_page1.json"
		if r.URL.Query().Get("$skip") == "2" {
			name = "property_page2.json"
		}
		_, _ = w.Write(loadFixture(t, name, srv.URL))
	})

	c := NewClient("tok-test")
	ctx := context.Background()

	page1, err := c.FetchPage(ctx, srv.URL+"/Property")
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Records) != 2 {
		t.Fatalf("page1 records = %d, want 2", len(page1.Records))
	}
	if page1.NextLink == "" {
		t.Fatal("page1 should carry @odata.nextLink")
	}
	if page1.WireBytes <= 0 {
		t.Error("WireBytes should be counted")
	}

	page2, err := c.FetchPage(ctx, page1.NextLink)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Records) != 1 || page2.NextLink != "" {
		t.Errorf("page2: %d records, nextLink=%q; want 1 record, empty", len(page2.Records), page2.NextLink)
	}
	if gotAuth[0] != "Bearer tok-test" || gotAuth[1] != "Bearer tok-test" {
		t.Errorf("Authorization headers = %v", gotAuth)
	}
}

func TestFetchPageGzipCountsWireBytes(t *testing.T) {
	plain := loadFixture(t, "property_page2.json", "http://ignored.invalid")
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(plain)
	_ = gz.Close()
	compressed := buf.Bytes()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept-Encoding") != "gzip" {
			t.Error("client must request gzip")
		}
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(compressed)
	}))
	defer srv.Close()

	page, err := NewClient("t").FetchPage(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 {
		t.Fatalf("records = %d", len(page.Records))
	}
	// The download budget is measured in wire (compressed) bytes.
	if page.WireBytes != int64(len(compressed)) {
		t.Errorf("WireBytes = %d, want compressed size %d (not %d plain)", page.WireBytes, len(compressed), len(plain))
	}
}

func TestFetchPage429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"limit exceeded"}`))
	}))
	defer srv.Close()

	_, err := NewClient("t").FetchPage(context.Background(), srv.URL)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("want HTTPError, got %v", err)
	}
	if httpErr.StatusCode != 429 || httpErr.RetryAfter != 120*time.Second {
		t.Errorf("got status=%d retryAfter=%v", httpErr.StatusCode, httpErr.RetryAfter)
	}
	if !httpErr.Temporary() {
		t.Error("429 is temporary")
	}
	if httpErr.WireBytes <= 0 {
		t.Error("error responses still count against the byte budget")
	}
}

func TestFetchPage400NotTemporary(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "stale $skiptoken", http.StatusBadRequest)
	}))
	defer srv.Close()

	_, err := NewClient("t").FetchPage(context.Background(), srv.URL)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("want HTTPError, got %v", err)
	}
	if httpErr.Temporary() {
		t.Error("400 must not be retried blindly — the caller rebuilds the URL from its cursor")
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	h := http.Header{}
	h.Set("Retry-After", now.Add(90*time.Second).Format(http.TimeFormat))
	if d := parseRetryAfter(h, now); d != 90*time.Second {
		t.Errorf("HTTP-date Retry-After = %v, want 90s", d)
	}
	h.Set("Retry-After", "garbage")
	if d := parseRetryAfter(h, now); d != 0 {
		t.Errorf("garbage Retry-After = %v, want 0", d)
	}
}
