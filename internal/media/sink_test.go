package media

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

func TestObjectPath(t *testing.T) {
	p := objectPath("TSTMEDIA0001", "image/jpeg")
	if !strings.HasSuffix(p, "/TSTMEDIA0001.jpg") {
		t.Errorf("path = %q, want key-named .jpg file", p)
	}
	parts := strings.Split(p, "/")
	if len(parts) != 3 || len(parts[0]) != 2 || len(parts[1]) != 2 {
		t.Errorf("path = %q, want two 2-char hash shards", p)
	}
	if p != objectPath("TSTMEDIA0001", "image/jpeg") {
		t.Error("objectPath must be deterministic")
	}

	if got := objectPath("k", "image/png; charset=binary"); !strings.HasSuffix(got, "k.png") {
		t.Errorf("content-type params must not break extension mapping: %q", got)
	}
	if got := objectPath("k", "application/octet-stream"); !strings.HasSuffix(got, "k.bin") {
		t.Errorf("unknown types get .bin: %q", got)
	}
	// Separators cannot survive sanitization, so a hostile key can never
	// introduce extra path components (and thus never traverse).
	got := objectPath("../../etc/passwd", "image/jpeg")
	if len(strings.Split(got, "/")) != 3 || path.Clean(got) != got {
		t.Errorf("keys must be sanitized for path safety: %q", got)
	}
}

func TestDiskSinkStoreAndRemove(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	sink, err := NewDiskSink(filepath.Join(root, "media"))
	if err != nil {
		t.Fatal(err)
	}
	rel, err := sink.Store(ctx, "TSTMEDIA0001", "image/jpeg", []byte("jpegdata"))
	if err != nil {
		t.Fatal(err)
	}
	if filepath.IsAbs(rel) {
		t.Errorf("Store must return a root-relative path, got %q", rel)
	}
	full := filepath.Join(root, "media", filepath.FromSlash(rel))
	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "jpegdata" {
		t.Errorf("stored data = %q", data)
	}
	if info, err := os.Stat(full); err != nil || info.Mode().Perm() != 0o644 {
		t.Errorf("mode = %v, want 0644 — media is usually served by another process", info.Mode())
	}

	if err := sink.Remove(ctx, rel); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(full); !os.IsNotExist(err) {
		t.Error("file must be gone after Remove")
	}
	if err := sink.Remove(ctx, rel); err != nil {
		t.Errorf("removing an already-gone file must not error: %v", err)
	}
}

func TestS3SinkStoreAndRemove(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	type call struct {
		method, path, contentType string
		body                      []byte
	}
	var calls []call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 0)
		if r.Body != nil {
			buf := make([]byte, 1024)
			for {
				n, err := r.Body.Read(buf)
				body = append(body, buf[:n]...)
				if err != nil {
					break
				}
			}
		}
		calls = append(calls, call{r.Method, r.URL.Path, r.Header.Get("Content-Type"), body})
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("ETag", `"test"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	ctx := context.Background()
	sink, err := NewS3Sink(ctx, S3Options{Endpoint: srv.URL, Bucket: "listings", Prefix: "photos"})
	if err != nil {
		t.Fatal(err)
	}
	key, err := sink.Store(ctx, "TSTMEDIA0001", "image/jpeg", []byte("jpegdata"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, "photos/") || !strings.HasSuffix(key, "TSTMEDIA0001.jpg") {
		t.Errorf("key = %q, want prefix + sharded key", key)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %+v", calls)
	}
	put := calls[0]
	// Path-style addressing: /<bucket>/<key>.
	if put.method != http.MethodPut || put.path != "/listings/"+key {
		t.Errorf("PUT path = %s %s, want /listings/%s", put.method, put.path, key)
	}
	if string(put.body) != "jpegdata" {
		t.Errorf("uploaded body = %q — streaming/chunked encodings break S3-compatible endpoints", put.body)
	}
	if put.contentType != "image/jpeg" {
		t.Errorf("content type = %q", put.contentType)
	}

	if err := sink.Remove(ctx, key); err != nil {
		t.Fatal(err)
	}
	if got := calls[len(calls)-1]; got.method != http.MethodDelete || got.path != "/listings/"+key {
		t.Errorf("DELETE call = %+v", got)
	}
}
