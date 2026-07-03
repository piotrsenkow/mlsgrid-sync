// Package media downloads listing photos queued by the sync engine
// (storage_status='pending') into a local-disk or S3-compatible sink. It
// follows the MLS Grid media rules: every download carries the mandatory
// `User-Agent: <access token>` header, files are self-hosted rather than
// hot-linked, and downloaded bytes count against the same hourly byte budget
// as the feed itself.
package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Sink stores downloaded media objects. Store returns a sink-relative
// path/key; it is persisted to media.local_path and handed back to Remove
// when the parent listing is hard-deleted.
type Sink interface {
	Store(ctx context.Context, mediaKey, contentType string, data []byte) (string, error)
	Remove(ctx context.Context, localPath string) error
}

// objectPath shards objects two levels deep by key hash so no directory (or
// S3 list prefix) accumulates millions of entries, regardless of how an MLS
// formats its MediaKeys.
func objectPath(mediaKey, contentType string) string {
	sum := sha256.Sum256([]byte(mediaKey))
	shard := hex.EncodeToString(sum[:2])
	return path.Join(shard[:2], shard[2:4], sanitizeKey(mediaKey)+extFor(contentType))
}

// sanitizeKey keeps MediaKeys filesystem- and URL-safe.
func sanitizeKey(key string) string {
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, key)
	if len(safe) > 128 {
		safe = safe[:128]
	}
	return safe
}

func extFor(contentType string) string {
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ".bin"
	}
	switch mt {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	default:
		return ".bin"
	}
}

// DiskSink writes files under a root directory.
type DiskSink struct {
	root string
}

// NewDiskSink returns a sink rooted at dir, creating it if needed.
func NewDiskSink(dir string) (*DiskSink, error) {
	if dir == "" {
		return nil, errors.New("media: disk sink path is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("media: creating sink root: %w", err)
	}
	return &DiskSink{root: dir}, nil
}

// Store writes the object atomically (temp file + rename) and returns its
// root-relative path, so local_path values survive the root moving.
func (d *DiskSink) Store(_ context.Context, mediaKey, contentType string, data []byte) (string, error) {
	rel := objectPath(mediaKey, contentType)
	full := filepath.Join(d.root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return "", fmt.Errorf("media: creating shard dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(full), ".download-*")
	if err != nil {
		return "", err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return "", err
	}
	// CreateTemp defaults to 0600; stored media is typically served by
	// another process (a web server), so open it up to a normal file mode.
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		_ = os.Remove(tmp.Name())
		return "", err
	}
	if err := os.Rename(tmp.Name(), full); err != nil {
		_ = os.Remove(tmp.Name())
		return "", err
	}
	return rel, nil
}

// Remove deletes a stored file; a file already gone is not an error.
func (d *DiskSink) Remove(_ context.Context, localPath string) error {
	err := os.Remove(filepath.Join(d.root, filepath.FromSlash(localPath)))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
