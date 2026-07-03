package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// PendingMedia returns queued media rows strictly after afterKey, in
// media_key order. The partial index on storage_status='pending' keeps this
// cheap regardless of how many rows are downloaded or skipped.
func (s *Store) PendingMedia(ctx context.Context, afterKey string, limit int) ([]store.MediaItem, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT media_key, listing_key, coalesce(media_url, ''), failure_count
		 FROM %s
		 WHERE storage_status = 'pending' AND media_key > $1
		 ORDER BY media_key
		 LIMIT $2`, s.table("media")), afterKey, limit)
	if err != nil {
		return nil, err
	}
	var items []store.MediaItem
	var it store.MediaItem
	if _, err := pgx.ForEachRow(rows,
		[]any{&it.MediaKey, &it.ListingKey, &it.MediaURL, &it.FailureCount},
		func() error {
			items = append(items, it)
			return nil
		}); err != nil {
		return nil, err
	}
	return items, nil
}

// MarkMediaDownloaded records a completed download.
func (s *Store) MarkMediaDownloaded(ctx context.Context, mediaKey, localPath, contentType string, bytes int64) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s
		 SET storage_status = 'downloaded', local_path = $2, content_type = $3,
		     bytes = $4, updated_at = now()
		 WHERE media_key = $1`, s.table("media")),
		mediaKey, localPath, contentType, bytes)
	return err
}

// MarkMediaFailed increments failure_count; permanent parks the row in
// storage_status='failed' so it is no longer claimed.
func (s *Store) MarkMediaFailed(ctx context.Context, mediaKey string, permanent bool) error {
	status := "pending"
	if permanent {
		status = "failed"
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s
		 SET failure_count = failure_count + 1, storage_status = $2, updated_at = now()
		 WHERE media_key = $1`, s.table("media")),
		mediaKey, status)
	return err
}

// RequeueFailedMedia returns failed rows to the queue with a fresh
// failure budget.
func (s *Store) RequeueFailedMedia(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s
		 SET storage_status = 'pending', failure_count = 0, updated_at = now()
		 WHERE storage_status = 'failed'`, s.table("media")))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// MediaStats counts media rows by storage_status.
func (s *Store) MediaStats(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		"SELECT storage_status, count(*) FROM %s GROUP BY storage_status",
		s.table("media")))
	if err != nil {
		return nil, err
	}
	stats := make(map[string]int64)
	var status string
	var n int64
	if _, err := pgx.ForEachRow(rows, []any{&status, &n}, func() error {
		stats[status] = n
		return nil
	}); err != nil {
		return nil, err
	}
	return stats, nil
}
