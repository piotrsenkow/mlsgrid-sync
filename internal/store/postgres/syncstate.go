package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// SyncState returns the cursor row, or nil when none exists. The caller owns
// the semantics of absence: backfill treats it as a fresh start, incremental
// sync refuses to run (an empty cursor must never mean "fetch everything").
func (s *Store) SyncState(ctx context.Context, resource, originatingSystem string) (*store.SyncState, error) {
	st := store.SyncState{Resource: resource, OriginatingSystem: originatingSystem}
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT last_modification_ts, in_progress_url, backfill_completed_at, last_full_reconcile_at
		 FROM %s WHERE resource = $1 AND originating_system = $2`,
		s.table("sync_state")), resource, originatingSystem).
		Scan(&st.LastModificationTS, &st.InProgressURL, &st.BackfillCompletedAt, &st.LastFullReconcileAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// SetSyncState upserts the cursor row.
func (s *Store) SetSyncState(ctx context.Context, st store.SyncState) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (resource, originating_system, last_modification_ts, in_progress_url,
		                 backfill_completed_at, last_full_reconcile_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (resource, originating_system) DO UPDATE SET
		     last_modification_ts   = EXCLUDED.last_modification_ts,
		     in_progress_url        = EXCLUDED.in_progress_url,
		     backfill_completed_at  = EXCLUDED.backfill_completed_at,
		     last_full_reconcile_at = EXCLUDED.last_full_reconcile_at,
		     updated_at             = now()`,
		s.table("sync_state")),
		st.Resource, st.OriginatingSystem, st.LastModificationTS, st.InProgressURL,
		st.BackfillCompletedAt, st.LastFullReconcileAt)
	return err
}
