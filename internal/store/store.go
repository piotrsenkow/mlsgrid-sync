// Package store defines the persistence interface the sync engine writes
// through. The schema it targets is specified in docs/schema-contract.md;
// internal/store/postgres is the v1 implementation, and the interface exists
// so additional stores (SQLite is the roadmap candidate) can slot in without
// touching the engine.
package store

import (
	"context"
	"time"

	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
)

// Store persists feed records and replication bookkeeping.
type Store interface {
	// Migrate applies pending schema migrations. Idempotent.
	Migrate(ctx context.Context) error

	// ContractVersion returns schema_meta's contract_version row.
	ContractVersion(ctx context.Context) (string, error)

	// UpsertProperties writes one page of Property records (with expanded
	// children) in a single transaction, capturing listing_event rows for
	// detected changes. Records lacking a ListingKey or ModificationTimestamp
	// are counted in Stats.Skipped rather than failing the page.
	UpsertProperties(ctx context.Context, recs []mlsgrid.Record) (UpsertStats, error)

	// DeleteProperties hard-deletes listings and their children (rooms,
	// unit types, media rows) and appends a delisted listing_event per row
	// that existed. Used for MlgCanView=false records — a license
	// obligation. Returns the number of property rows deleted.
	DeleteProperties(ctx context.Context, listingKeys []string) (int64, error)

	// UpsertOpenHouses writes OpenHouse records in a single transaction.
	UpsertOpenHouses(ctx context.Context, recs []mlsgrid.Record) (UpsertStats, error)

	// DeleteOpenHouses removes open-house rows by key.
	DeleteOpenHouses(ctx context.Context, openHouseKeys []string) (int64, error)

	// SyncState returns the cursor row for (resource, originatingSystem),
	// or nil when none exists — the caller decides what an absent cursor
	// means (backfill: fresh start; sync: refuse to run).
	SyncState(ctx context.Context, resource, originatingSystem string) (*SyncState, error)

	// SetSyncState upserts the cursor row.
	SetSyncState(ctx context.Context, s SyncState) error

	// Count reports stored rows for a resource ("Property" or "OpenHouse");
	// backfill's --force guard and status use it.
	Count(ctx context.Context, resource string) (int64, error)

	// ListKeys returns every stored key for a resource with its local
	// modification timestamp — the local side of a reconcile diff.
	ListKeys(ctx context.Context, resource string) (map[string]time.Time, error)

	// RateBudget loads the persisted rate-limit window counters (zero Usage
	// when none are stored). Persisting budgets means a crash-looping
	// process cannot launder its rate-limit usage.
	RateBudget(ctx context.Context) (ratelimit.Usage, error)

	// SetRateBudget upserts the current window counters and opportunistically
	// prunes windows older than 48h.
	SetRateBudget(ctx context.Context, u ratelimit.Usage) error

	// Close releases the underlying connections.
	Close()
}

// UpsertStats summarizes one page write.
type UpsertStats struct {
	Inserted int // rows that did not exist before
	Updated  int // rows that existed and were overwritten
	Skipped  int // records missing ListingKey/ModificationTimestamp
	Events   int // listing_event rows appended
}

// SyncState mirrors the sync_state table (see docs/schema-contract.md).
// Pointer fields are NULLable columns.
type SyncState struct {
	Resource            string
	OriginatingSystem   string
	LastModificationTS  *time.Time
	InProgressURL       *string
	BackfillCompletedAt *time.Time
	LastFullReconcileAt *time.Time
}
