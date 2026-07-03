package engine

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/piotrsenkow/mlsgrid-sync/internal/mlsgrid"
	"github.com/piotrsenkow/mlsgrid-sync/internal/ratelimit"
	"github.com/piotrsenkow/mlsgrid-sync/internal/store"
)

// resourceOps binds a feed resource to its store operations, letting the
// backfill/sync/reconcile loops stay resource-generic.
type resourceOps struct {
	// keyField is the resource's OData key (ListingKey, OpenHouseKey).
	keyField string
	upsert   func(context.Context, store.Store, []mlsgrid.Record) (store.UpsertStats, error)
	del      func(context.Context, store.Store, []string) (int64, error)
	// expandable reports whether $expand applies (Property children).
	expandable bool
}

func opsFor(resource string) (resourceOps, error) {
	switch resource {
	case "Property":
		return resourceOps{
			keyField: "ListingKey",
			upsert: func(ctx context.Context, st store.Store, recs []mlsgrid.Record) (store.UpsertStats, error) {
				return st.UpsertProperties(ctx, recs)
			},
			del: func(ctx context.Context, st store.Store, keys []string) (int64, error) {
				return st.DeleteProperties(ctx, keys)
			},
			expandable: true,
		}, nil
	case "OpenHouse":
		return resourceOps{
			keyField: "OpenHouseKey",
			upsert: func(ctx context.Context, st store.Store, recs []mlsgrid.Record) (store.UpsertStats, error) {
				return st.UpsertOpenHouses(ctx, recs)
			},
			del: func(ctx context.Context, st store.Store, keys []string) (int64, error) {
				return st.DeleteOpenHouses(ctx, keys)
			},
		}, nil
	default:
		return resourceOps{}, fmt.Errorf("engine: unsupported resource %q", resource)
	}
}

// split partitions a page into storable records and revoked keys, using the
// resource's key field.
func (o resourceOps) split(recs []mlsgrid.Record) (viewable []mlsgrid.Record, revokedKeys []string) {
	for _, rec := range recs {
		if rec.CanView() {
			viewable = append(viewable, rec)
			continue
		}
		if key := rec.String(o.keyField); key != "" {
			revokedKeys = append(revokedKeys, key)
		}
	}
	return viewable, revokedKeys
}

// budget wires optional rate-limiter persistence into an engine run: restore
// the window counters at start (so restarts cannot launder usage), persist a
// snapshot as work progresses. A nil limiter disables both. Persistence
// failures are logged, never fatal — losing a snapshot must not kill a sync.
type budget struct {
	limiter *ratelimit.Limiter
	store   store.Store
	log     *slog.Logger
}

func newBudget(l *ratelimit.Limiter, st store.Store, log *slog.Logger) *budget {
	return &budget{limiter: l, store: st, log: log}
}

func (b *budget) restore(ctx context.Context) {
	if b.limiter == nil {
		return
	}
	u, err := b.store.RateBudget(ctx)
	if err != nil {
		b.log.Warn("could not restore rate budget — starting from empty windows", "error", err)
		return
	}
	b.limiter.Restore(u)
}

func (b *budget) persist(ctx context.Context) {
	if b.limiter == nil {
		return
	}
	if err := b.store.SetRateBudget(ctx, b.limiter.Snapshot()); err != nil {
		b.log.Warn("could not persist rate budget", "error", err)
	}
}
