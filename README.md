# mlsgrid-sync

Replicate [MLS Grid](https://www.mlsgrid.com) listing feeds into PostgreSQL — correctly, resumably, and inside the rate limits.

> **Status: pre-release.** The schema contract and CLI skeleton are in place; the sync engine is being built milestone-by-milestone ([roadmap](docs/ROADMAP.md)). Not yet usable for production replication.

`mlsgrid-sync` is a single Go binary that performs the full MLS Grid replication lifecycle against the RESO Web API (OData):

- **Backfill** — resumable initial import (`MlgCanView eq true`), surviving interrupts and the API's `$skip` pagination wall
- **Incremental sync** — cursor-based catch-up (`--once` for cron, `--daemon` for continuous), with hard deletion of records the feed revokes
- **Reconcile** — periodic full-feed sweep that purges deletions missed while the tool wasn't running
- **Media** — metadata-only mode, or download-to-your-own-storage (local disk or any S3-compatible endpoint), as MLS Grid rules require
- **Field scopes** — keep everything (`full`), a curated set (`standard`, `analytics`), or the bare minimum (`minimal`); stable core columns + a `raw` JSONB column mean scope changes never require schema migrations

The database schema is a versioned, documented contract — see [docs/schema-contract.md](docs/schema-contract.md) — designed to be queried directly or through the companion MCP server, [`mlsgrid-mcp`](https://github.com/piotrsenkow/mlsgrid-mcp).

## Compliance — read this first

**This tool ships no MLS data and no credentials.** To use it you must:

1. Have an **executed [MLS Grid Data License Agreement](https://www.mlsgrid.com/s/MLS-Grid-Data-License-Agreement.pdf)** and **per-MLS approval** for every feed (`OriginatingSystemName`) you replicate.
2. Follow your license tier's display rules (IDX/VOW/back-office) on anything you build with the data.
3. Self-host any media you display — MLS Grid media URLs are for downloading only; hot-linking is prohibited.

The tool automatically enforces what it can: it deletes revoked records (`MlgCanView=false`) as the license requires, runs below the published rate limits (and refuses config that exceeds them), backs off on HTTP 429, and sends the required `User-Agent` token header on media downloads. Details in [docs/compliance.md](docs/compliance.md).

## Quickstart (will stabilize at v0.1.0)

```sh
cp config.example.yaml mlsgrid-sync.yaml   # edit: your feed's system slug + token env var
cp .env.example .env                       # edit: database URL + bearer token
mlsgrid-sync init-db                       # create schema + run migrations
mlsgrid-sync backfill                      # initial import (resumable)
mlsgrid-sync sync --daemon                 # continuous replication
```

## Commands

| Command | Purpose |
|---|---|
| `init-db` | Create the `mlsgrid` schema and run migrations |
| `backfill` | Initial full import; `--force` to redo, `--no-expand` for a fast column-only pass |
| `sync --once` / `--daemon` | Incremental replication from the persisted cursor |
| `reconcile` | Full-feed key sweep; purges records deleted while offline |
| `media retry` | Re-queue failed media downloads |
| `status` | Cursors, record counts, rate-budget usage |

## Design notes

Built from production experience running MLS Grid replication at scale (MRED / Chicagoland). The design decisions that matter — a dedicated `sync_state` cursor table that refuses to run with an empty watermark, `ge` cursor semantics with idempotent upserts, wall-clock-aligned persisted rate budgets, a circuit breaker on repeated 429s, MediaKey-immutability-based dedup — are documented in [docs/architecture.md](docs/architecture.md) (forthcoming) and encoded as test fixtures.

## License

[Apache-2.0](LICENSE). Not affiliated with or endorsed by MLS Grid LLC.
