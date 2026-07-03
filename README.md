# mlsgrid-sync

[![CI](https://github.com/piotrsenkow/mlsgrid-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/piotrsenkow/mlsgrid-sync/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/piotrsenkow/mlsgrid-sync)](https://github.com/piotrsenkow/mlsgrid-sync/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/piotrsenkow/mlsgrid-sync.svg)](https://pkg.go.dev/github.com/piotrsenkow/mlsgrid-sync)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Replicate [MLS Grid](https://www.mlsgrid.com) listing feeds into PostgreSQL — correctly, resumably, and inside the rate limits.

> **Status: v0.1.** The full replication lifecycle — backfill, incremental sync, reconcile, media download, field scopes — is implemented, tested against synthetic fixtures, and validated against a live MRED feed. Pre-1.0 caveats: the schema contract is stable (v1.0.0), but CLI flags and config keys may still shift ([roadmap](docs/ROADMAP.md)).

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

## Quickstart

With Docker (bundles PostgreSQL):

```sh
cp config.example.yaml mlsgrid-sync.yaml   # edit: your feed's system slug
export MLSGRID_TOKEN_MRED=...              # your MLS Grid bearer token
docker compose run --rm sync init-db       # create schema + run migrations
docker compose run --rm sync backfill      # initial import (resumable)
docker compose up -d sync                  # continuous replication
```

Or against your own PostgreSQL — install a [release binary](https://github.com/piotrsenkow/mlsgrid-sync/releases) or `go install github.com/piotrsenkow/mlsgrid-sync/cmd/mlsgrid-sync@latest`:

```sh
cp config.example.yaml mlsgrid-sync.yaml   # edit: your feed's system slug + token env var
export MLSGRID_DATABASE_URL=postgres://... # or database.url in the config
mlsgrid-sync init-db
mlsgrid-sync backfill
mlsgrid-sync sync --daemon                 # deploy/mlsgrid-sync.service for systemd
```

Trying it out on a token you also use elsewhere? Bound the trial: `backfill --since 24h --max-pages 5` imports a day's slice in a handful of requests, and incremental sync takes over from there.

## Commands

| Command | Purpose |
|---|---|
| `init-db` | Create the `mlsgrid` schema and run migrations |
| `backfill` | Initial full import; `--force` to redo, `--no-expand` for a fast column-only pass |
| `sync --once` / `--daemon` | Incremental replication from the persisted cursor |
| `reconcile` | Full-feed key sweep; purges records deleted while offline |
| `media download` | Drain queued photos to the configured sink; `--max-files` bounds a run |
| `media retry` | Re-queue failed media downloads |
| `status` | Cursors, record counts, media queue, rate-budget usage |

## Field scopes

Core columns (see the [schema contract](docs/schema-contract.md)) are always populated. The profile's `field_scope` controls everything beyond them — how much of each record survives into the `raw` JSONB column, which child expansions are requested, and whether requests are narrowed with `$select`:

| Scope | `raw` keeps | Notes |
|---|---|---|
| `minimal` | nothing (NULL) | skips Rooms/UnitTypes expansions and auto-narrows requests with `$select` — the cheapest scope against the hourly byte budget |
| `standard` (default) | a curated keep-list: remarks, participants, dates/prices, structure, interior/exterior, systems, HOA, schools, rental terms | drops vendor internals and the long tail |
| `analytics` | standard + tax, income, expenses, and MLS-local (`MRD_*`) fields | for investment analysis |
| `full` | the record byte-for-byte as received | lossless, largest |
| `path/to/scope.yaml` | your own `include:`/`exclude:` glob lists | exclude beats include; empty include means everything |

Changing scope never requires a schema migration, and already-stored rows are not rewritten — the scope applies to records as they arrive.

## Media

The default mode (`metadata-only`) stores media rows — URLs, captions, dimensions — without fetching files. Setting `media.mode: download` in a profile queues every discovered photo (`storage_status='pending'`) and `media download` drains the queue into a sink: a local directory or any S3-compatible bucket (AWS, MinIO, Cloudflare R2 — S3 credentials come from the standard AWS environment variables).

Rules the downloader enforces for you:

- Every request sends `User-Agent: <your access token>` — mandatory for MLS Grid media since 2026-06-01.
- Downloads count against the same hourly byte budget as the feed, so a big backlog just proceeds at your configured limits. Use `--max-files` to bound a run when the token is shared.
- MediaKeys are immutable, so a downloaded key is never fetched twice; replaced photos arrive as new keys.
- A dead URL never blocks the queue: it is retried on later runs and parked as `failed` after 3 attempts (`media retry` re-queues).
- When a revoked listing is hard-deleted, its downloaded files are removed from the sink too.

## Design notes

Built from production experience running MLS Grid replication at scale (MRED / Chicagoland). The design decisions that matter — a dedicated `sync_state` cursor table that refuses to run with an empty watermark, `ge` cursor semantics with idempotent upserts, wall-clock-aligned persisted rate budgets, a circuit breaker on repeated 429s, MediaKey-immutability-based dedup — are documented in [docs/architecture.md](docs/architecture.md) (including an appendix of the production incidents this design encodes) and enforced by test fixtures.

Found a feed quirk this tool mishandles? Open an [MLS quirk report](https://github.com/piotrsenkow/mlsgrid-sync/issues/new?template=mls_quirk_report.yml) — that's how alias maps and unmarshalers grow beyond MRED.

## License

[Apache-2.0](LICENSE). Not affiliated with or endorsed by MLS Grid LLC.
