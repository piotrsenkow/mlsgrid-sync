# mlsgrid-sync Roadmap

Milestones are sized so each is one focused working session. Check the box when the milestone's deliverables are merged and CI is green. Full design rationale lives in the project plan; the schema is specified in [schema-contract.md](schema-contract.md).

- [x] **M0 — Schema contract + compliance docs.** `docs/schema-contract.md` v1.0.0, `docs/compliance.md`.
- [x] **M1 — Scaffold.** GitHub repo (public, Apache-2.0 + NOTICE), CI (lint / test -race / build matrix), Makefile, Dockerfile, cobra CLI skeleton (`init-db`, `backfill`, `sync`, `reconcile`, `media`, `status`, `version`), viper config loader with multi-MLS profiles, `.env.example`, CLAUDE.md, README stub with compliance section.
- [x] **M2 — OData client + rate limiter.** MLS Grid client (auth, paging via `@odata.nextLink`, `$filter` builder, gzip), custom unmarshalers (int-or-float, naive timestamps, string-or-array, free-text dates), synthetic fixture corpus in `testdata/odata/`, full rate limiter (token bucket, UTC wall-clock windows, byte budget, 429/`Retry-After`, circuit breaker) with fake-clock tests. **No test may ever touch api.mlsgrid.com.**
- [x] **M3 — Store.** `internal/store` interface + Postgres impl (pgx/v5): embedded migrations implementing schema contract v1.0.0, single table-driven field→column map, MRED alias normalization (`internal/fieldscope` builtin map), upserts with `listing_event` change capture, golden-schema test, `init-db` command, testcontainers integration tests.
- [x] **M4 — Backfill.** `MlgCanView eq true` paging, resume via `sync_state.in_progress_url`, `$skip`-wall recovery (rebuild timestamp-filtered URL), progress reporting, `--force` guard over non-empty tables. Also shipped: `--since` (bounded import for trials / shared-token budgets) and `--max-pages` (smoke-test cap that keeps the resume cursor).
- [x] **M5 — Incremental sync.** Cursor invariants (`ge` semantics, refuse NULL watermark), `MlgCanView=false` hard deletes + `delisted` events, `sync --once` and `sync --daemon` (interval + jitter, localhost health endpoint), systemd unit. Also shipped: functional `status` command (cursors + counts).
- [x] **M6 — Reconcile + OpenHouse.** Full-feed key sweep + purge/re-queue, OpenHouse resource sync, rate-budget persistence to `rate_budget`. Reconcile skips remote-only records by default (bounded backfills stay bounded); `--include-missing` imports them. Known limitation: sweeps of feeds beyond the API's deep-paging limit (~500K records) need windowed sweeps — a v1.1 candidate.
- [x] **M7 — Media.** `metadata-only` and `download` modes, `User-Agent: <token>` header (tested), disk + S3-compatible sinks, MediaKey-immutability dedup, per-URL failure tolerance (3 strikes → `failed`), `media retry`. Also shipped: `media download --max-files` (bounded runs for shared tokens), downloads counted against the persisted byte budget, and sink-file cleanup when revoked listings are hard-deleted. Replaced photos whose old keys are orphan-cleaned leave their files behind — a `media prune` GC is a v1.1 candidate.
- [x] **M8 — Field scopes.** Presets (`minimal` / `standard` / `analytics` / `full`), custom YAML include/exclude globs, automatic `$select` optimization for narrow scopes. Scope filters the Property record's `raw` only (children/open-house `raw` stay lossless, per the contract); `minimal` also skips Rooms/UnitTypes expansions and `$select`s just the core-column fields (alias spellings included). Also shipped: custom alias-map YAML files (`field_aliases: path/to/aliases.yaml`).
- [x] **M9 — v0.1.0 release.** architecture.md (incl. production-incident appendix), README polish + badges, docker-compose quickstart (bundled Postgres), goreleaser + tag-triggered release workflow, issue templates (bug report + "MLS quirk report").

## v1.1 candidates (post-release, demand-driven)

- Member / Office / Lookup resources
- SQLite store
- Additional MLS alias maps contributed via "MLS quirk report" issues
- Windowed reconcile sweeps (feeds larger than the API's ~500K deep-paging limit)
- `media prune` — sink GC for files whose rows were orphan-cleaned (replaced photos)
- Media download pass scheduled inside `sync --daemon`
