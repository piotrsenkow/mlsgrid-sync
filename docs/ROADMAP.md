# mlsgrid-sync Roadmap

Milestones are sized so each is one focused working session. Check the box when the milestone's deliverables are merged and CI is green. Full design rationale lives in the project plan; the schema is specified in [schema-contract.md](schema-contract.md).

- [x] **M0 — Schema contract + compliance docs.** `docs/schema-contract.md` v1.0.0, `docs/compliance.md`.
- [x] **M1 — Scaffold.** GitHub repo (public, Apache-2.0 + NOTICE), CI (lint / test -race / build matrix), Makefile, Dockerfile, cobra CLI skeleton (`init-db`, `backfill`, `sync`, `reconcile`, `media`, `status`, `version`), viper config loader with multi-MLS profiles, `.env.example`, CLAUDE.md, README stub with compliance section.
- [ ] **M2 — OData client + rate limiter.** MLS Grid client (auth, paging via `@odata.nextLink`, `$filter` builder, gzip), custom unmarshalers (int-or-float, naive timestamps, string-or-array, free-text dates), synthetic fixture corpus in `testdata/odata/`, full rate limiter (token bucket, UTC wall-clock windows, byte budget, 429/`Retry-After`, circuit breaker) with fake-clock tests. **No test may ever touch api.mlsgrid.com.**
- [ ] **M3 — Store.** `internal/store` interface + Postgres impl (pgx/v5): embedded migrations implementing schema contract v1.0.0, single table-driven field→column map, MRED alias normalization (`internal/fieldscope` builtin map), upserts with `listing_event` change capture, golden-schema test, `init-db` command, testcontainers integration tests.
- [ ] **M4 — Backfill.** `MlgCanView eq true` paging, resume via `sync_state.in_progress_url`, `$skip`-wall recovery (rebuild timestamp-filtered URL), progress reporting, `--force` guard over non-empty tables.
- [ ] **M5 — Incremental sync.** Cursor invariants (`ge` semantics, refuse NULL watermark), `MlgCanView=false` hard deletes + `delisted` events, `sync --once` and `sync --daemon` (interval + jitter, localhost health endpoint), systemd unit.
- [ ] **M6 — Reconcile + OpenHouse.** Full-feed key sweep + purge/re-queue, OpenHouse resource sync, rate-budget persistence to `rate_budget`.
- [ ] **M7 — Media.** `metadata-only` and `download` modes, `User-Agent: <token>` header (tested), disk + S3-compatible sinks, MediaKey-immutability dedup, per-URL failure tolerance, `media retry`.
- [ ] **M8 — Field scopes.** Presets (`minimal` / `standard` / `analytics` / `full`), custom YAML include/exclude globs, automatic `$select` optimization for narrow scopes.
- [ ] **M9 — v0.1.0 release.** architecture.md, README polish, docker-compose quickstart (bundled Postgres), goreleaser, badges, issue templates (incl. "MLS quirk report").

## v1.1 candidates (post-release, demand-driven)

- Member / Office / Lookup resources
- SQLite store
- Additional MLS alias maps contributed via "MLS quirk report" issues
