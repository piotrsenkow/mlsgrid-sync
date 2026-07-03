# MLS Grid Compliance Guide

`mlsgrid-sync` is a replication tool. It ships **no MLS data and no credentials** — you bring your own MLS Grid subscription. This page distills the rules you agree to when you use the MLS Grid API, and explains which of them the tool enforces for you versus which remain your responsibility. It is a summary, not legal advice; the [MLS Grid Data License Agreement](https://www.mlsgrid.com/s/MLS-Grid-Data-License-Agreement.pdf) and your MLS's rules are controlling.

## Before you can sync anything

1. **Execute the MLS Grid Master Data License Agreement.** One agreement covers IDX, VOW, and back-office use across all participating MLSs.
2. **Get approved by each MLS** (`OriginatingSystemName`) you want to replicate. Approval is per-MLS; your token only returns data for feeds you're approved on.
3. **Receive your API token** from the subscription's token tab in the MLS Grid web application. Treat it as a secret: `mlsgrid-sync` only accepts it via environment variable or file reference, never in committable config.

Your license tier (IDX / VOW / back-office) determines what you may *display* and to *whom*. The feed itself carries per-record usage tags in `MlgCanUse` — the tool stores them (`property.mlg_can_use`) so downstream code can filter, but **display compliance is your responsibility**, including honoring `InternetAddressDisplayYN` / `InternetEntireListingDisplayYN` and your MLS's display rules. MLSs conduct quarterly compliance audits of sites displaying their listings.

## Rules the tool enforces for you

| Rule | How mlsgrid-sync complies |
|---|---|
| Deleted/off-market records (`MlgCanView=false`) must be removed from your database | Hard-deleted automatically during incremental sync — property, children, media rows, and downloaded files. A periodic reconcile pass catches deletions missed while the tool wasn't running (false records leave the feed after ~7 days). |
| Rate limits: max 2 requests/second, 7,200 requests/hour, 40,000 requests/24h, 4 GB downloaded/hour | Built-in limiter runs *under* every cap by default (1.8 rps, 6,800/hr, 38,000/day, 3.5 GB/hr), aligned to wall-clock windows and persisted across restarts. On HTTP 429 the tool backs off (honoring `Retry-After`) and halts after repeated 429s rather than hammering a suspended token. |
| Media URLs are for downloading only — never hot-link them | Download mode fetches to your own disk/S3 storage. Metadata-only mode stores URLs for pipeline use; the docs and column comments state the no-hot-linking rule. |
| Media downloads must send a `User-Agent` header equal to your access token (enforced by MLS Grid since 2026-06-01) | Sent automatically on every media request; covered by tests. |
| One `OriginatingSystemName` per request | The query builder always scopes requests to a single configured system. |

## What happens if limits are exceeded

Exceeding usage limits suspends your access token: every request returns HTTP 429 and a shut-off notice goes to your subscription's primary contact email. Suspension lifts automatically once your trailing usage falls back within limits — which is why the tool's circuit breaker **stops requesting** instead of retrying through a suspension. If you legitimately need more throughput (e.g. a large initial backfill), email support@mlsgrid.com *in advance*.

## Your responsibilities (the tool cannot do these for you)

- Maintain a valid license and per-MLS approval for every feed you sync.
- Follow your license tier's display rules on any website/app that shows the data (IDX rules, attribution/"courtesy of" requirements, VOW registration requirements).
- Do not redistribute the data beyond what your license permits.
- Self-host any media you display; never serve MLS Grid URLs to end users.
- Keep your token secret and rotate it via MLS Grid support if it leaks.

## Sources

- MLS Grid API v2.0 documentation: https://docs.mlsgrid.com/api-documentation/api-version-2.0
- MLS Grid Data License Agreement: https://www.mlsgrid.com/s/MLS-Grid-Data-License-Agreement.pdf
- MLS Grid resources & FAQ: https://www.mlsgrid.com/resources
