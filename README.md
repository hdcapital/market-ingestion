# market-ingestion

Shared **market-data lake** ingestion. Once a day per market this repo scrapes
company announcements/filings and writes them — text extracted, deduped,
flagged, **analysis-free** — into S3, so every downstream program (screeners,
watchlists, monitors) reads one store instead of scraping the same sites again.

| Market | Source | Scraper lineage | Schedule (UTC) |
|---|---|---|---|
| `asx` | asx.com.au today + previous-business-day announcements (Playwright + PDF) | `asx-analyst` (verbatim) | 00:35 Mon–Fri |
| `uk`  | Investegate RNS (HTTP) | `global-analyst/uk_scraper.py` (verbatim) | 21:45 Mon–Fri |
| `us`  | SEC EDGAR daily form index (HTTP) | `global-analyst/us_scraper.py` (verbatim) | 02:30 Tue–Sat (EDGAR posts day D's index ~02:00 UTC on D+1) |
| `otc` | otcmarkets.com news (Selenium) | `global-analyst/otc_scraper.py` (verbatim) | 22:30 Mon–Fri |

The scraper modules in `ingestion/scrapers/` are copied **verbatim** from the
proven repos — the weird bits (ASX interstitial handling, the touch-the-first-
PDF trick, the OTC overlay destruction and zoom trick) are the reason they
work. Don't clean them up. The thin adapters in `ingestion/adapters/` are the
only new logic: they turn scraper output into canonical documents.

## Lake layout (under `market-data/` in the bucket)

```
documents/<market>/<YYYY>/<MM>/<DD>/<native_id>.json   canonical document
documents/<market>/<YYYY>/<MM>/<DD>/<native_id>.pdf    raw PDF (ASX)
manifests/<market>/<YYYY-MM-DD>.jsonl                  one line per doc written that day
manifests/<market>/<YYYY-MM-DD>.done.json              run marker (the timing contract)
compact/<market>/<YYYY-MM>.parquet                     one-file-per-month rollup for fast SQL
state/<market>/                                        scraper state
```

**Consumer contract:** before reading a day, check
`manifests/<market>/<date>.done.json`. `status` is `ok`, `ok_empty`
(legitimate quiet day — weekends etc.) or `failed`. Never assume an absent
marker means an empty day. Most queries should read the small JSONL manifests
first and fetch only matching documents.

**Native IDs** are the same identities the existing programs already use, so
old records reconcile: UK = canonical RNS id, US = SEC accession number,
OTC = `md5(url)[:12]`, ASX = PDF MD5 (ETag-derived when available).

**Noise is flagged, never dropped**: `flags.is_admin_noise` carries each
market's proven triage regex (Appendix 3B/3Y, TR-1/PDMR, admin-only 8-K items).
The screener skips flagged docs; a watchlist may deliberately read them.
The US adapter applies **no SIC exclusions** — that's a screener judgement.

## Running

```bash
pip install -r requirements.txt
python -m ingestion.selftest                       # offline, no network
python -m ingestion.run --market uk --max-docs 3 --dry-run   # local _lake/
python -m ingestion.run --market uk                # real: needs AWS env vars
```

Secrets (same four as the screener repos): `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_BUCKET_NAME`, `AWS_DEFAULT_REGION`. The IAM
policy must allow list/get/put/delete under `market-data/*`. Each workflow
also accepts `max_docs` / `dry_run` inputs for cheap manual tests.

## Querying (analyst workflow)

Sync locally, then query with DuckDB — the monthly parquet files make a
year-long full-text scan take seconds:

```bash
aws s3 sync s3://YOUR-BUCKET/market-data/ ~/market-data/
```

```sql
-- everything a ticker announced, ex-noise
SELECT published_date, title, url
FROM read_parquet('~/market-data/compact/asx/*.parquet')
WHERE ticker = 'PPS' AND NOT is_admin_noise
ORDER BY published_date;

-- full-text across all US filings
SELECT published_date, company_name, form, url
FROM read_parquet('~/market-data/compact/us/*.parquet')
WHERE text ILIKE '%going concern%';
```

The current month isn't compacted yet — query it straight from the JSON:
`read_json_auto('~/market-data/documents/us/2026/*/*/*.json')`. Raw ASX PDFs
stay under documents/ (the parquet's raw_key column points at them).

Compaction runs automatically on the 2nd of each month for the previous
month (see ingest schedules below); re-running a month is safe and simply
rewrites the file.

No local sync handy? The "Query lake" workflow (`query.yml`) runs DuckDB SQL
in Actions against two views: `docs` (document JSONs, nested structure) and
`docs_compact` (the monthly parquet, flat columns, much faster). Pass the
`market` and/or `month` inputs to narrow the scan — an unrestricted `docs`
scan grows with the lake and can run long.

## Operational notes

- **Forward-only.** First run starts from "now"; there is no backfill mode.
- **Caps are circuit breakers, not filters.** Defaults (1M chars/doc, 5,000 US
  filings/day, 1,000 OTC items/day, 300 PDF pages) sit far above realistic
  peaks; when one binds the log says what was dropped.
- **Idempotent.** Documents are keyed by market + published date + native id;
  re-running a day rewrites identical objects and the manifest merge dedupes.
- **Failure = loud.** Storage preflight runs before any scraping; a scheduled
  run that fails shows a red X and GitHub emails the repo owner. Every
  downstream program depends on these runs — do not mute those notifications.
- **Budget.** The two browser markets are the slow ones (ASX ~10–20 min,
  OTC ~10–20 min); UK/US are HTTP-only. On a private repo this eats the
  2,000 free Actions minutes; making this repo public (it holds no secrets
  and only public-market data) makes minutes unlimited, as with
  `investegate-scraper`.
- **Timing is owned by the Cloudflare Worker scheduler** (`scheduler/` in this repo),
  which fires each ingest via `workflow_dispatch` at: ASX 00:35, UK 21:45,
  OTC 22:30 (UTC, Mon–Fri) and US 02:30 (UTC, Tue–Sat — EDGAR posts day D's
  index ~02:00 UTC on D+1). GitHub `schedule:` crons were removed for the
  other markets — GitHub silently never fired them for this repo — but
  `ingest-us.yml` keeps one at 03:17 UTC Tue–Sat as a fallback behind the
  Worker: a duplicate ingest is free (the lake dedupes), a missed one loses a
  day (the 2026-08-11 Worker-migration gap cost the lake a whole US day). The
  monthly compact job keeps its GitHub cron (a late fire is harmless there).
- **The screener is chained, not scheduled, on ASX days.** `ingest-asx.yml`
  dispatches `hdcapital/global-analyst`'s `daily.yml` as its final step, so
  the screener only ever fires after the day's ASX done-marker exists — a
  fixed screener slot raced long reporting-season ingests (2026-08-28: a
  63-minute ingest overran the old 01:30 UTC fire and the ASX day was
  screened a day late). This needs the `CONSUMER_DISPATCH_PAT` repo secret
  (fine-grained PAT, Actions read+write on `hdcapital/global-analyst`; the
  scheduler Worker's `GH_PAT` value works). The Worker keeps a Saturday
  01:30 UTC screener fire (no ASX ingest to chain from that day) and a
  Mon–Fri 02:15 UTC fallback fire in case the ingest fire itself is missed —
  a duplicate screener run dedupes to a cheap no-op.
