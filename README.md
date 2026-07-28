# market-ingestion

Shared **market-data lake** ingestion. Once a day per market this repo scrapes
company announcements/filings and writes them — text extracted, deduped,
flagged, **analysis-free** — into S3, so every downstream program (screeners,
watchlists, monitors) reads one store instead of scraping the same sites again.

| Market | Source | Scraper lineage | Schedule (UTC) |
|---|---|---|---|
| `asx` | asx.com.au today + previous-business-day announcements (Playwright + PDF) | `asx-analyst` (verbatim) | 00:35 Mon–Fri |
| `uk`  | Investegate RNS (HTTP) | `global-analyst/uk_scraper.py` (verbatim) | 21:45 Mon–Fri |
| `us`  | SEC EDGAR daily form index (HTTP) | `global-analyst/us_scraper.py` (verbatim) | 22:15 Mon–Fri |
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
- GitHub `schedule:` crons are best-effort and can slip under load. If a
  precise fire time ever matters, use the Cloudflare Worker
  `workflow_dispatch` pattern from `asx-analyst/scheduler`.
