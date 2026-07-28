#!/usr/bin/env python3
"""
Monthly compaction: roll a month of per-document JSON files into one Parquet
file per market, so a year of full-text SQL (DuckDB etc.) scans in seconds
instead of touching tens of thousands of small objects.

  python -m ingestion.compact                    # previous month, all markets
  python -m ingestion.compact --month 2026-07    # specific month
  python -m ingestion.compact --market uk        # one market
  python -m ingestion.compact --dry-run          # against local _lake/

Output:  compact/<market>/<YYYY-MM>.parquet   (+ .done.json marker)

Idempotent: re-running a month rewrites the file from whatever documents/
currently holds. The scheduled run (2nd of each month) compacts the previous
month after its final ingests have landed; a manual run on the current month
produces a partial file that the next run simply overwrites.

Raw PDFs are not compacted — they stay as sidecars under documents/; the
parquet carries their raw_key so an analyst can fetch the original.
"""

import argparse
import datetime
import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from . import lake

BATCH = 400  # documents pulled into memory per parquet row-group write

SCHEMA = pa.schema([
    ("doc_id", pa.string()),
    ("market", pa.string()),
    ("source", pa.string()),
    ("url", pa.string()),
    ("published_at", pa.string()),
    ("published_date", pa.string()),
    ("scraped_at", pa.string()),
    ("ticker", pa.string()),
    ("exchange_qualified", pa.string()),
    ("company_name", pa.string()),
    ("cik", pa.string()),
    ("doc_type", pa.string()),
    ("form", pa.string()),
    ("title", pa.string()),
    ("text", pa.string()),
    ("text_chars", pa.int64()),
    ("text_sha256", pa.string()),
    ("extraction", pa.string()),
    ("raw_key", pa.string()),
    ("truncated", pa.bool_()),
    ("is_admin_noise", pa.bool_()),
    ("noise_rule", pa.string()),
    ("scraper_version", pa.string()),
    ("run_id", pa.string()),
])


def _flatten(doc: dict):
    if not isinstance(doc, dict) or "doc_id" not in doc:
        return None
    company = doc.get("company") or {}
    content = doc.get("content") or {}
    flags = doc.get("flags") or {}
    scraper = doc.get("scraper") or {}
    text = content.get("text") or ""
    return {
        "doc_id": doc.get("doc_id"),
        "market": doc.get("market"),
        "source": doc.get("source"),
        "url": doc.get("url"),
        "published_at": str(doc.get("published_at") or ""),
        "published_date": doc.get("published_date"),
        "scraped_at": doc.get("scraped_at"),
        "ticker": company.get("ticker"),
        "exchange_qualified": company.get("exchange_qualified"),
        "company_name": company.get("name"),
        "cik": company.get("cik"),
        "doc_type": doc.get("doc_type"),
        "form": doc.get("form"),
        "title": doc.get("title"),
        "text": text,
        "text_chars": len(text),
        "text_sha256": content.get("text_sha256"),
        "extraction": content.get("extraction"),
        "raw_key": content.get("raw_key"),
        "truncated": bool(content.get("truncated")),
        "is_admin_noise": bool(flags.get("is_admin_noise")),
        "noise_rule": flags.get("noise_rule"),
        "scraper_version": scraper.get("version"),
        "run_id": scraper.get("run_id"),
    }


def previous_month() -> str:
    first = datetime.date.today().replace(day=1)
    prev = first - datetime.timedelta(days=1)
    return prev.strftime("%Y-%m")


def compact_market(store, market: str, month: str) -> dict:
    y, m = month.split("-")
    prefix = f"{lake.LAKE_PREFIX}documents/{market}/{y}/{m}/"
    keys = sorted(k for k in store.list_keys(prefix) if k.endswith(".json"))
    out_key = f"{lake.LAKE_PREFIX}compact/{market}/{month}.parquet"
    stats = {"market": market, "month": month, "docs_listed": len(keys),
             "rows_written": 0, "skipped_unreadable": 0, "parquet_key": None}

    if not keys:
        print(f"   {market} {month}: no documents — nothing to compact.")
        return stats

    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.close()
    writer = pq.ParquetWriter(tmp.name, SCHEMA, compression="zstd")
    try:
        for i in range(0, len(keys), BATCH):
            rows = []
            for k in keys[i:i + BATCH]:
                row = _flatten(store.get_json(k))
                if row is None:
                    stats["skipped_unreadable"] += 1
                    print(f"      ⚠️ unreadable/malformed, skipped: {k}")
                else:
                    rows.append(row)
            if rows:
                writer.write_table(pa.Table.from_pylist(rows, schema=SCHEMA))
                stats["rows_written"] += len(rows)
            print(f"   {market} {month}: {min(i + BATCH, len(keys))}/{len(keys)} processed", flush=True)
    finally:
        writer.close()

    store.put_file(out_key, tmp.name)
    os.unlink(tmp.name)
    stats["parquet_key"] = out_key

    # Read-back validation: the parquet must agree with what we wrote.
    check = pq.read_table(_fetch_local(store, out_key), columns=["doc_id"])
    if check.num_rows != stats["rows_written"]:
        raise RuntimeError(f"validation failed: parquet has {check.num_rows} rows, "
                           f"expected {stats['rows_written']}")
    print(f"   ✅ {market} {month}: {stats['rows_written']} rows -> {out_key}")
    return stats


def _fetch_local(store, key: str) -> str:
    """Pull a written parquet back to a temp path for validation."""
    data = store.get_bytes(key)
    if data is None:
        raise RuntimeError(f"validation failed: cannot read back {key}")
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.write(data)
    tmp.close()
    return tmp.name


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", default=previous_month(),
                    help="YYYY-MM (default: previous month)")
    ap.add_argument("--market", default="all",
                    choices=("all", "asx", "uk", "us", "otc"))
    ap.add_argument("--dry-run", action="store_true",
                    help="Compact a local _lake/ directory instead of S3")
    ap.add_argument("--local-root", default="_lake")
    args = ap.parse_args()

    try:
        datetime.datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        print(f"Bad --month {args.month!r}, expected YYYY-MM")
        return 2

    store = lake.make_store(args.dry_run, args.local_root)
    print(f"=== COMPACT {args.month} ({args.market}) -> {store.describe()} ===")
    if not store.preflight():
        print("❌ Storage preflight failed — aborting.")
        return 2

    markets = ("asx", "uk", "us", "otc") if args.market == "all" else (args.market,)
    failed = False
    for market in markets:
        try:
            stats = compact_market(store, market, args.month)
        except Exception as e:
            print(f"   ❌ {market} {args.month} failed: {e}")
            failed = True
            continue
        store.put_json(
            f"{lake.LAKE_PREFIX}compact/{market}/{args.month}.done.json",
            {**stats, "finished_at": lake.utcnow_iso(),
             "git_sha": os.environ.get("GITHUB_SHA", "local")[:12]})
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
