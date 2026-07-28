#!/usr/bin/env python3
"""
Entry point: python -m ingestion.run --market {asx,uk,us,otc} [--max-docs N] [--dry-run]

One market per invocation (one Actions job per market keeps failures isolated).
Writes documents + a daily manifest + a .done.json run marker, then validates
one written document against the schema so a structural regression fails the
run loudly instead of poisoning the lake quietly.
"""

import argparse
import importlib
import os
import sys
import traceback

MARKETS = ("asx", "uk", "us", "otc")

REQUIRED_DOC_FIELDS = (
    "schema_version", "doc_id", "market", "source", "url", "published_at",
    "published_date", "scraped_at", "company", "doc_type", "title",
    "content", "flags", "scraper",
)


def apply_test_caps(market: str, max_docs: int):
    """Shrink the scrapers' own collection knobs for small test runs so a
    3-document test doesn't paginate an entire day first."""
    if market == "otc":
        os.environ.setdefault("MAX_OTC_ITEMS", str(max(max_docs * 4, 20)))
        os.environ.setdefault("OTC_DAYS_BACK", "2")
    if market == "us":
        os.environ.setdefault("MAX_US_FILINGS", str(max(max_docs * 10, 50)))


def validate_written(store, entries) -> bool:
    if not entries:
        print("VALIDATION: nothing written this run — skipping document check.")
        return True
    doc = store.get_json(entries[0]["key"])
    if doc is None:
        print(f"VALIDATION FAILED: could not read back {entries[0]['key']}")
        return False
    missing = [f for f in REQUIRED_DOC_FIELDS if f not in doc]
    if missing:
        print(f"VALIDATION FAILED: missing fields {missing}")
        return False
    preview = dict(doc)
    preview["content"] = dict(doc["content"])
    preview["content"]["text"] = (doc["content"]["text"] or "")[:300] + "…"
    import json
    print("VALIDATION: PASS — sample document:")
    print(json.dumps(preview, indent=2, ensure_ascii=False)[:2500])
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=MARKETS)
    ap.add_argument("--max-docs", type=int, default=0,
                    help="Cap documents written this run (0 = uncapped)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Write to a local _lake/ directory instead of S3")
    ap.add_argument("--local-root", default="_lake")
    args = ap.parse_args()

    if args.max_docs:
        apply_test_caps(args.market, args.max_docs)

    from . import lake  # after env tweaks
    store = lake.make_store(args.dry_run, args.local_root)
    print(f"=== INGEST {args.market.upper()} -> {store.describe()} "
          f"(max_docs={args.max_docs or 'uncapped'}) ===")
    if not store.preflight():
        print("❌ Storage preflight failed — aborting before any scraping.")
        return 2

    started_at = lake.utcnow_iso()
    run_date = lake.today_utc()
    adapter = importlib.import_module(f".adapters.{args.market}", package="ingestion")

    try:
        stats, errors, entries, status = adapter.ingest(
            store, max_docs=args.max_docs or None)
    except Exception as e:
        traceback.print_exc()
        lake.write_done_marker(store, args.market, run_date, status="failed",
                               counts={}, errors=[f"unhandled: {e}"],
                               started_at=started_at)
        return 1

    mkey = lake.append_manifest(store, args.market, run_date, entries)
    dkey = lake.write_done_marker(store, args.market, run_date, status=status,
                                  counts=stats, errors=errors,
                                  started_at=started_at)

    print(f"\n=== RESULT {args.market.upper()}: {status} ===")
    print(f"  stats    : {stats}")
    print(f"  manifest : {mkey}")
    print(f"  marker   : {dkey}")
    for e in errors[:10]:
        print(f"  ⚠️ {e}")

    if not validate_written(store, entries):
        return 1
    return 0 if status in ("ok", "ok_empty") else 1


if __name__ == "__main__":
    sys.exit(main())
