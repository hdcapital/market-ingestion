#!/usr/bin/env python3
"""US (SEC EDGAR) adapter: daily-index walk -> canonical documents.

Deliberately NO SIC exclusion filter here — that is a screener-side judgement.
The lake stores every in-scope form; consumers apply their own exclusions.
"""

from .. import lake


def ingest(store, max_docs=None):
    from ..scrapers import us_scraper

    stats = {"collected": 0, "written": 0, "skipped_seen": 0, "errors": 0}
    errors, entries = [], []

    filings = us_scraper.collect_us_filings()
    stats["collected"] = len(filings)
    if not filings:
        # Weekend/holiday indexes 404 — that is a legitimate empty day.
        return stats, errors, entries, "ok_empty"

    seen = lake.load_seen_ids(store, "us")
    for f in filings:
        if max_docs and stats["written"] >= max_docs:
            break
        if f["id"] in seen:
            stats["skipped_seen"] += 1
            continue
        raw = us_scraper.fetch_us_filing_text(f)
        if raw is None:
            stats["errors"] += 1
            errors.append(f"download failed: {f['id']}")
            continue
        text = us_scraper.strip_sgml_noise(raw)
        admin = f["form"].startswith("8-K") and us_scraper.is_admin_only_8k(text)
        # The daily form index writes dates as YYYYMMDD (no dashes) — normalise
        # to ISO for the date-partitioned lake key; fall back to today if odd.
        digits = (f.get("date") or "").replace("-", "")
        if len(digits) == 8 and digits.isdigit():
            published = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
        else:
            published = lake.today_utc()
        doc = lake.build_doc(
            market="us", source="sec-edgar-daily-index", native_id=f["id"], url=f["url"],
            published_at=published, published_date=published,
            ticker=f.get("ticker", ""), company_name=f.get("company", ""),
            cik=str(int(f["cik"])) if f.get("cik") else None,
            doc_type="filing", form=f["form"],
            title=f"{f['form']} — {f.get('company', '')}",
            text=text, extraction="sgml-strip",
            is_admin_noise=admin, noise_rule="admin_only_8k_items" if admin else None,
        )
        if lake.write_document(store, doc):
            stats["written"] += 1
            entries.append(lake.manifest_entry(doc))
            seen.add(f["id"])
        else:
            stats["errors"] += 1
            errors.append(f"write failed: {f['id']}")

    status = "ok" if stats["written"] else ("ok_empty" if stats["skipped_seen"] else "failed")
    if not stats["written"] and not errors:
        status = "ok_empty"
    return stats, errors, entries, status
