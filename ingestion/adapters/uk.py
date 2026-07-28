#!/usr/bin/env python3
"""UK (Investegate/RNS) adapter: scrape -> canonical documents. No AI, no email."""

import os
import time

from .. import lake


def ingest(store, max_docs=None):
    from ..scrapers import uk_scraper

    stats = {"collected": 0, "written": 0, "skipped_seen": 0, "errors": 0}
    errors, entries = [], []

    pages = 1 if (max_docs and max_docs <= 20) else int(os.environ.get("UK_PAGES", "3"))
    per_page = 100 if (max_docs and max_docs <= 20) else int(os.environ.get("UK_PER_PAGE", "300"))

    items, session = uk_scraper.collect_uk_announcements(pages=pages, per_page=per_page)
    stats["collected"] = len(items)
    if not items:
        errors.append("Investegate listing returned no announcements")
        return stats, errors, entries, "failed"

    seen = lake.load_seen_ids(store, "uk")
    for it in items:
        if max_docs and stats["written"] >= max_docs:
            break
        if it["id"] in seen:
            stats["skipped_seen"] += 1
            continue
        title, body, dt_iso = uk_scraper.fetch_uk_detail(session, it["url"])
        if title is None and body is None:
            stats["errors"] += 1
            errors.append(f"detail fetch failed: {it['url']}")
            continue
        published_date = (dt_iso or lake.today_utc())[:10]
        headline = it.get("headline", "")
        doc = lake.build_doc(
            market="uk", source="investegate-rns", native_id=it["id"], url=it["url"],
            published_at=dt_iso or lake.utcnow_iso(), published_date=published_date,
            ticker=uk_scraper.guess_ticker(headline),
            title=title or headline, text=body or "", extraction="html",
            is_admin_noise=uk_scraper.is_uk_admin_noise(title or headline, body or ""),
            noise_rule="uk_admin_regex",
        )
        if lake.write_document(store, doc):
            stats["written"] += 1
            entries.append(lake.manifest_entry(doc))
            seen.add(it["id"])
        else:
            stats["errors"] += 1
            errors.append(f"write failed: {it['id']}")
        time.sleep(uk_scraper.SLEEP_BETWEEN_ITEM_SEC)

    status = "ok" if stats["written"] else ("ok_empty" if not errors else "failed")
    if stats["written"] and errors:
        status = "ok"  # partial success still counts; errors stay visible in the marker
    return stats, errors, entries, status
