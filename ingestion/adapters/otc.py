#!/usr/bin/env python3
"""OTC Markets adapter: Selenium news scan -> canonical documents.

Syndicated stories (same title pushed out for many symbols) are fetched once
and cached within the run; every (symbol, story) pair is still stored as its
own document — consumers dedupe with content.text_sha256 if they want to.
"""

import hashlib
import re
from datetime import datetime

from .. import lake

_WS_RE = re.compile(r"\s+")


def _story_key(title: str) -> str:
    return _WS_RE.sub(" ", (title or "").strip().lower())


def ingest(store, max_docs=None):
    from ..scrapers import otc_scraper

    stats = {"collected": 0, "written": 0, "skipped_seen": 0, "errors": 0}
    errors, entries = [], []

    items, driver = otc_scraper.collect_otc_news()
    stats["collected"] = len(items)
    if not items:
        return stats, ["OTC collection returned no items (site block or empty day)"], entries, "failed"

    try:
        seen = lake.load_seen_ids(store, "otc")
        body_cache = {}
        for it in items:
            if max_docs and stats["written"] >= max_docs:
                break
            native_id = hashlib.md5(it["url"].encode()).hexdigest()[:12]
            if native_id in seen:
                stats["skipped_seen"] += 1
                continue
            skey = _story_key(it["title"])
            if skey in body_cache:
                body = body_cache[skey]
            else:
                body = otc_scraper.fetch_otc_body(driver, it["url"])
                body_cache[skey] = body
            if not body:
                stats["errors"] += 1
                errors.append(f"empty body: {it['url']}")
                continue
            try:
                pub = datetime.strptime(it["date"], "%m/%d/%Y").strftime("%Y-%m-%d")
            except Exception:
                pub = lake.today_utc()
            doc = lake.build_doc(
                market="otc", source="otcmarkets-news", native_id=native_id, url=it["url"],
                published_at=pub, published_date=pub,
                ticker=it.get("symbol", ""), doc_type="news",
                title=it.get("title", ""), text=body, extraction="browser",
            )
            if lake.write_document(store, doc):
                stats["written"] += 1
                entries.append(lake.manifest_entry(doc))
                seen.add(native_id)
            else:
                stats["errors"] += 1
                errors.append(f"write failed: {native_id}")
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    if stats["written"]:
        status = "ok"
    elif stats["skipped_seen"] and not errors:
        status = "ok_empty"
    else:
        status = "failed" if errors else "ok_empty"
    return stats, errors, entries, status
