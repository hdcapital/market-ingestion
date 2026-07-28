#!/usr/bin/env python3
"""ASX adapter: Playwright harvest + HTTP PDF download -> canonical documents.

Everything is stored, including admin paperwork — flagged, not dropped
(a watchlist legitimately cares about a substantial-holder notice even though
the screener skips it). The skip-pattern list is copied from the proven
asx-analyst analysis.py triage so the flag matches what the screener would do.

Native id = the PDF's MD5 (from the ETag when the server provides it, else
computed from the downloaded bytes) — the same identity the original ASX
scraper used.
"""

import hashlib
import io
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from .. import lake

SYDNEY = ZoneInfo("Australia/Sydney")

# --- verbatim skip patterns from asx-analyst/analysis.py --------------------
SKIP_TITLE_PATTERNS = [
    r"application for quotation",
    r"proposed issue of securities",
    r"notification regarding unquoted",
    r"notification of cessation",
    r"cleansing notice",
    r"cleansing statement",
    r"appendix 2a",
    r"appendix 3b",
    r"appendix 3g",
    r"appendix 3h",
    r"appendix 3x",
    r"appendix 3z",
    r"initial director'?s interest",
    r"final director'?s interest",
    r"becoming a substantial holder",
    r"ceasing to be a substantial holder",
    r"change in substantial holding",
    r"change of registry",
    r"change of registered office",
    r"change of company secretary",
    r"trading halt",
    r"suspension from official quotation",
    r"reinstatement to official quotation",
    r"listing rule waiver",
    r"constitution",
    r"notice of annual general meeting",
    r"notice of general meeting",
    r"proxy form",
    r"results of (annual general )?meeting",
    r"letter to (share|option)holders",
    r"dividend/distribution",
    r"update - dividend",
    r"daily fund update",
    r"net tangible asset backing",
    r"nta( and)?( monthly)? (backing|update)",
    r"estimated distribution",
    r"change of director'?s interest",
    r"appendix 3y",
]
_SKIP_RE = re.compile("|".join(SKIP_TITLE_PATTERNS), re.IGNORECASE)

MAX_PDF_PAGES = 80


def is_admin_noise(headline: str) -> bool:
    if not headline:
        return False
    return bool(_SKIP_RE.search(headline))


def extract_pdf_text(pdf_bytes: bytes):
    """PyMuPDF extraction with the asx-analyst quality classification."""
    import fitz
    try:
        doc = fitz.open(stream=io.BytesIO(pdf_bytes), filetype="pdf")
    except Exception:
        return "", "pdf-extraction-failed"
    try:
        chunks, total = [], 0
        for i in range(min(doc.page_count, MAX_PDF_PAGES)):
            try:
                t = doc.load_page(i).get_text("text")
            except Exception:
                t = ""
            chunks.append(t)
            total += len(t)
            if total >= lake.MAX_DOC_TEXT_CHARS:
                break
        text = "\n".join(chunks)
    finally:
        doc.close()
    if len(text.strip()) < 100:
        return text, "pdf-image-or-table"
    return text, "pdf-pymupdf"


def ingest(store, max_docs=None):
    from ..scrapers import asx_scraper

    stats = {"collected": 0, "written": 0, "skipped_seen": 0, "errors": 0}
    errors, entries = [], []

    links_info, cookies_dict = asx_scraper.harvest_links_and_cookies()
    stats["collected"] = len(links_info)
    if not links_info:
        # Weekend / pre-open: the today page can legitimately be empty.
        now = datetime.now(SYDNEY)
        status = "ok_empty" if now.weekday() >= 5 else "failed"
        if status == "failed":
            errors.append("ASX harvest returned no links on a weekday")
        return stats, errors, entries, status

    published_date = datetime.now(SYDNEY).strftime("%Y-%m-%d")
    seen = lake.load_seen_ids(store, "asx")

    for info in links_info:
        if max_docs and stats["written"] >= max_docs:
            break
        ticker, url, headline = info["ticker"], info["url"], info.get("headline", "")

        # HEAD first: the ETag is usually the PDF's MD5, so a repeat is skipped
        # without downloading — the original scraper's cheap-dedupe trick.
        source_md5, _size, _looks_pdf = asx_scraper.head_check(url, cookies_dict)
        if source_md5 and source_md5 in seen:
            stats["skipped_seen"] += 1
            continue

        try:
            content, full_md5 = asx_scraper.download_pdf(url, cookies_dict)
        except Exception as e:
            stats["errors"] += 1
            errors.append(f"download failed {ticker}: {e}")
            continue
        if content is None:
            stats["errors"] += 1
            errors.append(f"download skipped {ticker}: non-PDF or HTTP error")
            continue

        native_id = source_md5 or full_md5 or hashlib.md5(url.encode()).hexdigest()
        if native_id in seen:
            stats["skipped_seen"] += 1
            continue

        text, extraction = extract_pdf_text(content)
        doc = lake.build_doc(
            market="asx", source="asx-announcements", native_id=native_id, url=url,
            published_at=published_date, published_date=published_date,
            ticker=ticker, title=headline, text=text, extraction=extraction,
            is_admin_noise=is_admin_noise(headline),
            noise_rule="asx_admin_regex" if is_admin_noise(headline) else None,
        )
        if lake.write_document(store, doc, raw_bytes=content):
            stats["written"] += 1
            entries.append(lake.manifest_entry(doc))
            seen.add(native_id)
        else:
            stats["errors"] += 1
            errors.append(f"write failed: {native_id}")

    if stats["written"]:
        status = "ok"
    elif stats["skipped_seen"] and not errors:
        status = "ok_empty"
    else:
        status = "failed" if errors else "ok_empty"
    return stats, errors, entries, status
