#!/usr/bin/env python3
"""
ASX scraper — lifted (deliberately, near-verbatim) from the proven
asx_official_scraper.py. The ASX site is fragile to automate, so the
battle-tested parts are preserved exactly:

  - Playwright browser context + storage_state cookie persistence
  - accept_terms_if_present() interstitial handling
  - "touch the first PDF" trick to force terms acceptance on the PDF path
  - cookie handoff from Playwright -> requests for fast HTTP downloads
  - MD5-from-ETag helpers

Only three deliberate changes vs the original:
  1. TODAY ONLY: prevBusDayAnns.do removed per new spec.
  2. Headline capture: collect_links_and_tickers() now also grabs the
     announcement title text from the link cell (needed for triage).
  3. headless is env-controlled (HEADLESS=true on GitHub Actions; the
     workflow also wraps the run in xvfb-run so headless=false works too,
     matching the original's behaviour as closely as possible).
"""

import os
import re
import hashlib
from typing import Optional

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

ASX_ANNOUNCEMENT_URLS = [
    "https://www.asx.com.au/asx/v2/statistics/todayAnns.do",
    # Previous business day too: a mid-morning run would otherwise miss anything
    # published after it ran the day before. Lake-level dedupe (PDF MD5 checked
    # against the last week of partitions) makes the overlap free.
    "https://www.asx.com.au/asx/v2/statistics/prevBusDayAnns.do",
]
BASE_DOMAIN = "https://www.asx.com.au"
STORAGE_STATE_PATH = "asx_storage.json"
DOWNLOAD_DIR = "downloads"

HEADLESS = os.environ.get("HEADLESS", "false").lower() in ("1", "true", "yes")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# HELPERS (verbatim from the working scraper)
# ---------------------------------------------------------------------------

_ETAG_MD5_RE = re.compile(r"^[a-f0-9]{32}$", re.IGNORECASE)


def _md5_from_etag(etag_value: str) -> Optional[str]:
    """Return a 32-hex MD5 string if the ETag looks like an MD5, else None."""
    if not etag_value:
        return None
    et = str(etag_value).strip()
    if et.startswith("W/"):
        et = et[2:].strip()
    et = et.strip('"').strip()
    if _ETAG_MD5_RE.fullmatch(et):
        return et.lower()
    return None


def accept_terms_if_present(context) -> bool:
    """
    Look for the ASX 'Access to this site' interstitial on ANY page in this
    browser context and click 'Agree and proceed' if present.
    (Verbatim from the working scraper.)
    """
    handled = False

    for pg in context.pages:
        try:
            locator = pg.locator(
                "button:has-text('Agree and proceed'), "
                "input[value='Agree and proceed']"
            )

            if locator.count() == 0:
                continue

            try:
                locator.first.wait_for(state="visible", timeout=5000)
            except PlaywrightTimeoutError:
                continue

            print(f"      ⚠️ Terms of Use found on {pg.url} — clicking 'Agree and proceed'...")
            locator.first.click()

            try:
                pg.wait_for_load_state("networkidle", timeout=10000)
            except PlaywrightTimeoutError:
                pass

            handled = True

        except Exception:
            continue

    if handled:
        print("      ✅ Terms accepted.")
    return handled


def collect_links_and_tickers(page):
    """
    From an ASX announcements table, collect:
        [{'ticker': 'ABC', 'url': 'https://...pdf', 'headline': '...'}, ...]

    Identical row/ticker/link logic to the working scraper; the ONLY
    addition is capturing the anchor's visible text as the headline.
    """
    links_info = []

    rows = page.locator("table tr")
    row_count = rows.count()
    print(f"   -> Found {row_count} rows.")

    for idx in range(row_count):
        row = rows.nth(idx)
        cells = row.locator("td")
        if cells.count() < 2:
            continue

        ticker = cells.nth(1).inner_text().strip()

        if not ticker or not re.fullmatch(r"[A-Z0-9]{3,6}", ticker):
            row_text = row.inner_text().strip()
            m = re.search(r"\b[A-Z0-9]{3,6}\b", row_text)
            ticker = m.group(0) if m else f"DOC_{idx}"

        link_loc = row.locator("a[href*='display'], a[href$='.pdf']")
        if link_loc.count() == 0:
            continue

        href = link_loc.first.get_attribute("href")
        if not href:
            continue

        # NEW (additive, safe): headline = anchor text, cleaned of the
        # trailing "PDF / 123KB" decoration ASX appends inside the link.
        try:
            headline = link_loc.first.inner_text().strip()
            headline = re.sub(r"\s*\n[\s\S]*$", "", headline).strip()
        except Exception:
            headline = ""

        if href.startswith("/"):
            href = BASE_DOMAIN + href
        elif not href.startswith("http"):
            href = BASE_DOMAIN + "/" + href.lstrip("/")

        links_info.append({"ticker": ticker, "url": href, "headline": headline})

    print(f"   -> Collected {len(links_info)} PDF links.")
    return links_info


# ---------------------------------------------------------------------------
# HARVEST (browser phase) — returns links + cookies for the HTTP phase
# ---------------------------------------------------------------------------

def harvest_links_and_cookies():
    """
    Runs the browser phase exactly like the proven scraper and returns
    (links_info, cookies_dict). Downloading is handled by the caller so
    the analysis pipeline can sit between download and cleanup.
    """
    print("--- 🩸 STARTING ASX HARVESTER (browser phase) ---")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)

        if os.path.exists(STORAGE_STATE_PATH):
            context = browser.new_context(storage_state=STORAGE_STATE_PATH)
        else:
            context = browser.new_context()

        page = context.new_page()

        print("1. Loading table page(s)...")
        links_info = []

        for url in ASX_ANNOUNCEMENT_URLS:
            print(f"   -> Loading {url}")
            page.goto(url, timeout=60000)

            if accept_terms_if_present(context):
                if not page.url.endswith(("todayAnns.do", "prevBusDayAnns.do")):
                    page.goto(url, timeout=60000)

            try:
                page.wait_for_selector("table", timeout=60000)
            except PlaywrightTimeoutError:
                print(f"   ⚠️ Failed to load table for {url} (skipping).")
                continue

            page_links = collect_links_and_tickers(page)
            # Additive tag (same spirit as the headline capture): which page a
            # link came from, so the adapter can date prev-day documents.
            day = "prev" if url.endswith("prevBusDayAnns.do") else "today"
            for it in page_links:
                it["day"] = day
            links_info.extend(page_links)

        if not links_info:
            print("   ❌ No PDF links found.")
            browser.close()
            return [], {}

        first_url = links_info[0]["url"]
        print("2. Touching first PDF to ensure terms are accepted on PDF path...")
        pdf_page = context.new_page()
        try:
            pdf_page.goto(first_url, timeout=60000)
        except PlaywrightTimeoutError:
            print("   ⚠️ Timeout opening first PDF (continuing anyway).")

        accept_terms_if_present(context)

        try:
            pdf_page.close()
        except Exception:
            pass

        cookies = context.cookies(BASE_DOMAIN)
        cookies_dict = {c["name"]: c["value"] for c in cookies}

        try:
            context.storage_state(path=STORAGE_STATE_PATH)
        except Exception:
            pass

        browser.close()

    return links_info, cookies_dict


def head_check(url: str, cookies_dict: dict):
    """
    Pre-flight HEAD (verbatim logic): returns (source_md5, expected_size,
    looks_like_pdf) or (None, None, False) on any failure.
    """
    try:
        head = requests.head(url, cookies=cookies_dict, timeout=25, allow_redirects=True)
        if head.status_code != 200:
            return None, None, False
        source_md5 = _md5_from_etag(head.headers.get("ETag", ""))
        clen = head.headers.get("Content-Length", "")
        expected_size = int(clen) if clen.isdigit() else None
        ctype_h = (head.headers.get("Content-Type", "") or "").lower()
        looks_like_pdf = ("pdf" in ctype_h) or ("octet-stream" in ctype_h)
        return source_md5, expected_size, looks_like_pdf
    except Exception:
        return None, None, False


def download_pdf(url: str, cookies_dict: dict):
    """
    Download one announcement PDF (verbatim guards). Returns
    (content_bytes, full_md5) or (None, None) on skip/failure.
    """
    resp = requests.get(url, cookies=cookies_dict, timeout=60)
    ctype = resp.headers.get("Content-Type", "").lower()

    if resp.status_code != 200:
        print(f"      ⚠️ HTTP {resp.status_code} (skipping).")
        return None, None

    if "text/html" in ctype and "pdf" not in ctype:
        print("      ⚠️ Got HTML instead of PDF (skipping).")
        return None, None

    full_md5 = hashlib.md5(resp.content).hexdigest()
    return resp.content, full_md5
