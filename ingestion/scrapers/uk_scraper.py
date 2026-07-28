#!/usr/bin/env python3
"""
UK (Investegate/RNS) scraper — lifted from the proven investegate_scraper.py.

Preserved verbatim: DEFAULT_HEADERS, fetch(), parse_list_page(),
parse_detail_page(), canonical_rns_id(), clean_text().

Changes vs the original (deliberate, minimal):
  - Keyword/watchlist scoring removed — the AI screener replaces it.
  - Seen-history moves from .state/seen.json to S3 (handled by main.py),
    matching how the ASX screener dedupes.
  - TR-1 / PDMR noise filters kept, plus a small admin skip list in the
    same spirit as the ASX version.
"""

import os
import re
import html
import time
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparse

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"
}
# Generous per-request ceiling — Investegate detail pages can be slow to render.
REQUEST_TIMEOUT = int(os.environ.get("UK_REQUEST_TIMEOUT", "45"))
SLEEP_BETWEEN_ITEM_SEC = float(os.environ.get("UK_SLEEP_BETWEEN_ITEM_SEC", "0.8"))

INVESTEGATE_BASE = "https://www.investegate.co.uk"


# ----- verbatim helpers -----------------------------------------------------

def canonical_rns_id(url: str) -> str:
    """Extract an ID like '9272576' from an announcement url."""
    parts = url.rstrip("/").split("/")
    last = parts[-1] if parts else url
    return last.split("?")[0].strip().lower()


def clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip()


def fetch(url: str, session: requests.Session) -> Optional[str]:
    try:
        r = session.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return r.text
        return None
    except Exception:
        return None


def parse_list_page(html_text: str):
    soup = BeautifulSoup(html_text, "lxml")
    rows = []
    for a in soup.select('a[href*="/announcement/"]'):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(INVESTEGATE_BASE, href)
        headline = a.get_text(strip=True)
        rows.append({"href": full, "headline": headline})

    out, seen = [], set()
    for r in rows:
        cid = canonical_rns_id(r["href"])
        if cid and cid not in seen:
            seen.add(cid)
            out.append(r)
    return out


def parse_detail_page(html_text: str):
    soup = BeautifulSoup(html_text, "lxml")

    title = soup.find("h1").get_text(strip=True) if soup.find("h1") else ""
    parts = [t.get_text(" ", strip=True) for t in soup.find_all(["p", "li"])]
    body = clean_text("\n".join(parts))

    dt_iso = None
    match = re.search(r"\b\d{1,2}\s+\w+\s+\d{4}\b", soup.get_text(" ", strip=True))
    if match:
        try:
            dt_iso = dtparse.parse(match.group(0), dayfirst=True).isoformat()
        except Exception:
            dt_iso = None

    return title, body, dt_iso


# ----- noise filters (TR-1/PDMR preserved from original, plus admin list) ---

_UK_ADMIN_TITLE_RE = re.compile(
    r"total voting rights|holding\(s\) in company|tr-1|block listing|"
    r"transaction in own shares|director/pdmr|pdmr shareholding|"
    r"result of agm|result of general meeting|notice of agm|notice of gm|"
    r"publication of prospectus|publication of circular|posting of annual report|"
    r"dividend declaration|exchange rate|second price monitoring|price monitoring extension|"
    r"form 8|rule 2\.9|rule 8\.3|opening position disclosure",
    re.IGNORECASE,
)


def is_uk_admin_noise(title: str, body: str) -> bool:
    title_l = (title or "").lower()
    if _UK_ADMIN_TITLE_RE.search(title_l):
        return True
    # Routine TR-1 body signature (verbatim check from original)
    if body and re.search(r"\bacquisition or disposal of voting rights\b", body, re.I):
        return True
    # PDMR / director dealings title prefixes (verbatim from original)
    if title_l.startswith((
        "director/pdmr", "pdmr", "director dealings", "share dealings", "shareholding of directors"
    )):
        return True
    return False


# Investegate list headlines usually lead with the EPIC: "SGE Sage Group plc
# Final Results". Best-effort extraction for the market-snapshot lookup — a
# wrong guess just means a failed (fail-open) Yahoo lookup, never a lost item.
_EPIC_RE = re.compile(r"^([A-Z0-9]{2,6}(?:\.[A-Z])?)\s+\S")


def guess_ticker(headline: str) -> str:
    m = _EPIC_RE.match((headline or "").strip())
    return m.group(1) if m else ""


# ----- collection ------------------------------------------------------------

def collect_uk_announcements(pages: int = 3, per_page: int = 300, throttle: float = SLEEP_BETWEEN_ITEM_SEC):
    """
    Returns a list of dicts: {id, url, headline} for recent RNS items.
    Detail fetching is done lazily by the caller (after dedupe), one item at
    a time, to avoid pulling hundreds of pages we already analysed.
    """
    session = requests.Session()
    rows = []
    for p in range(1, pages + 1):
        html_text = fetch(f"{INVESTEGATE_BASE}/?perPage={per_page}&page={p}", session)
        if html_text:
            rows.extend(parse_list_page(html_text))
        time.sleep(0.5)

    out, seen = [], set()
    for r in rows:
        cid = canonical_rns_id(r["href"])
        if cid and cid not in seen:
            seen.add(cid)
            out.append({"id": cid, "url": r["href"], "headline": r["headline"]})
    return out, session


def fetch_uk_detail(session: requests.Session, url: str):
    """Returns (title, body, dt_iso) or (None, None, None) on failure."""
    html_text = fetch(url, session)
    if not html_text:
        return None, None, None
    return parse_detail_page(html_text)
