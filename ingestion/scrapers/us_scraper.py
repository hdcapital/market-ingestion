#!/usr/bin/env python3
"""
US (SEC EDGAR) scraper.

Access patterns lifted from the proven app10.py:
  - SEC_USER_AGENT header (SEC requires an identifying UA — do not remove)
  - http_get_with_retry() with backoff
  - _build_full_submission_txt_url() accession URL construction
  - normalize_text_for_matching() text cleanup

Different from app10 BY DESIGN: app10 is your special-situations agent and
uses EFTS keyword search. This module feeds the LONG-TERM FUNDAMENTAL
screener, so instead of keywords it walks the EDGAR daily index and takes
the DOMESTIC-issuer filings where fundamentals actually move:
  - 8-K / 8-K/A : current reports (results, guidance, disposals, management)
  - 10-K / 10-K/A : annual report (full MD&A, cash flow, balance sheet)
  - 10-Q / 10-Q/A : quarterly report

Foreign private issuers file 6-K (current) and 20-F / 40-F (annual) instead;
those forms are deliberately EXCLUDED so the US feed is domestic filers only.
(Note: a company incorporated abroad but registered with the SEC as a
domestic filer still files 10-K/8-K and will therefore appear — the form
index has no cleaner country flag than the reporting regime itself.)

Forms and caps are configurable via env. 10-Q season is high-volume and each
filing is large, so MAX_US_FILINGS caps the daily intake — raise it for fuller
coverage at higher OpenAI cost.
"""

import os
import re
import time
import datetime
from typing import Any, Dict, List, Optional

import requests

SEC_USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "HD Capital Partners (contact: daniel@hdcp.com.au)",  # same UA as app10
)
HTTP_MAX_RETRIES = 4
HTTP_BACKOFF_SECONDS = 2.0
# Generous — full-submission .txt files can be large and slow to serve.
DOWNLOAD_TIMEOUT = int(os.environ.get("SEC_DOWNLOAD_TIMEOUT", "90"))
SLEEP_SECONDS = float(os.environ.get("SEC_SLEEP_SECONDS", "0.25"))

# Domestic-issuer fundamental forms (8-K/10-K/10-Q + amendments) PLUS the
# special-situation forms where Lane-B events actually surface:
#   SC TO-I / SC TO-T : issuer / third-party tender offers (odd-lot provisions)
#   SC 14D9           : target's response to a tender
#   SC 13D            : activist stake with stated intentions (initial filing
#                       only — /A amendments are high-volume noise)
#   SC 13E3           : going-private transaction (controller/insider buyout of
#                       minorities, squeeze-out) — initial filing only
#   25 / 25-NSE       : ACTUAL delisting notification
#   15-12B / 15-12G   : deregistration / going dark
# Foreign private issuers' 6-K/20-F/40-F are intentionally omitted (see module
# docstring). Override via the US_FORMS env.
US_FORMS = set(
    f.strip().upper()
    for f in os.environ.get(
        "US_FORMS",
        "8-K,8-K/A,10-K,10-K/A,10-Q,10-Q/A,"
        "SC TO-I,SC TO-T,SC 14D9,SC 13D,SC 13E3,25,25-NSE,15-12B,15-12G").split(",")
    if f.strip()
)

# Forms that are terse regulatory notifications where the form's EXISTENCE is
# the event (a Form 25 delisting can be a page of checkboxes). These bypass the
# minimum-body-length guard so they still reach the model.
TERSE_EVENT_FORMS = {"25", "25-NSE", "15-12B", "15-12G", "SC 13D", "SC 13E3"}
US_LOOKBACK_DAYS = int(os.environ.get("US_LOOKBACK_DAYS", "2"))
MAX_US_FILINGS = int(os.environ.get("MAX_US_FILINGS", "1500"))

# SIC (industry) pre-filter — drop a filing BEFORE downloading or paying the LLM
# when the company's SIC code marks it as a structural hard-exclusion the model
# would reject anyway. Kept DELIBERATELY NARROW to the pure commodity-extraction
# codes, where the "profitable hidden gem" probability is ~zero under this
# strategy. We do NOT exclude oilfield/mining *services* codes (a diversified
# recurring-revenue services firm is in scope) or investment/holding codes
# (6726/6770) — a company trading below its hidden assets is exactly a needle
# and often sits under those codes. Add codes via EXCLUDE_SIC to go harder.
US_SIC_FILTER = os.environ.get("US_SIC_FILTER", "true").lower() in ("1", "true", "yes")
EXCLUDE_SIC = set(
    s.strip()
    # `or default` (not get's default) so an EMPTY env value — e.g. an unset
    # GitHub repo variable resolving to "" — falls back instead of disabling it.
    for s in (os.environ.get("EXCLUDE_SIC") or
              "1000,1040,1090,1220,1221,1311").split(",")  # mining + crude oil & gas
    if s.strip()
)


def fetch_sic(session: requests.Session, cik: str) -> str:
    """Look up a filer's SIC industry code from the SEC submissions API.

    One tiny cached fetch per company (see filter_by_sic). Returns '' on any
    failure so the caller FAILS OPEN — an unknown company is always kept and
    read, never silently dropped.
    """
    try:
        url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
        r = http_get_with_retry(session, url, label=f"submissions {cik}")
        if r is None:
            return ""
        return str(r.json().get("sic") or "").strip()
    except Exception:
        return ""


BROWSE_EDGAR = "https://www.sec.gov/cgi-bin/browse-edgar"
# CIKs appear either as atom <CIK>0001234567</CIK> tags or in company-row hrefs
# as ...&CIK=0001234567... (>=3 digits avoids stray small numbers on the page).
_CIK_RE = re.compile(r"(?:<CIK>|[?&]CIK=)0*(\d{3,10})", re.IGNORECASE)


def _ciks_for_sic(session: requests.Session, sic: str, page_cap: int = 80) -> set:
    """Every CIK EDGAR lists under a given SIC industry code (paginated)."""
    found: set = set()
    start, count = 0, 100
    for pg in range(page_cap):
        params = dict(action="getcompany", SIC=sic, type="", dateb="",
                      owner="include", count=count, start=start, output="atom")
        r = http_get_with_retry(session, BROWSE_EDGAR, params=params,
                                label=f"browse SIC {sic} @{start}")
        if r is None:
            if pg == 0:
                print(f"      [seed] SIC {sic}: request returned None (non-200).")
            break
        text = r.text
        page = {str(int(c)) for c in _CIK_RE.findall(text)}
        if pg == 0 and not page:
            # One-shot diagnostic so a parse/endpoint mismatch is visible in the
            # Actions log instead of silently seeding nothing.
            print(f"      [seed] SIC {sic}: {len(text)} bytes, 0 CIKs parsed. "
                  f"head={text[:500]!r}")
        new = page - found
        if not new:
            break  # no fresh CIKs on this page -> reached the end
        found |= page
        start += count
        time.sleep(SLEEP_SECONDS)
    return found


def seed_excluded_ciks(sic_cache: Dict[str, str], sics=None) -> int:
    """Enumerate every company under each excluded SIC from EDGAR and add
    {cik: sic} to sic_cache. Returns how many NEW ciks were added.

    Best-effort: a network error on one SIC just yields fewer entries, never an
    exception. This lets the screener exclude the whole known commodity-
    extraction universe from its very next run instead of learning each company
    lazily the first time it happens to file.
    """
    sics = sorted(sics if sics is not None else EXCLUDE_SIC)
    session = requests.Session()
    added = 0
    for sic in sics:
        try:
            ciks = _ciks_for_sic(session, sic)
        except Exception as e:
            print(f"   ⚠️ seed SIC {sic} failed: {e}")
            continue
        fresh = 0
        for c in ciks:
            if c not in sic_cache:
                sic_cache[c] = sic
                fresh += 1
        added += fresh
        print(f"   • SIC {sic}: {len(ciks)} companies listed ({fresh} new)")
    return added


def filter_by_sic(filings: List[Dict[str, Any]], sic_cache: Dict[str, str]):
    """Split filings into (kept, dropped) by SIC, enriching sic_cache in place.

    sic_cache maps cik -> sic code and is persisted in S3 across runs, so each
    company is looked up from the SEC exactly once, ever. Returns
    (kept, dropped, cache_changed). Fails open: a company whose SIC can't be
    resolved is kept.
    """
    if not (US_SIC_FILTER and EXCLUDE_SIC):
        return filings, [], False
    session = requests.Session()
    kept, dropped = [], []
    changed = False
    for f in filings:
        cik = str(f.get("cik") or "")
        sic = sic_cache.get(cik)
        if sic is None:
            sic = fetch_sic(session, cik)
            sic_cache[cik] = sic  # cache even '' so we don't refetch failures forever
            changed = True
            time.sleep(SLEEP_SECONDS)
        if sic and sic in EXCLUDE_SIC:
            f["_excluded_sic"] = sic
            dropped.append(f)
        else:
            kept.append(f)
    return kept, dropped, changed

# 8-K item codes that are pure admin when they appear ALONE (with 9.01 exhibits):
#   5.07 vote results, 5.03 bylaws/fiscal year, 3.03 modification of rights,
#   5.08 director nominations deadline, 7.01 Reg FD only, 9.01 exhibits.
ADMIN_ONLY_ITEMS = {"5.07", "5.03", "3.03", "5.08", "7.01", "9.01"}
_ITEM_RE = re.compile(r"\bItem\s+(\d{1,2}\.\d{2})", re.IGNORECASE)


def http_get_with_retry(session: requests.Session, url: str, *, params=None,
                        timeout=DOWNLOAD_TIMEOUT, label="") -> Optional[requests.Response]:
    """Lifted retry pattern from app10.py."""
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            r = session.get(url, params=params, timeout=timeout,
                            headers={"User-Agent": SEC_USER_AGENT})
            if r.status_code == 200:
                return r
            if r.status_code in (403, 429, 500, 502, 503, 504):
                time.sleep(HTTP_BACKOFF_SECONDS * attempt)
                continue
            return None
        except Exception:
            time.sleep(HTTP_BACKOFF_SECONDS * attempt)
    print(f"      [!] Giving up on {label or url}")
    return None


def _accession_nodash(accession: str) -> str:
    return accession.replace("-", "")


def build_full_submission_txt_url(cik: str, accession: str) -> str:
    """Verbatim from app10.py."""
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{_accession_nodash(accession)}/{accession}.txt"


def normalize_text_for_matching(text: str) -> str:
    """Verbatim from app10.py."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"-\s*\n\s*", "", text)
    text = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", text)
    text = re.sub(r"[\u2018\u2019]", "'", text)
    text = re.sub(r'[\u201C\u201D]', '"', text)
    return text


def strip_sgml_noise(text: str) -> str:
    """
    Full-submission .txt files are SGML wrappers containing HTML, XBRL and
    base64 exhibits. Keep only readable prose: drop uuencoded/base64 blocks,
    XML sections and tags, then collapse whitespace.
    """
    # Drop binary/graphic/zip documents entirely
    text = re.sub(r"<DOCUMENT>\s*<TYPE>(GRAPHIC|ZIP|EXCEL|EX-101[^\n]*|XML)[\s\S]*?</DOCUMENT>",
                  " ", text, flags=re.IGNORECASE)
    # Drop XBRL and XML islands
    text = re.sub(r"<XBRL[\s\S]*?</XBRL>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<\?xml[\s\S]*?>", " ", text)
    # Strip HTML/SGML tags
    text = re.sub(r"<[^>]{1,300}>", " ", text)
    # Kill leftover base64-looking runs
    text = re.sub(r"(?:[A-Za-z0-9+/=]{60,}\s*){5,}", " ", text)
    text = re.sub(r"&nbsp;?", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = normalize_text_for_matching(text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def is_admin_only_8k(text_head: str) -> bool:
    """True if every 8-K item mentioned in the header is in the admin set."""
    items = set(_ITEM_RE.findall(text_head[:20000]))
    if not items:
        return False  # can't tell -> let the AI decide
    return items.issubset(ADMIN_ONLY_ITEMS)


def _quarter(month: int) -> int:
    return (month - 1) // 3 + 1


def load_cik_ticker_map(session: requests.Session) -> Dict[str, str]:
    """CIK -> ticker from SEC's official mapping file (one small fetch)."""
    r = http_get_with_retry(session, "https://www.sec.gov/files/company_tickers.json",
                            label="cik->ticker map")
    if r is None:
        return {}
    try:
        data = r.json()
        return {str(v["cik_str"]): v["ticker"] for v in data.values()}
    except Exception:
        return {}


def collect_us_filings() -> List[Dict[str, Any]]:
    """
    Walk the EDGAR daily form index for the last US_LOOKBACK_DAYS and return:
      [{id (accession), cik, ticker, company, form, date, url}, ...]
    Weekends/holidays 404 and are skipped silently. Dedupe across days is by
    accession number; S3-level dedupe in main.py makes the overlap free.
    """
    session = requests.Session()
    tickers = load_cik_ticker_map(session)

    filings: Dict[str, Dict[str, Any]] = {}
    today_us = datetime.datetime.now(datetime.timezone.utc).date()

    for delta in range(US_LOOKBACK_DAYS + 1):
        d = today_us - datetime.timedelta(days=delta)
        url = (f"https://www.sec.gov/Archives/edgar/daily-index/"
               f"{d.year}/QTR{_quarter(d.month)}/form.{d.strftime('%Y%m%d')}.idx")
        r = http_get_with_retry(session, url, label=f"daily index {d}")
        if r is None:
            continue

        # form.idx is fixed-width-ish: Form Type | Company | CIK | Date | File Name
        for line in r.text.splitlines():
            m = re.match(r"^(\S[\S /-]*?)\s{2,}(.+?)\s{2,}(\d+)\s{2,}(\d{4}-?\d{2}-?\d{2})\s{2,}(\S+\.txt)\s*$", line)
            if not m:
                continue
            form, company, cik, fdate, path = m.groups()
            if form.strip().upper() not in US_FORMS:
                continue
            acc_m = re.search(r"(\d{10}-\d{2}-\d{6})", path)
            if not acc_m:
                continue
            accession = acc_m.group(1)
            if accession in filings:
                continue
            filings[accession] = {
                "id": accession,
                "cik": cik,
                "ticker": tickers.get(str(int(cik)), ""),
                "company": company.strip(),
                "form": form.strip().upper(),
                "date": fdate,
                "url": f"https://www.sec.gov/Archives/{path.strip()}",
            }
        time.sleep(SLEEP_SECONDS)

    out = sorted(filings.values(), key=lambda x: x["date"], reverse=True)
    if MAX_US_FILINGS and len(out) > MAX_US_FILINGS:
        print(f"   ⚠️ Capping US filings at {MAX_US_FILINGS} of {len(out)} (raise MAX_US_FILINGS to widen).")
        out = out[:MAX_US_FILINGS]
    return out


def fetch_us_filing_text(filing: Dict[str, Any]) -> Optional[str]:
    session = requests.Session()
    r = http_get_with_retry(session, filing["url"], label=f"filing {filing['id']}")
    if r is None:
        return None
    time.sleep(SLEEP_SECONDS)
    return r.text
