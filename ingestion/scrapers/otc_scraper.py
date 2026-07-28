#!/usr/bin/env python3
"""
OTC Markets scraper — Selenium pipeline lifted from the proven otc_news.py.

Preserved (the parts that were hard to get working):
  - ChromeOptions incl. force-device-scale-factor=0.65 zoom trick
  - overlay destruction via execute_script
  - the "More" button pagination loop with last-row date depth check
  - link extraction from the first table
  - per-item body-text fetch with the <p>-tag wait + body fallback

Robustness changes (CI kept timing out on driver.get of this heavy JS site):
  - page_load_strategy="eager" so get() returns at DOMContentLoaded instead of
    blocking on trackers/ads that may never finish loading (the read-timeout).
  - explicit page-load timeout + an outer retry loop around the whole collection.
  - wait for the news table to actually render before scraping it.
  - realistic User-Agent + optional headless mode (env OTC_HEADLESS).
  - on failure: dump the page title / URL / HTML snippet to STDOUT and save a
    screenshot + page source, so CI logs show whether it's a load hang or a
    bot-block instead of a bare timeout.

Run this module directly (`python otc_scraper.py`) for a fast, isolated OTC
self-test — no S3 / OpenAI / email involved.
"""

import os
import re
import sys
import time
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException

DAYS_BACK = int(os.environ.get("OTC_DAYS_BACK", "2"))
MAX_OTC_ITEMS = int(os.environ.get("MAX_OTC_ITEMS", "300"))

NEWS_URL = "https://www.otcmarkets.com/market-activity/news-filings"
USER_AGENT = os.environ.get(
    "OTC_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)
PAGE_LOAD_TIMEOUT = int(os.environ.get("OTC_PAGE_LOAD_TIMEOUT", "60"))
TABLE_WAIT = int(os.environ.get("OTC_TABLE_WAIT", "45"))
# The attached-document link (and inline press-release text) is injected by the
# page's JS a beat after load; poll up to this long for it before falling back.
DOC_LINK_WAIT = int(os.environ.get("OTC_DOC_LINK_WAIT", "14"))
# Timeout for downloading the attached announcement document (PDF).
DOC_TIMEOUT = int(os.environ.get("OTC_DOC_TIMEOUT", "30"))

# The real announcement (esp. exchange filings) is an attached PDF the page
# links to at .../news-otcapi/news/document/content/id?id=<docId>.
_DOC_URL_RE = re.compile(
    r'https?://[^\s"\'<>]*news-otcapi/news/document/content[^\s"\'<>]*', re.I)
COLLECT_ATTEMPTS = int(os.environ.get("OTC_COLLECT_ATTEMPTS", "3"))
RETRY_SLEEP = int(os.environ.get("OTC_RETRY_SLEEP", "5"))
HEADLESS = os.environ.get("OTC_HEADLESS", "0").lower() in ("1", "true", "yes")

# First table's data rows. Kept as a constant so the wait and the scrape agree.
ROWS_XPATH = "(//table)[1]/tbody/tr"


def _make_driver():
    options = webdriver.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("force-device-scale-factor=0.65")  # zoom trick — keep
    options.add_argument("--no-sandbox")            # required on CI
    options.add_argument("--disable-dev-shm-usage")  # required on CI
    options.add_argument("--disable-gpu")
    options.add_argument(f"--user-agent={USER_AGENT}")
    # eager = stop blocking once the DOM is ready; don't wait for every ad/tracker
    options.page_load_strategy = "eager"

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def _dump_debug(driver, tag: str):
    """Surface what the browser actually saw — to stdout (for CI logs) and files."""
    if driver is None:
        return
    try:
        title = driver.title
        cur = driver.current_url
        src = driver.page_source or ""
        print(f"    [debug:{tag}] title={title!r} url={cur!r} page_source_len={len(src)}")
        snippet = " ".join(src.split())[:800]
        print(f"    [debug:{tag}] html_snippet={snippet!r}")
    except Exception as e:
        print(f"    [debug:{tag}] could not read page state: {e}")
    try:
        driver.save_screenshot(f"otc_debug_{tag}.png")
    except Exception:
        pass
    try:
        with open(f"otc_debug_{tag}.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source or "")
    except Exception:
        pass


def _open_news_page(driver):
    """Navigate to the news page and wait for the table to render. Raises on failure."""
    print(f"[-] Opening {NEWS_URL} ...")
    try:
        driver.get(NEWS_URL)
    except TimeoutException:
        # eager strategy already returns early; if even that times out we still
        # try to use whatever DOM exists rather than aborting outright.
        print(f"[-] get() hit the {PAGE_LOAD_TIMEOUT}s page-load timeout — continuing with current DOM.")

    print("[-] Destroying overlays...")
    try:
        driver.execute_script("""
            var overlays = document.querySelectorAll('#onetrust-banner-sdk, .onetrust-pc-dark-filter, .sticky-footer');
            overlays.forEach(el => el.remove());
        """)
    except Exception:
        pass

    print(f"[-] Waiting up to {TABLE_WAIT}s for the news table to render...")
    WebDriverWait(driver, TABLE_WAIT).until(
        EC.presence_of_element_located((By.XPATH, ROWS_XPATH))
    )


def _paginate_and_extract(driver):
    wait = WebDriverWait(driver, 15)
    cutoff_date = datetime.now() - timedelta(days=DAYS_BACK)
    print(f"[-] Scanning for news since {cutoff_date.strftime('%Y-%m-%d')}...")

    while True:
        try:
            rows = driver.find_elements(By.XPATH, ROWS_XPATH)
            if rows:
                last_row = rows[-1]
                cols = last_row.find_elements(By.TAG_NAME, "td")
                if len(cols) > 0:
                    date_text = cols[0].text.strip()
                    try:
                        row_date = datetime.strptime(date_text, "%m/%d/%Y")
                        print(f"    Current Depth: {date_text} ({len(rows)} items queued)...", end="\r")
                        if row_date < cutoff_date:
                            break
                    except Exception:
                        pass

            more_btn = wait.until(EC.presence_of_element_located(
                (By.XPATH, "(//table)[1]/following::*[contains(text(), 'More')][1]")
            ))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_btn)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", more_btn)
            time.sleep(2)
        except (TimeoutException, NoSuchElementException):
            break
        except Exception:
            break

    print("\n[-] Pagination complete. Extracting links...")

    collected_news = []
    cutoff_date = datetime.now() - timedelta(days=DAYS_BACK)
    rows = driver.find_elements(By.XPATH, ROWS_XPATH)
    for row in rows:
        try:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 3:
                continue
            date_str = cols[0].text.strip()
            try:
                if datetime.strptime(date_str, "%m/%d/%Y") < cutoff_date:
                    continue
            except Exception:
                continue
            try:
                link_elem = cols[2].find_element(By.TAG_NAME, "a")
                collected_news.append({
                    "date": date_str,
                    "symbol": cols[1].text.split("\n")[-1].strip(),
                    "title": link_elem.text.strip(),
                    "url": link_elem.get_attribute("href"),
                })
            except Exception:
                continue
        except Exception:
            continue

    return collected_news


def collect_otc_news():
    """
    Returns (list_of_dicts, driver). Each dict: {date, symbol, title, url}.
    On total failure returns ([], None). Retries the whole flow a few times
    because CI browser/site startup is flaky.
    """
    print("--- 🚀 STARTING OTC DEEP SCAN (collection) ---")
    last_err = None

    for attempt in range(1, COLLECT_ATTEMPTS + 1):
        driver = None
        try:
            driver = _make_driver()
            _open_news_page(driver)
            collected_news = _paginate_and_extract(driver)

            if not collected_news:
                # Table rendered but yielded nothing usable — treat as a soft
                # failure worth a retry, and dump what we saw.
                raise RuntimeError("news table present but no rows extracted")

            print(f"[-] Queued {len(collected_news)} OTC items (attempt {attempt}).")
            if MAX_OTC_ITEMS and len(collected_news) > MAX_OTC_ITEMS:
                collected_news = collected_news[:MAX_OTC_ITEMS]
            return collected_news, driver

        except Exception as e:
            last_err = e
            print(f"[!] OTC collection attempt {attempt}/{COLLECT_ATTEMPTS} failed: {e}")
            _dump_debug(driver, f"attempt{attempt}")
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass
            if attempt < COLLECT_ATTEMPTS:
                time.sleep(RETRY_SLEEP)

    print(f"OTC collection failed entirely after {COLLECT_ATTEMPTS} attempts: {last_err}")
    return [], None


def _find_doc_url(driver):
    """Return the attached-document (PDF) URL on the current article page, or None."""
    try:
        for a in driver.find_elements(By.CSS_SELECTOR, "a[href*='news-otcapi/news/document']"):
            href = a.get_attribute("href")
            if href:
                return href
    except Exception:
        pass
    try:
        m = _DOC_URL_RE.search(driver.page_source or "")
        if m:
            return m.group(0)
    except Exception:
        pass
    return None


def _download_doc_text(driver, doc_url, referer):
    """Download the announcement's attached PDF and extract its text. Never raises."""
    try:
        import io
        import requests
        from pypdf import PdfReader
    except Exception as e:
        print(f"      [otc-doc] deps unavailable: {e}")
        return ""
    try:
        sess = requests.Session()
        # Carry the browser's cookies so the document endpoint doesn't 403.
        for c in driver.get_cookies():
            try:
                sess.cookies.set(c["name"], c["value"], domain=c.get("domain"))
            except Exception:
                pass
        r = sess.get(doc_url, timeout=DOC_TIMEOUT,
                     headers={"User-Agent": USER_AGENT, "Referer": referer,
                              "Accept": "application/pdf,*/*"})
        if r.status_code != 200 or not r.content:
            print(f"      [otc-doc] status {r.status_code}, {len(r.content or b'')} bytes")
            return ""
        ctype = (r.headers.get("Content-Type") or "").lower()
        if not ("pdf" in ctype or r.content[:5] == b"%PDF-"):
            print(f"      [otc-doc] not a pdf (ctype={ctype!r}, head={r.content[:8]!r})")
            return ""
        reader = PdfReader(io.BytesIO(r.content))
        text = "\n".join((pg.extract_text() or "") for pg in reader.pages)
        return " ".join(text.split())  # normalise whitespace
    except Exception as e:
        print(f"      [otc-doc] error: {e}")
        return ""


def _read_body(driver):
    try:
        return driver.find_element(By.TAG_NAME, "body").text or ""
    except Exception:
        return ""


def _poll_doc_or_prose(driver, url):
    """Poll up to DOC_LINK_WAIT. Returns (kind, text):
       'doc'  -> attached-document text (may be empty if parse failed)
       'body' -> inline press-release prose
       'none' -> nothing conclusive yet; text is best-effort page body.
    """
    body = ""
    end = time.time() + DOC_LINK_WAIT
    while True:
        doc_url = _find_doc_url(driver)
        if doc_url:
            return ("doc", _download_doc_text(driver, doc_url, referer=url))
        body = _read_body(driver)
        # Press release: substantial inline prose and no PDF-viewer markers.
        if len(body) > 2500 and "Loading PDF" not in body and "Page 1 of" not in body:
            return ("body", body)
        if time.time() >= end:
            return ("none", body)
        time.sleep(0.5)


def fetch_otc_body(driver, url: str) -> str:
    """Fetch the readable text of one OTC announcement. Never raises.

    Two kinds of announcement:
      1. Exchange filings (esp. cross-listed ASX names) attach a PDF — the page
         shows only the title + a PDF viewer. We find the attached-document URL
         and download + parse the PDF so we get the real content.
      2. Press releases render their text inline — we read the page body.

    The attached-document link is injected late by the page's JS and, when
    routing between news pages, sometimes doesn't appear on the first load — so
    if we don't find it (and the page isn't an inline press release), we
    re-navigate once and poll again, which usually renders it. Whatever happens,
    we return the best text we have (title + metadata at worst, never empty).
    """
    try:
        driver.get(url)
    except TimeoutException:
        pass  # eager load already returns early; use whatever rendered
    kind, text = _poll_doc_or_prose(driver, url)
    if kind == "doc" and len(text) > 200:
        return text
    if kind == "body":
        return text

    # One clean re-navigation usually renders a slow document link.
    try:
        driver.get(url)
    except TimeoutException:
        pass
    kind2, text2 = _poll_doc_or_prose(driver, url)
    if kind2 == "doc" and len(text2) > 200:
        return text2
    if kind2 == "body":
        return text2

    return text2 or text or _read_body(driver)


# --- standalone self-test: `python otc_scraper.py` --------------------------
if __name__ == "__main__":
    os.environ.setdefault("MAX_OTC_ITEMS", os.environ.get("MAX_OTC_ITEMS", "5"))
    print(f"=== OTC SELF-TEST (headless={HEADLESS}, days_back={DAYS_BACK}, "
          f"max_items={MAX_OTC_ITEMS}) ===")
    items, driver = collect_otc_news()
    print(f"=== COLLECTED {len(items)} items ===")
    for it in items[:5]:
        print(f"  - {it['date']} {it['symbol']:>8}  {it['title'][:60]!r}  {it['url']}")

    ok = bool(items)
    if driver is not None:
        # Dump the extracted body for each item so we can see PDF-filing text
        # (via the attached-document download) vs inline press-release text.
        for it in items[:5]:
            body = fetch_otc_body(driver, it["url"])
            print(f"\n=== BODY  {it['symbol']}  {it['title'][:50]!r}  ({len(body)} chars) ===")
            print(body[:400])
            print("=== END BODY ===")
        _dump_debug(driver, "final")
        try:
            driver.quit()
        except Exception:
            pass

    print("=== OTC SELF-TEST: " + ("PASS ===" if ok else "FAIL ==="))
    sys.exit(0 if ok else 1)
