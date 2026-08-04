#!/usr/bin/env python3
"""Offline self-test: exercises the whole lake layer with fake documents —
no network, no S3. Run with `python -m ingestion.selftest`. Exits non-zero on
any failure so it can gate CI before live scraping starts."""

import datetime
import shutil
import sys
import tempfile

from . import lake

# Fixture dates must stay inside load_seen_ids' lookback window, so they are
# derived from the real clock rather than hardcoded.
TODAY = datetime.datetime.now(datetime.timezone.utc).date().isoformat()


def main() -> int:
    root = tempfile.mkdtemp(prefix="lake-selftest-")
    store = lake.LocalStore(root)
    failures = []

    def check(name, cond):
        print(f"  {'✅' if cond else '❌'} {name}")
        if not cond:
            failures.append(name)

    doc = lake.build_doc(
        market="uk", source="investegate-rns", native_id="9999999",
        url="https://example.com/a/9999999", published_at=f"{TODAY}T08:00:00",
        published_date=TODAY, ticker="TST", title="Test Results",
        text="hello world " * 50, extraction="html",
        is_admin_noise=False,
    )
    check("build_doc has doc_id", doc["doc_id"] == "uk:9999999")
    check("build_doc sha256 set", bool(doc["content"]["text_sha256"]))
    check("write_document", lake.write_document(store, doc))
    check("seen ids contains it", "9999999" in lake.load_seen_ids(store, "uk"))

    pdf_doc = lake.build_doc(
        market="asx", source="asx-announcements", native_id="a" * 32,
        url="https://example.com/pdf", published_at=TODAY,
        published_date=TODAY, ticker="TST", title="Quarterly",
        text="pdf text", extraction="pdf-pymupdf",
    )
    check("write_document with raw", lake.write_document(store, pdf_doc, raw_bytes=b"%PDF-fake"))
    check("raw sidecar recorded", pdf_doc["content"]["raw_key"].endswith(".pdf"))
    check("raw sidecar readable", store.get_bytes(pdf_doc["content"]["raw_key"]) == b"%PDF-fake")

    entries = [lake.manifest_entry(doc), lake.manifest_entry(pdf_doc)]
    mkey = lake.append_manifest(store, "uk", TODAY, entries[:1])
    lake.append_manifest(store, "uk", TODAY, entries[:1])  # idempotent re-append
    lines = store.get_bytes(mkey).decode().strip().splitlines()
    check("manifest idempotent", len(lines) == 1)

    dkey = lake.write_done_marker(store, "uk", TODAY, status="ok",
                                  counts={"written": 1}, errors=[],
                                  started_at=lake.utcnow_iso())
    marker = store.get_json(dkey)
    check("done marker readable", marker is not None and marker["status"] == "ok")

    big = lake.build_doc(
        market="us", source="sec-edgar-daily-index", native_id="0000000000-26-000001",
        url="https://example.com", published_at=TODAY, published_date=TODAY,
        text="x" * (lake.MAX_DOC_TEXT_CHARS + 10), extraction="sgml-strip",
    )
    check("oversize text truncated+flagged",
          big["content"]["truncated"] and len(big["content"]["text"]) == lake.MAX_DOC_TEXT_CHARS)

    # Regression (2026-08): a present-but-empty US_FORMS env parsed to an empty
    # set, so every filing was skipped and the run wrote an ok_empty marker —
    # a lost day of EDGAR looking exactly like a weekend. Blank values are the
    # normal case in Actions (unset repo variable, blank workflow input).
    from .scrapers import us_scraper as us
    check("blank US_FORMS parses to nothing",
          all(us.parse_forms(v) == set() for v in (None, "", "   ", ",")))
    check("blank US_FORMS falls back to the default list",
          all((us.parse_forms(v) or us.parse_forms(us.US_FORMS_DEFAULT))
              == us.parse_forms(us.US_FORMS_DEFAULT) for v in (None, "", "   ")))
    check("US_FORMS is never empty", bool(us.US_FORMS) and "8-K" in us.US_FORMS)
    check("explicit US_FORMS override still wins",
          us.parse_forms(" sc 13d/a , 8-K ") == {"SC 13D/A", "8-K"})

    shutil.rmtree(root, ignore_errors=True)
    print(f"\nSELFTEST: {'PASS' if not failures else 'FAIL ' + str(failures)}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
