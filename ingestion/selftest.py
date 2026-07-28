#!/usr/bin/env python3
"""Offline self-test: exercises the whole lake layer with fake documents —
no network, no S3. Run with `python -m ingestion.selftest`. Exits non-zero on
any failure so it can gate CI before live scraping starts."""

import shutil
import sys
import tempfile

from . import lake


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
        url="https://example.com/a/9999999", published_at="2026-07-27T08:00:00",
        published_date="2026-07-27", ticker="TST", title="Test Results",
        text="hello world " * 50, extraction="html",
        is_admin_noise=False,
    )
    check("build_doc has doc_id", doc["doc_id"] == "uk:9999999")
    check("build_doc sha256 set", bool(doc["content"]["text_sha256"]))
    check("write_document", lake.write_document(store, doc))
    check("seen ids contains it", "9999999" in lake.load_seen_ids(store, "uk"))

    pdf_doc = lake.build_doc(
        market="asx", source="asx-announcements", native_id="a" * 32,
        url="https://example.com/pdf", published_at="2026-07-27",
        published_date="2026-07-27", ticker="TST", title="Quarterly",
        text="pdf text", extraction="pdf-pymupdf",
    )
    check("write_document with raw", lake.write_document(store, pdf_doc, raw_bytes=b"%PDF-fake"))
    check("raw sidecar recorded", pdf_doc["content"]["raw_key"].endswith(".pdf"))
    check("raw sidecar readable", store.get_bytes(pdf_doc["content"]["raw_key"]) == b"%PDF-fake")

    entries = [lake.manifest_entry(doc), lake.manifest_entry(pdf_doc)]
    mkey = lake.append_manifest(store, "uk", "2026-07-27", entries[:1])
    lake.append_manifest(store, "uk", "2026-07-27", entries[:1])  # idempotent re-append
    lines = store.get_bytes(mkey).decode().strip().splitlines()
    check("manifest idempotent", len(lines) == 1)

    dkey = lake.write_done_marker(store, "uk", "2026-07-27", status="ok",
                                  counts={"written": 1}, errors=[],
                                  started_at=lake.utcnow_iso())
    marker = store.get_json(dkey)
    check("done marker readable", marker is not None and marker["status"] == "ok")

    big = lake.build_doc(
        market="us", source="sec-edgar-daily-index", native_id="0000000000-26-000001",
        url="https://example.com", published_at="2026-07-27", published_date="2026-07-27",
        text="x" * (lake.MAX_DOC_TEXT_CHARS + 10), extraction="sgml-strip",
    )
    check("oversize text truncated+flagged",
          big["content"]["truncated"] and len(big["content"]["text"]) == lake.MAX_DOC_TEXT_CHARS)

    shutil.rmtree(root, ignore_errors=True)
    print(f"\nSELFTEST: {'PASS' if not failures else 'FAIL ' + str(failures)}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
