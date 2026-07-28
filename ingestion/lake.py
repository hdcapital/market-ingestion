#!/usr/bin/env python3
"""
Storage layer for the market-data lake.

Layout (under S3_LAKE_PREFIX, default "market-data/"):

  documents/<market>/<YYYY>/<MM>/<DD>/<native_id>.json   canonical document
  documents/<market>/<YYYY>/<MM>/<DD>/<native_id>.pdf    raw artifact (PDF sources only)
  manifests/<market>/<YYYY-MM-DD>.jsonl                  one line per document written that day
  manifests/<market>/<YYYY-MM-DD>.done.json              run marker: status/counts/errors
  state/<market>/...                                     per-scraper state (cursors, caches)

Two backends with the same interface:
  S3Store    — production (boto3, bucket from AWS_BUCKET_NAME)
  LocalStore — dry runs / CI tests (same layout under a local directory)

The lake is append-only and analysis-free: scrapers write documents and flags,
consumers keep their own verdicts elsewhere. Documents are idempotent by key
(market + published date + native id), so re-running a day is safe.
"""

import datetime
import hashlib
import json
import os
from typing import Optional

SCHEMA_VERSION = 1
LAKE_PREFIX = os.environ.get("S3_LAKE_PREFIX", "market-data/")

AWS_BUCKET = os.environ.get("AWS_BUCKET_NAME")

# Cap stored text so one enormous 10-K exhibit can't balloon a document object.
MAX_DOC_TEXT_CHARS = int(os.environ.get("MAX_DOC_TEXT_CHARS", "300000"))


def utcnow_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

class LocalStore:
    """Same layout as S3, rooted at a local directory. Used by --dry-run."""

    def __init__(self, root: str):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _path(self, key: str) -> str:
        return os.path.join(self.root, key)

    def put_bytes(self, key: str, data: bytes) -> bool:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        return True

    def put_json(self, key: str, obj) -> bool:
        return self.put_bytes(key, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"))

    def get_bytes(self, key: str) -> Optional[bytes]:
        try:
            with open(self._path(key), "rb") as f:
                return f.read()
        except OSError:
            return None

    def get_json(self, key: str):
        raw = self.get_bytes(key)
        if raw is None:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    def list_keys(self, prefix: str) -> set:
        keys = set()
        base = self._path(prefix)
        for dirpath, _dirs, files in os.walk(base):
            for name in files:
                full = os.path.join(dirpath, name)
                keys.add(os.path.relpath(full, self.root).replace(os.sep, "/"))
        return keys

    def preflight(self) -> bool:
        return True

    def describe(self) -> str:
        return f"local:{self.root}"


class S3Store:
    """Production backend. Same helpers as the proven screener code."""

    def __init__(self, bucket: str):
        import boto3  # deferred so dry runs don't need it configured
        self.bucket = bucket
        self.client = boto3.client("s3")

    def put_bytes(self, key: str, data: bytes) -> bool:
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
            return True
        except Exception as e:
            print(f"      ❌ S3 PUT FAILED ({key}): {e}")
            return False

    def put_json(self, key: str, obj) -> bool:
        return self.put_bytes(key, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"))

    def get_bytes(self, key: str) -> Optional[bytes]:
        try:
            r = self.client.get_object(Bucket=self.bucket, Key=key)
            return r["Body"].read()
        except Exception:
            return None

    def get_json(self, key: str):
        raw = self.get_bytes(key)
        if raw is None:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    def list_keys(self, prefix: str) -> set:
        keys: set = set()
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.add(obj["Key"])
        except Exception as e:
            print(f"      ⚠️ S3 list failed for {prefix}: {e}")
        return keys

    def preflight(self) -> bool:
        """Probe list + write ONCE up front so a permissions problem is the
        first line of the log, not a silent throw-away of scraped work
        (the s3:PutObject-denied lesson from the screener's first run)."""
        ok = True
        try:
            self.client.list_objects_v2(Bucket=self.bucket, Prefix=LAKE_PREFIX, MaxKeys=1)
        except Exception as e:
            print(f"   ❌ S3 LIST check failed: {e}")
            ok = False
        probe = f"{LAKE_PREFIX}.preflight/probe-{os.getpid()}.txt"
        try:
            self.client.put_object(Bucket=self.bucket, Key=probe, Body=b"ok")
            try:
                self.client.delete_object(Bucket=self.bucket, Key=probe)
            except Exception:
                pass
        except Exception as e:
            print(f"   ❌ S3 WRITE check failed: {e}")
            ok = False
        return ok

    def describe(self) -> str:
        return f"s3://{self.bucket}/{LAKE_PREFIX}"


def make_store(dry_run: bool, local_root: str = "_lake"):
    if dry_run:
        return LocalStore(local_root)
    if not AWS_BUCKET or not os.environ.get("AWS_ACCESS_KEY_ID"):
        raise SystemExit(
            "AWS_BUCKET_NAME / AWS_ACCESS_KEY_ID not set. Add the AWS secrets "
            "(same four as the screener repos) or pass --dry-run."
        )
    return S3Store(AWS_BUCKET)


# ---------------------------------------------------------------------------
# Canonical documents
# ---------------------------------------------------------------------------

def doc_key(market: str, published_date: str, native_id: str, ext: str = "json") -> str:
    y, m, d = published_date.split("-")
    return f"{LAKE_PREFIX}documents/{market}/{y}/{m}/{d}/{native_id}.{ext}"


def build_doc(*, market: str, source: str, native_id: str, url: str,
              published_at: str, published_date: str,
              ticker: str = "", company_name: str = "", cik: Optional[str] = None,
              doc_type: str = "announcement", form: Optional[str] = None,
              title: str = "", text: str = "", extraction: str = "html",
              raw_key: Optional[str] = None,
              is_admin_noise: bool = False, noise_rule: Optional[str] = None) -> dict:
    text = (text or "")
    truncated = len(text) > MAX_DOC_TEXT_CHARS
    if truncated:
        text = text[:MAX_DOC_TEXT_CHARS]
    exchange = {"asx": "ASX", "uk": "LSE", "us": "", "otc": "OTC"}.get(market, "")
    return {
        "schema_version": SCHEMA_VERSION,
        "doc_id": f"{market}:{native_id}",
        "market": market,
        "source": source,
        "url": url,
        "published_at": published_at,
        "published_date": published_date,
        "scraped_at": utcnow_iso(),
        "company": {
            "ticker": ticker or None,
            "exchange_qualified": f"{exchange}:{ticker}" if (ticker and exchange) else None,
            "name": company_name or None,
            "cik": cik,
        },
        "doc_type": doc_type,
        "form": form,
        "title": title,
        "content": {
            "text": text,
            "text_sha256": hashlib.sha256(text.encode("utf-8", "replace")).hexdigest() if text else None,
            "extraction": extraction,
            "raw_key": raw_key,
            "truncated": truncated,
        },
        "flags": {
            "is_admin_noise": bool(is_admin_noise),
            "noise_rule": noise_rule,
        },
        "scraper": {
            "repo": "market-ingestion",
            "version": os.environ.get("GITHUB_SHA", "local")[:12],
            "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        },
    }


def write_document(store, doc: dict, raw_bytes: Optional[bytes] = None) -> bool:
    market = doc["market"]
    native_id = doc["doc_id"].split(":", 1)[1]
    key = doc_key(market, doc["published_date"], native_id)
    if raw_bytes:
        raw_key = doc_key(market, doc["published_date"], native_id, ext="pdf")
        doc["content"]["raw_key"] = raw_key
        if not store.put_bytes(raw_key, raw_bytes):
            return False
    return store.put_json(key, doc)


def manifest_entry(doc: dict) -> dict:
    native_id = doc["doc_id"].split(":", 1)[1]
    return {
        "doc_id": doc["doc_id"],
        "native_id": native_id,
        "published_date": doc["published_date"],
        "ticker": doc["company"]["ticker"],
        "form": doc["form"],
        "title": (doc["title"] or "")[:200],
        "key": doc_key(doc["market"], doc["published_date"], native_id),
        "text_chars": len(doc["content"]["text"] or ""),
        "text_sha256": doc["content"]["text_sha256"],
        "is_admin_noise": doc["flags"]["is_admin_noise"],
    }


# ---------------------------------------------------------------------------
# Dedupe, manifests, done markers
# ---------------------------------------------------------------------------

def load_seen_ids(store, market: str, days_back: int = 6) -> set:
    """Native ids already in the lake over the last `days_back` date partitions.
    One listing pass per date — the screener's list-once-then-test-in-memory
    pattern, so a mostly-repeat day costs almost nothing."""
    seen: set = set()
    today = datetime.datetime.now(datetime.timezone.utc).date()
    for delta in range(days_back + 1):
        d = today - datetime.timedelta(days=delta)
        prefix = f"{LAKE_PREFIX}documents/{market}/{d.year}/{d.month:02d}/{d.day:02d}/"
        for key in store.list_keys(prefix):
            name = key.rsplit("/", 1)[-1]
            if name.endswith(".json"):
                seen.add(name[:-5])
    return seen


def append_manifest(store, market: str, run_date: str, entries: list) -> str:
    """Merge new entries into the day's JSONL manifest (idempotent by doc_id)."""
    key = f"{LAKE_PREFIX}manifests/{market}/{run_date}.jsonl"
    existing, order = {}, []
    raw = store.get_bytes(key)
    if raw:
        for line in raw.decode("utf-8", "replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
                if e.get("doc_id") not in existing:
                    order.append(e["doc_id"])
                existing[e.get("doc_id")] = e
            except Exception:
                continue
    for e in entries:
        if e["doc_id"] not in existing:
            order.append(e["doc_id"])
        existing[e["doc_id"]] = e
    body = "\n".join(json.dumps(existing[d], ensure_ascii=False) for d in order)
    store.put_bytes(key, (body + "\n").encode("utf-8") if body else b"")
    return key


def write_done_marker(store, market: str, run_date: str, *, status: str,
                      counts: dict, errors: list, started_at: str) -> str:
    """The timing contract: consumers check this before reading a day."""
    key = f"{LAKE_PREFIX}manifests/{market}/{run_date}.done.json"
    store.put_json(key, {
        "schema_version": SCHEMA_VERSION,
        "market": market,
        "run_date": run_date,
        "status": status,                      # ok | ok_empty | failed
        "started_at": started_at,
        "finished_at": utcnow_iso(),
        "counts": counts,
        "errors": errors[:20],
        "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "git_sha": os.environ.get("GITHUB_SHA", "local")[:12],
    })
    return key
