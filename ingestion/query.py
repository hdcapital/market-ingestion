#!/usr/bin/env python3
"""
Ad-hoc SQL over the lake with DuckDB.

  QUERY_SQL='SELECT count(*) FROM docs' python -m ingestion.query

Exposes one view, `docs` — every document JSON in the lake (all markets,
all dates), with the canonical nested structure (company.ticker,
content.text, flags.is_admin_noise, ...). Results are printed to the log
(truncated for readability) and written in full to query_results.csv.

Intended to be driven by .github/workflows/query.yml, which supplies the
SQL as a workflow_dispatch input and uploads the CSV as an artifact.
"""

import os
import sys

import duckdb


DISPLAY_ROWS = 50
DISPLAY_CELL = 500


def main() -> int:
    sql = (os.environ.get("QUERY_SQL") or "").strip()
    if not sql:
        print("QUERY_SQL is empty — nothing to run.")
        return 2
    bucket = os.environ.get("AWS_BUCKET_NAME")
    prefix = os.environ.get("S3_LAKE_PREFIX", "market-data/")
    if not bucket:
        print("AWS_BUCKET_NAME not set.")
        return 2

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_region='{os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')}'")
    con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
    con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")

    glob = f"s3://{bucket}/{prefix}documents/*/*/*/*/*.json"
    print(f"Building docs view over {glob} ...")
    con.execute(f"""
        CREATE VIEW docs AS
        SELECT * FROM read_json_auto('{glob}',
                                     union_by_name=true,
                                     maximum_object_size=33554432)
    """)

    print(f"Running query:\n{sql}\n")
    rel = con.sql(sql)
    rel.write_csv("query_results.csv")

    # Re-read the CSV for display so we never hold two full result sets.
    rows = con.execute(
        "SELECT * FROM read_csv_auto('query_results.csv', header=true)").fetchall()
    cols = [d[0] for d in con.description]
    print(f"=== RESULT: {len(rows)} row(s) ===")
    print(" | ".join(cols))
    for r in rows[:DISPLAY_ROWS]:
        cells = []
        for v in r:
            s = str(v)
            cells.append(s[:DISPLAY_CELL] + "…" if len(s) > DISPLAY_CELL else s)
        print(" | ".join(cells))
    if len(rows) > DISPLAY_ROWS:
        print(f"... {len(rows) - DISPLAY_ROWS} more row(s) in the query_results.csv artifact")
    return 0


if __name__ == "__main__":
    sys.exit(main())
