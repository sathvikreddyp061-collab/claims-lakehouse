"""Materialize the Iceberg bronze table into DuckDB so dbt can read it.

dbt-duckdb sources work best against tables that live in the same DuckDB
file as the rest of the project. Rather than wire up the experimental
duckdb-iceberg extension (which still requires manually pointing at the
current snapshot's metadata.json), we just glob all parquet files in the
bronze data prefix via DuckDB's httpfs extension — which is what
`iceberg_scan` does under the hood for the data layer anyway.

Run via `make dbt` (which depends on this) — idempotent: drops + recreates
the bronze table on every call.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb


DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", "data/duckdb/claims.duckdb"))
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
ACCESS = os.getenv("S3_ACCESS_KEY", "minioadmin")
SECRET = os.getenv("S3_SECRET_KEY", "minioadmin")


def main() -> None:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # MinIO speaks S3 — configure the same alias the rest of the stack uses
    endpoint = S3_ENDPOINT.replace("http://", "").replace("https://", "")
    con.execute(f"SET s3_endpoint='{endpoint}'")
    con.execute(f"SET s3_access_key_id='{ACCESS}'")
    con.execute(f"SET s3_secret_access_key='{SECRET}'")
    con.execute("SET s3_url_style='path'")
    con.execute("SET s3_use_ssl=false")

    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("DROP TABLE IF EXISTS bronze.claims_edi837")

    glob = f"s3://{S3_BUCKET}/warehouse/bronze/claims_edi837/data/**/*.parquet"
    print(f"loading bronze from {glob}")
    con.execute(f"""
        CREATE TABLE bronze.claims_edi837 AS
        SELECT * FROM read_parquet('{glob}', hive_partitioning=1, union_by_name=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM bronze.claims_edi837").fetchone()[0]
    print(f"loaded {n:,} rows into bronze.claims_edi837")


if __name__ == "__main__":
    main()
