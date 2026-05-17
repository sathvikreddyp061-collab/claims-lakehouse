"""Sync the dbt gold marts (DuckDB) → Postgres mirror for the Vercel dashboard.

DuckDB can read its own tables via a regular SELECT; we batch-insert into
Postgres using psycopg's COPY for throughput. Idempotent: each table is
TRUNCATEd before the load. Both ends share the same column order.
"""

from __future__ import annotations

import io
import os
import sys

import duckdb
import psycopg


DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/duckdb/claims.duckdb")
PG_URL = os.getenv("POSTGRES_URL", "postgresql://claims:claims@localhost:5432/claims")


TABLES = {
    # duckdb fully-qualified name → postgres target
    "gold.member_360": "gold.member_360",
    "gold.claims_daily": "gold.claims_daily",
}


def main() -> int:
    ddb = duckdb.connect(DUCKDB_PATH, read_only=True)
    with psycopg.connect(PG_URL, autocommit=False) as pg:
        with pg.cursor() as cur:
            for src, dst in TABLES.items():
                df = ddb.execute(f"SELECT * FROM {src}").df()
                if df.empty:
                    print(f"skip {src} → {dst} (empty)")
                    continue
                cur.execute(f"TRUNCATE TABLE {dst}")
                buf = io.StringIO()
                df.to_csv(buf, index=False, header=False)
                buf.seek(0)
                cols = ", ".join(df.columns)
                copy_sql = f"COPY {dst} ({cols}) FROM STDIN WITH (FORMAT CSV)"
                with cur.copy(copy_sql) as cp:
                    cp.write(buf.read())
                print(f"loaded {len(df):,} rows → {dst}")
        pg.commit()
    return 0


if __name__ == "__main__":
    sys.exit(main())
