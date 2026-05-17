"""Apply the Great Expectations suites against the warehouse.

Loads each JSON suite under great_expectations/expectations/, runs it against
the matching DuckDB table, and exits non-zero on any failed expectation —
that's how the Airflow DAG hard-gates `dbt run` results before they propagate
to gold / Postgres mirror.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
import great_expectations as ge


SUITE_DIR = Path("great_expectations/expectations")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/duckdb/claims.duckdb")

# Suite name → fully-qualified table name. Add new suites here.
SUITE_TO_TABLE: dict[str, str] = {
    "claims_silver": "silver.claims_enriched",
}


def main() -> int:
    if not Path(DUCKDB_PATH).exists():
        print(f"ERROR: {DUCKDB_PATH} not found — run `make dbt` first", file=sys.stderr)
        return 2

    failures = 0
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    for suite_name, table in SUITE_TO_TABLE.items():
        suite_path = SUITE_DIR / f"{suite_name}.json"
        if not suite_path.exists():
            print(f"skip: suite {suite_name}.json not found")
            continue
        df = con.execute(f"select * from {table}").df()
        gdf = ge.from_pandas(df)

        suite = json.loads(suite_path.read_text())
        print(f"\n=== {suite_name} → {table} ({len(df):,} rows) ===")
        for exp in suite["expectations"]:
            method_name = exp["expectation_type"]
            method = getattr(gdf, method_name, None)
            if method is None:
                print(f"  ! unknown expectation {method_name}")
                failures += 1
                continue
            result = method(**exp["kwargs"])
            status = "PASS" if result.success else "FAIL"
            print(f"  [{status}] {method_name} {exp['kwargs']}")
            if not result.success:
                failures += 1
                if "unexpected_count" in result.result:
                    print(f"          unexpected={result.result['unexpected_count']}")

    if failures:
        print(f"\n{failures} expectation(s) failed.")
        return 1
    print("\nall expectations passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
