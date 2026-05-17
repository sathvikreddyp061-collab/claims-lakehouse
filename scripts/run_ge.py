"""Apply the Great Expectations-compatible suites against the warehouse.

We honor the GX 0.18 JSON suite format (so the JSON file is a portable
data contract — you could feed the same file to a real GX runner) but we
evaluate each expectation directly in pandas. Reasons:
  * GX 0.18 has a pydantic-v1 import error on Python 3.12 (ForwardRef
    `_evaluate` signature change). GX 1.x has a different runtime API.
  * The 5 expectation types we use are trivially expressible in pandas —
    no need to drag in a 100MB framework for what's <30 lines of code.

The JSON suite stays as the source of truth. If we ever want the GX docs +
data-doc UI back, swap this runner without touching the suite files.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd


SUITE_DIR = Path("great_expectations/expectations")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/duckdb/claims.duckdb")

# Suite name → fully-qualified table name. Add new suites here.
SUITE_TO_TABLE: dict[str, str] = {
    "claims_silver": "silver.claims_enriched",
}


def _check(exp: dict, df: pd.DataFrame) -> tuple[bool, str]:
    t = exp["expectation_type"]
    k = exp["kwargs"]
    if t == "expect_column_values_to_not_be_null":
        n = df[k["column"]].isna().sum()
        return n == 0, f"nulls={n}"
    if t == "expect_column_values_to_be_unique":
        d = df[k["column"]].duplicated().sum()
        return d == 0, f"duplicates={d}"
    if t == "expect_column_values_to_be_between":
        s = df[k["column"]]
        bad = ((s < k["min_value"]) | (s > k["max_value"])).sum()
        return bad == 0, f"out_of_range={bad}"
    if t == "expect_column_values_to_be_in_set":
        s = df[k["column"]]
        bad = (~s.isin(k["value_set"])).sum()
        return bad == 0, f"unexpected={bad}"
    if t == "expect_table_row_count_to_be_between":
        n = len(df)
        ok = k["min_value"] <= n <= k["max_value"]
        return ok, f"rows={n}"
    return False, f"unknown expectation: {t}"


def main() -> int:
    if not Path(DUCKDB_PATH).exists():
        print(f"ERROR: {DUCKDB_PATH} not found — run `make dbt` first", file=sys.stderr)
        return 2

    failures = 0
    passes = 0
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    for suite_name, table in SUITE_TO_TABLE.items():
        suite_path = SUITE_DIR / f"{suite_name}.json"
        if not suite_path.exists():
            print(f"skip: suite {suite_name}.json not found")
            continue
        # dbt-duckdb materializes into the `main` database with the schema
        # name prefixed with `main_` (so `silver.claims_enriched` is actually
        # `main_silver.claims_enriched`). Try both before giving up.
        schema, name = table.split(".")
        candidates = [table, f"main_{schema}.{name}"]
        df: pd.DataFrame | None = None
        for cand in candidates:
            try:
                df = con.execute(f"select * from {cand}").df()
                table = cand
                break
            except duckdb.CatalogException:
                continue
        if df is None:
            print(f"ERROR: could not find table for {table}", file=sys.stderr)
            failures += 1
            continue

        suite = json.loads(suite_path.read_text())
        print(f"\n=== {suite_name} → {table} ({len(df):,} rows) ===")
        for exp in suite["expectations"]:
            ok, detail = _check(exp, df)
            status = "PASS" if ok else "FAIL"
            label = f"{exp['expectation_type']}({exp['kwargs'].get('column', '')})".strip()
            print(f"  [{status}] {label}  {detail}")
            if ok:
                passes += 1
            else:
                failures += 1

    print(f"\n{passes} passed, {failures} failed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
