"""Read Synthea CSV exports into typed pandas DataFrames.

Synthea ships a fixed set of CSVs: patients, encounters, claims, organizations,
providers, payers, immunizations, etc. We use the subset that actually feeds
the EDI 837 generator: patients, encounters, claims, providers, payers.

See https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary
for the canonical schema.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


SYNTHEA_OUTPUT = Path(os.getenv("SYNTHEA_OUTPUT_DIR", "data/synthea_output/csv"))


def _read(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = SYNTHEA_OUTPUT / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — did you run `make synthea`?"
        )
    return pd.read_csv(path, parse_dates=parse_dates, low_memory=False)


def patients() -> pd.DataFrame:
    df = _read("patients", parse_dates=["BIRTHDATE", "DEATHDATE"])
    df.columns = df.columns.str.lower()
    return df


def encounters() -> pd.DataFrame:
    df = _read("encounters", parse_dates=["START", "STOP"])
    df.columns = df.columns.str.lower()
    return df


def claims() -> pd.DataFrame:
    """Synthea claims.csv has one row per claim, plus claims_transactions.csv
    with line-item detail. We use claims for the EDI 837 header + minimum
    required line items (a single CL line per claim is fine for the demo)."""
    df = _read("claims", parse_dates=["SERVICEDATE", "LASTBILLEDDATE1"])
    df.columns = df.columns.str.lower()
    return df


def providers() -> pd.DataFrame:
    df = _read("providers")
    df.columns = df.columns.str.lower()
    return df


def payers() -> pd.DataFrame:
    df = _read("payers")
    df.columns = df.columns.str.lower()
    return df


def organizations() -> pd.DataFrame:
    df = _read("organizations")
    df.columns = df.columns.str.lower()
    return df
