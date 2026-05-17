"""Convert Synthea CSV claims → EDI 837 files.

Output: one .edi file per claim under data/edi_output/, plus a single JSONL
manifest (`manifest.jsonl`) that the Kafka publisher reads. Each manifest line:
  { "claim_id": ..., "edi_path": "...", "service_date": "YYYY-MM-DD", ... }

This step is deterministic — re-running over the same Synthea export
produces byte-identical files. Easy to diff in CI.
"""

from __future__ import annotations

import json
import os
import random
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd

from producer.edi.x12_837 import Claim837, ServiceLine, serialize
from producer.synthea import reader


EDI_OUT = Path(os.getenv("EDI_OUTPUT_DIR", "data/edi_output"))


def _to_date(v) -> date:
    if isinstance(v, pd.Timestamp):
        return v.date()
    if isinstance(v, str):
        return pd.to_datetime(v).date()
    return v or date.today()


def build_claims_df() -> pd.DataFrame:
    """Join the Synthea CSVs into a single denormalized claim DataFrame."""
    pts = reader.patients()
    enc = reader.encounters()
    cls = reader.claims()
    prov = reader.providers()
    payers = reader.payers()

    # Patient enrichment
    pts = pts.rename(columns={"id": "patient_id"})[
        ["patient_id", "first", "last", "birthdate", "address", "state"]
    ]
    # Encounter → place-of-service inference (very rough — encounter class)
    enc = enc.rename(columns={"id": "encounter_id", "patient": "patient_id"})
    enc["place_of_service"] = enc["encounterclass"].map(
        {"ambulatory": "11", "outpatient": "22", "inpatient": "21",
         "emergency": "23", "wellness": "11", "urgentcare": "20"}
    ).fillna("11")
    enc = enc[["encounter_id", "patient_id", "provider", "organization", "place_of_service"]]

    # Claims join: claim has encounterid, patientid, providerid, primarypatientinsuranceid (payer)
    cls = cls.rename(columns={
        "id": "claim_id",
        "patientid": "patient_id",
        "providerid": "provider_id",
        "primarypatientinsuranceid": "payer_id",
        "encounterid": "encounter_id",
        "servicedate": "service_date",
    })

    prov = prov.rename(columns={"id": "provider_id", "organization": "org_id"})[
        ["provider_id", "name", "org_id"]
    ].rename(columns={"name": "provider_name"})

    payers = payers.rename(columns={"id": "payer_id", "name": "payer_name"})[
        ["payer_id", "payer_name"]
    ]

    df = (cls
          .merge(pts, on="patient_id", how="left")
          .merge(enc, on=["encounter_id", "patient_id"], how="left")
          .merge(prov, on="provider_id", how="left")
          .merge(payers, on="payer_id", how="left"))
    df["place_of_service"] = df["place_of_service"].fillna("11")
    return df


def to_837(row: pd.Series, icn: int) -> Claim837:
    total = Decimal(str(row.get("total_claim_cost") or row.get("totalclaimcost") or "100.00"))
    return Claim837(
        claim_id=str(row["claim_id"])[:18],
        submitter_id="SUBMTR-001",
        submitter_name="Acme Clearinghouse",
        receiver_id=str(row.get("payer_id") or "PAYER001")[:14],
        receiver_name=(row.get("payer_name") or "Generic Payer")[:60],
        subscriber_id=str(row["patient_id"])[:30],
        subscriber_first=str(row.get("first") or "X")[:25],
        subscriber_last=str(row.get("last") or "Patient")[:25],
        subscriber_dob=_to_date(row.get("birthdate")),
        billing_provider_npi=str(row.get("org_id") or "9999999999")[:10],
        billing_provider_name=(row.get("provider_name") or "Provider")[:60],
        rendering_provider_npi=str(row.get("provider_id") or "9999999999")[:10],
        diagnosis_code="Z00.00",  # generic preventive — Synthea conditions could be joined here
        place_of_service=str(row.get("place_of_service") or "11"),
        service_date=_to_date(row.get("service_date") or row.get("servicedate")),
        total_charge=total,
        lines=[ServiceLine(
            procedure_code="99213",
            charge_amount=total,
            units=1,
            service_date=_to_date(row.get("service_date") or row.get("servicedate")),
        )],
        interchange_control_number=icn,
    )


def main(limit: int | None = None) -> None:
    EDI_OUT.mkdir(parents=True, exist_ok=True)
    df = build_claims_df()
    if limit:
        df = df.head(limit)
    print(f"writing {len(df):,} 837 files to {EDI_OUT}")

    manifest_path = EDI_OUT / "manifest.jsonl"
    rng = random.Random(1729)
    with manifest_path.open("w") as mf:
        for icn, (_, row) in enumerate(df.iterrows(), start=1):
            claim = to_837(row, icn)
            # 5% of claims simulate a network jitter to test late-arrival paths
            if rng.random() < 0.05:
                claim.service_date = pd.Timestamp(claim.service_date) - pd.Timedelta(days=rng.randint(1, 14))
                claim.service_date = claim.service_date.date()
            edi_text = serialize(claim)
            edi_path = EDI_OUT / f"{claim.claim_id}.edi"
            edi_path.write_text(edi_text)
            mf.write(json.dumps({
                "claim_id": claim.claim_id,
                "edi_path": str(edi_path),
                "service_date": claim.service_date.isoformat(),
                "subscriber_id": claim.subscriber_id,
                "total_charge": str(claim.total_charge),
                "receiver_id": claim.receiver_id,
            }) + "\n")
    print(f"done — manifest at {manifest_path}")


if __name__ == "__main__":
    main()
