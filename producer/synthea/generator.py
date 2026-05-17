"""Synthea-compatible synthetic generator (Python, no Java needed).

The official Synthea Docker image is `linux/amd64` only and crashes on arm64
macOS — see ADR-0003. This module produces CSVs with the SAME column names
and semantics as Synthea so `producer.synthea.reader` and downstream stages
don't know the difference.

Tables emitted:
    patients.csv         (one row per patient)
    organizations.csv    (synthetic hospitals + clinics)
    providers.csv        (one per organization, simple)
    payers.csv           (handful of commercial + government payers)
    encounters.csv       (encounters per patient)
    claims.csv           (1:1 with encounters)

Determinism: same seed → identical output.
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Iterator


STATES = ["MA", "CA", "TX", "NY", "WA", "IL", "FL", "PA", "OH", "GA"]
FIRST_NAMES_F = ["Emma", "Olivia", "Ava", "Sophia", "Isabella", "Mia", "Charlotte", "Amelia"]
FIRST_NAMES_M = ["Liam", "Noah", "Oliver", "Elijah", "James", "William", "Benjamin", "Lucas"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson"]
ENCOUNTER_CLASSES = [
    ("ambulatory", 0.55),
    ("outpatient", 0.15),
    ("wellness", 0.12),
    ("urgentcare", 0.08),
    ("emergency", 0.06),
    ("inpatient", 0.04),
]
PAYER_NAMES = [
    "Aetna Health", "Blue Cross Blue Shield", "United Healthcare", "Cigna",
    "Humana", "Anthem", "Kaiser Permanente", "Centene", "Medicare", "Medicaid",
]
ORG_PREFIXES = ["General Hospital", "Medical Center", "Family Clinic",
                "Specialty Care", "Community Health"]


def _weighted_choice(rng: random.Random, choices: list[tuple[str, float]]) -> str:
    vals, weights = zip(*choices)
    return rng.choices(vals, weights=weights)[0]


def _uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128)))


def generate_population(population: int, horizon_years: int, seed: int, out_dir: Path) -> dict[str, int]:
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- payers ---
    payers: list[dict] = []
    for name in PAYER_NAMES:
        payers.append({
            "Id": _uuid(rng),
            "NAME": name,
            "ADDRESS": f"{rng.randint(100,9999)} Main St",
            "CITY": rng.choice(["Boston", "Austin", "Seattle", "Chicago", "Atlanta"]),
            "STATE_HEADQUARTERED": rng.choice(STATES),
            "ZIP": f"{rng.randint(10000,99999):05d}",
            "PHONE": f"800-555-{rng.randint(1000,9999):04d}",
            "AMOUNT_COVERED": "",
            "AMOUNT_UNCOVERED": "",
            "REVENUE": "",
            "COVERED_ENCOUNTERS": "",
            "UNCOVERED_ENCOUNTERS": "",
            "COVERED_MEDICATIONS": "",
            "UNCOVERED_MEDICATIONS": "",
            "COVERED_PROCEDURES": "",
            "UNCOVERED_PROCEDURES": "",
            "COVERED_IMMUNIZATIONS": "",
            "UNCOVERED_IMMUNIZATIONS": "",
            "UNIQUE_CUSTOMERS": "",
            "QOLS_AVG": "",
            "MEMBER_MONTHS": "",
        })

    # --- organizations + providers ---
    num_orgs = max(20, population // 200)  # 1 org per ~200 patients
    orgs: list[dict] = []
    providers: list[dict] = []
    for _ in range(num_orgs):
        oid = _uuid(rng)
        state = rng.choice(STATES)
        org_name = f"{rng.choice(['Mercy','Hope','Valley','Riverside','Hillside','Central','Eastside'])} {rng.choice(ORG_PREFIXES)}"
        orgs.append({
            "Id": oid,
            "NAME": org_name,
            "ADDRESS": f"{rng.randint(100,9999)} Hospital Way",
            "CITY": rng.choice(["Boston", "Austin", "Seattle", "Chicago", "Atlanta"]),
            "STATE": state,
            "ZIP": f"{rng.randint(10000,99999):05d}",
            "LAT": round(rng.uniform(25, 49), 4),
            "LON": round(rng.uniform(-124, -67), 4),
            "PHONE": f"555-{rng.randint(1000,9999):04d}",
            "REVENUE": "",
            "UTILIZATION": "",
        })
        # 1-3 providers per org
        for _ in range(rng.randint(1, 3)):
            providers.append({
                "Id": _uuid(rng),
                "ORGANIZATION": oid,
                "NAME": f"Dr. {rng.choice(LAST_NAMES)}",
                "GENDER": rng.choice(["M", "F"]),
                "SPECIALITY": rng.choice(["GENERAL_PRACTICE","INTERNAL_MEDICINE","CARDIOLOGY","PEDIATRICS","FAMILY_MEDICINE"]),
                "ADDRESS": "",
                "CITY": "",
                "STATE": "",
                "ZIP": "",
                "LAT": "",
                "LON": "",
                "ENCOUNTERS": "",
                "PROCEDURES": "",
            })

    # --- patients ---
    horizon_end = datetime.now().date()
    horizon_start_dt = datetime.combine(
        date(horizon_end.year - horizon_years, horizon_end.month, horizon_end.day),
        datetime.min.time(),
    )
    patients: list[dict] = []
    for i in range(population):
        pid = _uuid(rng)
        gender = rng.choice(["M", "F"])
        first = rng.choice(FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F)
        last = rng.choice(LAST_NAMES)
        birth = date(rng.randint(1935, 2024), rng.randint(1, 12), rng.randint(1, 28))
        patients.append({
            "Id": pid,
            "BIRTHDATE": birth.isoformat(),
            "DEATHDATE": "",
            "SSN": f"999-{rng.randint(10,99)}-{rng.randint(1000,9999):04d}",
            "DRIVERS": "",
            "PASSPORT": "",
            "PREFIX": "Mr." if gender == "M" else "Ms.",
            "FIRST": first,
            "LAST": last,
            "SUFFIX": "",
            "MAIDEN": "",
            "MARITAL": rng.choice(["S", "M", "D", "W"]),
            "RACE": rng.choice(["white","black","asian","hispanic","other"]),
            "ETHNICITY": rng.choice(["hispanic","nonhispanic"]),
            "GENDER": gender,
            "BIRTHPLACE": "",
            "ADDRESS": f"{rng.randint(100,9999)} {rng.choice(['Oak','Elm','Maple','Pine'])} St",
            "CITY": rng.choice(["Boston","Austin","Seattle","Chicago","Atlanta"]),
            "STATE": rng.choice(STATES),
            "COUNTY": "",
            "ZIP": f"{rng.randint(10000,99999):05d}",
            "LAT": round(rng.uniform(25, 49), 4),
            "LON": round(rng.uniform(-124, -67), 4),
            "HEALTHCARE_EXPENSES": "",
            "HEALTHCARE_COVERAGE": "",
            "INCOME": "",
        })

    # --- encounters + claims ---
    encounters: list[dict] = []
    claims: list[dict] = []
    for p in patients:
        # encounter cadence ~ Poisson with mean 3 per year per patient
        per_year = rng.gammavariate(3.0, 1.0)
        total_enc = max(1, int(per_year * horizon_years))
        for _ in range(total_enc):
            org = rng.choice(orgs)
            provider = rng.choice([pr for pr in providers if pr["ORGANIZATION"] == org["Id"]] or providers)
            payer = rng.choice(payers)
            ec = _weighted_choice(rng, ENCOUNTER_CLASSES)
            start = horizon_start_dt + timedelta(days=rng.randint(0, horizon_years * 365 - 1),
                                                 hours=rng.randint(7, 18))
            stop = start + timedelta(minutes=rng.randint(15, 240))
            eid = _uuid(rng)
            total_cost = round(rng.lognormvariate(4.7, 0.8), 2)  # ~$110 typical
            encounters.append({
                "Id": eid,
                "START": start.isoformat(),
                "STOP": stop.isoformat(),
                "PATIENT": p["Id"],
                "ORGANIZATION": org["Id"],
                "PROVIDER": provider["Id"],
                "PAYER": payer["Id"],
                "ENCOUNTERCLASS": ec,
                "CODE": "185349003",
                "DESCRIPTION": "Encounter for check up",
                "BASE_ENCOUNTER_COST": "85.00",
                "TOTAL_CLAIM_COST": f"{total_cost:.2f}",
                "PAYER_COVERAGE": f"{round(total_cost * 0.8, 2):.2f}",
                "REASONCODE": "",
                "REASONDESCRIPTION": "",
            })
            claims.append({
                "Id": _uuid(rng),
                "PATIENTID": p["Id"],
                "PROVIDERID": provider["Id"],
                "PRIMARYPATIENTINSURANCEID": payer["Id"],
                "SECONDARYPATIENTINSURANCEID": "",
                "DEPARTMENTID": "1",
                "PATIENTDEPARTMENTID": "1",
                "DIAGNOSIS1": "Z00.00",
                "DIAGNOSIS2": "",
                "DIAGNOSIS3": "",
                "DIAGNOSIS4": "",
                "DIAGNOSIS5": "",
                "DIAGNOSIS6": "",
                "DIAGNOSIS7": "",
                "DIAGNOSIS8": "",
                "REFERRINGPROVIDERID": "",
                "APPOINTMENTID": "",
                "CURRENTILLNESSDATE": start.date().isoformat(),
                "SERVICEDATE": start.date().isoformat(),
                "SUPERVISINGPROVIDERID": "",
                "STATUS1": "BILLED",
                "STATUS2": "",
                "STATUSP": "",
                "OUTSTANDING1": "0",
                "OUTSTANDING2": "0",
                "OUTSTANDINGP": "0",
                "LASTBILLEDDATE1": start.date().isoformat(),
                "LASTBILLEDDATE2": "",
                "LASTBILLEDDATEP": "",
                "HEALTHCARECLAIMTYPEID1": "1",
                "HEALTHCARECLAIMTYPEID2": "",
                "ENCOUNTERID": eid,
                "TOTAL_CLAIM_COST": f"{total_cost:.2f}",
            })

    # --- write everything ---
    def write_csv(name: str, rows: list[dict]) -> None:
        if not rows:
            return
        path = out_dir / f"{name}.csv"
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    write_csv("patients", patients)
    write_csv("organizations", orgs)
    write_csv("providers", providers)
    write_csv("payers", payers)
    write_csv("encounters", encounters)
    write_csv("claims", claims)

    return {
        "patients": len(patients),
        "organizations": len(orgs),
        "providers": len(providers),
        "payers": len(payers),
        "encounters": len(encounters),
        "claims": len(claims),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--population", type=int,
                        default=int(os.getenv("SYNTHEA_POPULATION", "10000")))
    parser.add_argument("--years", type=int,
                        default=int(os.getenv("SYNTHEA_HORIZON_YEARS", "2")))
    parser.add_argument("-s", "--seed", type=int,
                        default=int(os.getenv("SYNTHEA_SEED", "20260516")))
    parser.add_argument("--out", default=os.getenv("SYNTHEA_OUTPUT_DIR", "data/synthea_output/csv"))
    args = parser.parse_args()

    out = Path(args.out)
    print(f"generating synthetic population: n={args.population:,} years={args.years} seed={args.seed}")
    counts = generate_population(args.population, args.years, args.seed, out)
    for k, v in counts.items():
        print(f"  {k:<14} {v:>10,}")
    print(f"\nwrote → {out}")


if __name__ == "__main__":
    main()
