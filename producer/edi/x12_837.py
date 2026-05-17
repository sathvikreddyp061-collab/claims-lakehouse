"""EDI X12 837 (Health Care Claim) — minimal generator + parser.

Real EDI 837 is a thousand-segment spec. We implement the **professional 837P**
subset that's enough to demo realistic claims processing:

  ISA  Interchange Control Header
  GS   Functional Group Header
  ST   Transaction Set Header
  BHT  Beginning of Hierarchical Transaction
  NM1  Submitter / receiver / billing provider / subscriber
  HL   Hierarchical levels (Billing Provider → Subscriber → Patient)
  CLM  Claim information (claim id, total charges, place-of-service)
  DTP  Date qualifier (service date)
  SV1  Service line (procedure code, charge, units)
  SE/GE/IEA  Trailers

Each segment ends with `~`, elements separated by `*`. The format is a flat
text file; one file can carry many claims under one ST/SE pair.

This module exposes:
  - `Claim837` dataclass (Pydantic) — the typed in-memory representation
  - `build_segments(claim)` → list[str] — the EDI segments
  - `serialize(claim)` → str  — final EDI 837 file content (single claim)
  - `parse(content)` → list[Claim837] — inverse, for the bronze consumer

Round-trip tested in `tests/test_edi_roundtrip.py`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from decimal import Decimal


SEG_TERM = "~"
ELEM_SEP = "*"


@dataclass(slots=True)
class ServiceLine:
    procedure_code: str       # CPT/HCPCS
    charge_amount: Decimal
    units: int = 1
    service_date: date | None = None  # if None, falls back to the claim DOS


@dataclass(slots=True)
class Claim837:
    claim_id: str
    submitter_id: str                # Billing provider TIN/NPI
    submitter_name: str
    receiver_id: str                 # Payer ID
    receiver_name: str
    subscriber_id: str               # Member ID
    subscriber_first: str
    subscriber_last: str
    subscriber_dob: date
    billing_provider_npi: str
    billing_provider_name: str
    rendering_provider_npi: str
    diagnosis_code: str              # ICD-10
    place_of_service: str            # POS code, e.g. "11" office
    service_date: date
    total_charge: Decimal
    lines: list[ServiceLine] = field(default_factory=list)
    interchange_control_number: int = 1


# ---------- write ----------

def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _hhmm(t: datetime) -> str:
    return t.strftime("%H%M")


def _amt(d: Decimal) -> str:
    # X12 amounts: no leading zero before decimal, max 2 decimal places, no commas
    return f"{Decimal(d):.2f}"


def build_segments(c: Claim837, now: datetime | None = None) -> list[str]:
    now = now or datetime.now(timezone.utc)
    segs: list[str] = []
    icn = f"{c.interchange_control_number:09d}"

    # Envelope: ISA + GS
    segs.append(ELEM_SEP.join([
        "ISA", "00", "          ", "00", "          ",
        "ZZ", c.submitter_id.ljust(15)[:15],
        "ZZ", c.receiver_id.ljust(15)[:15],
        _yyyymmdd(now.date())[2:],   # YYMMDD
        _hhmm(now),
        "^", "00501", icn,
        "0", "P", ":",
    ]))
    segs.append(ELEM_SEP.join([
        "GS", "HC", c.submitter_id, c.receiver_id,
        _yyyymmdd(now.date()), _hhmm(now), str(c.interchange_control_number),
        "X", "005010X222A1",
    ]))

    # Transaction set
    segs.append(ELEM_SEP.join(["ST", "837", "0001", "005010X222A1"]))
    segs.append(ELEM_SEP.join([
        "BHT", "0019", "00", c.claim_id,
        _yyyymmdd(now.date()), _hhmm(now), "CH",
    ]))

    # Submitter
    segs.append(ELEM_SEP.join(["NM1", "41", "2", c.submitter_name, "", "", "", "", "46", c.submitter_id]))
    segs.append(ELEM_SEP.join(["PER", "IC", c.submitter_name, "TE", "8005550100"]))
    # Receiver
    segs.append(ELEM_SEP.join(["NM1", "40", "2", c.receiver_name, "", "", "", "", "46", c.receiver_id]))

    # HL 1 — Billing provider
    segs.append(ELEM_SEP.join(["HL", "1", "", "20", "1"]))
    segs.append(ELEM_SEP.join(["NM1", "85", "2", c.billing_provider_name, "", "", "", "", "XX", c.billing_provider_npi]))

    # HL 2 — Subscriber (we treat subscriber == patient for the demo)
    segs.append(ELEM_SEP.join(["HL", "2", "1", "22", "0"]))
    segs.append(ELEM_SEP.join(["SBR", "P", "18", "", "", "", "", "", "", "CI"]))
    segs.append(ELEM_SEP.join([
        "NM1", "IL", "1", c.subscriber_last, c.subscriber_first, "", "", "", "MI", c.subscriber_id
    ]))
    segs.append(ELEM_SEP.join(["DMG", "D8", _yyyymmdd(c.subscriber_dob), "U"]))

    # Claim
    segs.append(ELEM_SEP.join([
        "CLM", c.claim_id, _amt(c.total_charge), "", "",
        f"{c.place_of_service}:B:1", "Y", "A", "Y", "I",
    ]))
    segs.append(ELEM_SEP.join(["DTP", "472", "D8", _yyyymmdd(c.service_date)]))
    segs.append(ELEM_SEP.join(["HI", f"ABK:{c.diagnosis_code}"]))
    segs.append(ELEM_SEP.join([
        "NM1", "82", "1", "", "", "", "", "", "XX", c.rendering_provider_npi
    ]))

    # Service lines
    for idx, line in enumerate(c.lines, start=1):
        segs.append(ELEM_SEP.join([
            "LX", str(idx)
        ]))
        segs.append(ELEM_SEP.join([
            "SV1", f"HC:{line.procedure_code}", _amt(line.charge_amount),
            "UN", str(line.units), c.place_of_service, "", "1",
        ]))
        segs.append(ELEM_SEP.join([
            "DTP", "472", "D8", _yyyymmdd(line.service_date or c.service_date)
        ]))

    # Trailers
    se_count = len([s for s in segs if not s.startswith(("ISA", "GS"))]) + 1  # +1 for SE itself
    segs.append(ELEM_SEP.join(["SE", str(se_count), "0001"]))
    segs.append(ELEM_SEP.join(["GE", "1", str(c.interchange_control_number)]))
    segs.append(ELEM_SEP.join(["IEA", "1", icn]))

    return segs


def serialize(c: Claim837, now: datetime | None = None) -> str:
    return SEG_TERM.join(build_segments(c, now)) + SEG_TERM + "\n"


# ---------- read ----------

_SEG_RE = re.compile(r"([A-Z]{1,3})\*([^~]*)~", re.MULTILINE)


def parse(content: str) -> list[Claim837]:
    """Parse one or more 837 envelopes from `content`. Returns a Claim837 per
    CLM segment. Tolerant of multi-line / single-line input."""
    # Normalize line breaks → segments are ~-terminated regardless.
    flat = content.replace("\n", "").replace("\r", "")
    segments = [s for s in flat.split(SEG_TERM) if s.strip()]

    claims: list[Claim837] = []
    submitter_id = submitter_name = receiver_id = receiver_name = ""
    billing_npi = billing_name = subscriber_id = ""
    subscriber_first = subscriber_last = ""
    subscriber_dob: date | None = None
    rendering_npi = diagnosis = ""
    icn = 1

    current: Claim837 | None = None

    for raw in segments:
        elems = raw.split(ELEM_SEP)
        tag = elems[0]

        if tag == "ISA":
            icn = int(elems[13]) if len(elems) > 13 and elems[13].isdigit() else 1
        elif tag == "NM1" and len(elems) > 1:
            entity = elems[1]
            if entity == "41":   # submitter
                submitter_name = elems[3] if len(elems) > 3 else ""
                submitter_id = elems[9] if len(elems) > 9 else ""
            elif entity == "40":  # receiver
                receiver_name = elems[3] if len(elems) > 3 else ""
                receiver_id = elems[9] if len(elems) > 9 else ""
            elif entity == "85":  # billing provider
                billing_name = elems[3] if len(elems) > 3 else ""
                billing_npi = elems[9] if len(elems) > 9 else ""
            elif entity == "IL":  # subscriber
                subscriber_last = elems[3] if len(elems) > 3 else ""
                subscriber_first = elems[4] if len(elems) > 4 else ""
                subscriber_id = elems[9] if len(elems) > 9 else ""
            elif entity == "82":  # rendering provider
                rendering_npi = elems[9] if len(elems) > 9 else ""
                # Like HI, NM1*82* lives after CLM in 837P — patch in-flight.
                if current is not None:
                    current.rendering_provider_npi = rendering_npi
        elif tag == "DMG" and len(elems) > 2 and elems[1] == "D8":
            subscriber_dob = datetime.strptime(elems[2], "%Y%m%d").date()
        elif tag == "HI" and len(elems) > 1:
            # ABK:I10code → split on ":"
            parts = elems[1].split(":")
            if len(parts) == 2:
                diagnosis = parts[1]
                # In a typical 837P, HI comes AFTER CLM. Update the in-flight
                # claim directly so round-trip is preserved.
                if current is not None:
                    current.diagnosis_code = diagnosis
        elif tag == "CLM" and len(elems) > 2:
            pos = "11"
            if len(elems) > 5:
                pos = elems[5].split(":")[0]
            current = Claim837(
                claim_id=elems[1],
                submitter_id=submitter_id,
                submitter_name=submitter_name,
                receiver_id=receiver_id,
                receiver_name=receiver_name,
                subscriber_id=subscriber_id,
                subscriber_first=subscriber_first,
                subscriber_last=subscriber_last,
                subscriber_dob=subscriber_dob or date.today(),
                billing_provider_npi=billing_npi,
                billing_provider_name=billing_name,
                rendering_provider_npi=rendering_npi,
                diagnosis_code=diagnosis,
                place_of_service=pos,
                service_date=date.today(),
                total_charge=Decimal(elems[2]),
                interchange_control_number=icn,
            )
        elif tag == "DTP" and current is not None and len(elems) > 3 and elems[1] == "472":
            current.service_date = datetime.strptime(elems[3], "%Y%m%d").date()
        elif tag == "SV1" and current is not None and len(elems) > 5:
            proc = elems[1].split(":")[-1]
            current.lines.append(ServiceLine(
                procedure_code=proc,
                charge_amount=Decimal(elems[2]),
                units=int(elems[4]) if len(elems) > 4 and elems[4].isdigit() else 1,
                service_date=current.service_date,
            ))
        elif tag == "SE" and current is not None:
            claims.append(current)
            current = None

    return claims
