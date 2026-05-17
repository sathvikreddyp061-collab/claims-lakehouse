"""EDI 837 generator/parser round-trip — the contract test backing ADR-0002.

If the writer or parser ever drift apart on the fields we promise to round-trip
(claim_id, subscriber_id, total_charge, service_date, diagnosis_code, …), this
test catches it before the producer + bronze pipeline silently lose data.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from producer.edi.x12_837 import Claim837, ServiceLine, parse, serialize


def _sample_claim() -> Claim837:
    return Claim837(
        claim_id="CLM-0001",
        submitter_id="SUBMTR-001",
        submitter_name="Acme Clearinghouse",
        receiver_id="PAYER001",
        receiver_name="BCBS",
        subscriber_id="MEM-12345",
        subscriber_first="Jane",
        subscriber_last="Doe",
        subscriber_dob=date(1985, 4, 12),
        billing_provider_npi="1234567890",
        billing_provider_name="Acme Clinic",
        rendering_provider_npi="9876543210",
        diagnosis_code="Z00.00",
        place_of_service="11",
        service_date=date(2026, 5, 1),
        total_charge=Decimal("182.50"),
        lines=[
            ServiceLine(procedure_code="99213", charge_amount=Decimal("182.50"), units=1,
                        service_date=date(2026, 5, 1)),
        ],
        interchange_control_number=42,
    )


def test_serialize_includes_required_segments():
    text = serialize(_sample_claim())
    for tag in ("ISA*", "GS*", "ST*837", "BHT*", "NM1*41*", "NM1*40*", "NM1*85*",
                "NM1*IL*", "CLM*CLM-0001*", "DTP*472*", "HI*ABK:Z00.00",
                "SV1*HC:99213*", "SE*", "GE*", "IEA*"):
        assert tag in text, f"missing segment {tag}"


def test_roundtrip_preserves_core_fields():
    original = _sample_claim()
    text = serialize(original)
    parsed = parse(text)
    assert len(parsed) == 1
    p = parsed[0]
    assert p.claim_id == original.claim_id
    assert p.subscriber_id == original.subscriber_id
    assert p.subscriber_first == original.subscriber_first
    assert p.subscriber_last == original.subscriber_last
    assert p.subscriber_dob == original.subscriber_dob
    assert p.total_charge == original.total_charge
    assert p.service_date == original.service_date
    assert p.diagnosis_code == original.diagnosis_code
    assert p.place_of_service == original.place_of_service
    assert p.billing_provider_npi == original.billing_provider_npi
    assert p.rendering_provider_npi == original.rendering_provider_npi
    assert len(p.lines) == 1
    assert p.lines[0].procedure_code == "99213"
    assert p.lines[0].charge_amount == Decimal("182.50")


def test_parse_handles_multiple_claims_in_one_envelope():
    # Two ST/SE pairs in one IEA — uncommon but legal
    a = serialize(_sample_claim())
    b = _sample_claim()
    b.claim_id = "CLM-0002"
    b.total_charge = Decimal("75.00")
    b.lines[0].charge_amount = Decimal("75.00")
    text = a + serialize(b)
    parsed = parse(text)
    ids = sorted(c.claim_id for c in parsed)
    assert ids == ["CLM-0001", "CLM-0002"]


@pytest.mark.parametrize("amt", [Decimal("0.01"), Decimal("9999999.99"), Decimal("100.00")])
def test_amount_precision_is_preserved(amt):
    c = _sample_claim()
    c.total_charge = amt
    c.lines[0].charge_amount = amt
    parsed = parse(serialize(c))[0]
    assert parsed.total_charge == amt
    assert parsed.lines[0].charge_amount == amt
