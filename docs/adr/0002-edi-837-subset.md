# ADR-0002: Hand-rolled EDI 837 generator with a minimal segment set

**Status:** Accepted · 2026-05-16

## Context

Synthea exports claims as CSV. Real healthcare clearinghouses exchange EDI
X12 837 (~Health Care Claim) files — a positional, segment-terminated text
format with a thousand-segment spec across the 837P/I/D variants. The portfolio
narrative talks about "EDI 837 claims feed" — we need the wire format to look
real, but the full 837P implementation is months of work.

## Options

1. **Full Java parser** (e.g. SmooksEDI, edi-fact-x12) — accurate, slow to
   integrate, JVM dependency, overkill for a demo.
2. **`edi-x12` PyPI library** — incomplete coverage of 837P, last released
   2022, would still need extension.
3. **Hand-rolled 837P subset in `producer/edi/x12_837.py`** — implement the
   segments that actually flow in the demo (ISA, GS, ST, BHT, NM1×n, HL×2,
   CLM, DTP, HI, SV1, SE, GE, IEA). Document what's omitted. Round-trip
   tested.

## Decision

**Option 3.** ~200 LOC, write + parse in one module, single round-trip test
gates regressions. The pipeline downstream of Kafka neither knows nor cares
that we're not the full spec.

## Consequences

- **Realistic enough** — the wire payload in Redpanda Console is a true 837
  envelope. Recruiters who know the format will recognize ISA/GS/CLM/SV1.
- **Not production-ready** — no support for adjustments, COB, secondary
  payer, attachments, or 999 acknowledgments. Documented in the module
  docstring so this is clear.
- **Round-trip enforces compatibility** — `tests/test_edi_roundtrip.py`
  guarantees `parse(serialize(claim)) == claim` for the fields we round-trip.
  If we ever extend the writer, the test catches asymmetric changes to the
  parser.
