"""Read the EDI 837 manifest and publish each claim to Kafka.

Each Kafka message is JSON-encoded so we can avoid the Avro toolchain here:
the EDI body itself is a binary-but-text payload, and ergonomically JSON-wrapping
keeps the bronze consumer simple. The schema-evolution story would be Avro in
production, but for a portfolio demo readable JSON wins.

Message shape:
  {
    "claim_id": "...",
    "subscriber_id": "...",
    "receiver_id": "...",
    "service_date": "YYYY-MM-DD",
    "total_charge": "100.00",
    "edi_837":     "<raw segment-terminated text>",
    "ingest_ts":   "ISO-8601 UTC"
  }
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, start_http_server


load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("TOPIC_CLAIM", "claims.edi837.v1")
EPS = int(os.getenv("EVENTS_PER_SECOND", "1000"))
EDI_OUT = Path(os.getenv("EDI_OUTPUT_DIR", "data/edi_output"))
METRICS_PORT = int(os.getenv("PRODUCER_METRICS_PORT", "9301"))


events_total = Counter("claims_producer_events_total", "Claims published to Kafka")
events_dropped = Counter("claims_producer_events_dropped_total", "Claims dropped on backpressure")
eps_gauge = Gauge("claims_producer_eps", "Sustained events per second")


def _on_delivery(err, msg):
    if err is not None:
        events_dropped.inc()


def main() -> None:
    manifest_path = EDI_OUT / "manifest.jsonl"
    if not manifest_path.exists():
        print(f"ERROR: {manifest_path} not found — run `make edi` first", file=sys.stderr)
        sys.exit(1)
    print(f"[producer] bootstrap={BOOTSTRAP} topic={TOPIC} eps={EPS}")
    start_http_server(METRICS_PORT)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 10,
        "batch.size": 64 * 1024,
        "compression.type": "lz4",
        "acks": "1",
        "queue.buffering.max.messages": 100_000,
    })

    stop = False
    def _sigint(_signum, _frame):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    slice_s = 0.05
    per_slice = max(1, int(EPS * slice_s))
    next_tick = time.perf_counter()
    sent_last_sec = 0
    sec_anchor = time.perf_counter()
    total_sent = 0

    with manifest_path.open() as mf:
        rows = (json.loads(line) for line in mf)
        emit_buffer: list[dict] = []
        for row in rows:
            if stop:
                break
            edi_text = Path(row["edi_path"]).read_text()
            payload = {
                "claim_id": row["claim_id"],
                "subscriber_id": row["subscriber_id"],
                "receiver_id": row["receiver_id"],
                "service_date": row["service_date"],
                "total_charge": row["total_charge"],
                "edi_837": edi_text,
                "ingest_ts": datetime.now(timezone.utc).isoformat(),
            }
            emit_buffer.append(payload)

            if len(emit_buffer) >= per_slice:
                for ev in emit_buffer:
                    try:
                        producer.produce(
                            topic=TOPIC,
                            key=ev["subscriber_id"].encode(),
                            value=json.dumps(ev).encode(),
                            on_delivery=_on_delivery,
                        )
                        events_total.inc()
                        sent_last_sec += 1
                        total_sent += 1
                    except BufferError:
                        events_dropped.inc()
                        producer.poll(0.001)
                emit_buffer.clear()
                producer.poll(0)

                now = time.perf_counter()
                if now - sec_anchor >= 1.0:
                    eps_gauge.set(sent_last_sec / (now - sec_anchor))
                    sec_anchor = now
                    sent_last_sec = 0

                next_tick += slice_s
                sleep_for = next_tick - time.perf_counter()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                else:
                    next_tick = time.perf_counter()

        # Final flush of any remainder
        for ev in emit_buffer:
            try:
                producer.produce(
                    topic=TOPIC,
                    key=ev["subscriber_id"].encode(),
                    value=json.dumps(ev).encode(),
                )
                events_total.inc()
                total_sent += 1
            except BufferError:
                events_dropped.inc()

    print(f"[producer] flushing {total_sent:,} claims...", file=sys.stderr)
    producer.flush(15.0)
    print(f"[producer] done — {total_sent:,} claims published")


if __name__ == "__main__":
    main()
