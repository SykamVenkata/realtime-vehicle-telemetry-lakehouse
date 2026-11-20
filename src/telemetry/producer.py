"""Kafka sink for the generator.

Works with a local Kafka broker and with Azure Event Hubs through its Kafka
endpoint. Configuration comes from environment variables so no secret ever
lands in the repo:

- ``KAFKA_BOOTSTRAP_SERVERS``          e.g. ``localhost:9092`` (local Docker)
- ``EVENTHUBS_CONNECTION_STRING``      if set, connects to Event Hubs over
                                       SASL_SSL on port 9093 instead
"""

from __future__ import annotations

import json
import os
import re
import sys


def kafka_config() -> dict:
    conn = os.getenv("EVENTHUBS_CONNECTION_STRING")
    if conn:
        match = re.search(r"Endpoint=sb://([^/;]+)", conn)
        if not match:
            raise ValueError("EVENTHUBS_CONNECTION_STRING has no Endpoint=sb://... part")
        return {
            "bootstrap.servers": f"{match.group(1)}:9093",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": conn,
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    return {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "linger.ms": 50,
        "compression.type": "lz4",
    }


def kafka_sink(topic: str):
    try:
        from confluent_kafka import Producer
    except ImportError as exc:  # pragma: no cover - depends on optional install
        raise SystemExit("Install the producer extra: pip install -e '.[producer]'") from exc

    producer = Producer(kafka_config())
    errors = {"count": 0}

    def on_delivery(err, _msg) -> None:
        if err is not None:
            errors["count"] += 1
            if errors["count"] <= 5:
                print(f"[producer] delivery failed: {err}", file=sys.stderr)

    def send(event: dict) -> None:
        # Keying by vehicle keeps each vehicle's events in order within a partition.
        key = (event.get("vehicle_id") or "unknown").encode()
        while True:
            try:
                producer.produce(topic, key=key, value=json.dumps(event).encode(), on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.1)  # local queue full: let it drain, then retry
        producer.poll(0)

    def close() -> None:
        producer.flush(30)
        if errors["count"]:
            print(f"[producer] {errors['count']} events failed delivery", file=sys.stderr)

    return send, close
