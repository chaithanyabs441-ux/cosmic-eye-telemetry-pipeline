"""Publish synthetic live events to the persistent Phase 6 Redpanda topic."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
sys.path.insert(0, str(ROOT / "pipeline"))
from ingest import build_events  # noqa: E402


def main() -> int:
    from confluent_kafka import KafkaError, KafkaException, Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default=os.getenv("KAFKA_LIVE_TOPIC", "cosmic-eye-live"))
    parser.add_argument("--rate", type=float, default=2.0, help="synthetic events per second")
    parser.add_argument("--count", type=int, default=0, help="events to send; 0 runs until Ctrl+C")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--inject-anomaly-every", type=int, default=10,
                        help="raise a synthetic exposure anomaly every N exposure events; 0 disables")
    parser.add_argument("--create-topic-only", action="store_true",
                        help="create the topic and exit without publishing events")
    args = parser.parse_args()
    if args.rate <= 0 or args.count < 0 or args.inject_anomaly_every < 0:
        parser.error("rate must be positive; count and anomaly interval cannot be negative")

    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
    topic = args.topic.strip()
    admin = AdminClient({"bootstrap.servers": brokers})
    create = admin.create_topics([
        NewTopic(
            topic,
            num_partitions=1,
            replication_factor=1,
            config={"retention.ms": "86400000", "retention.bytes": "104857600"},
        )
    ])
    try:
        create[topic].result(timeout=20)
    except KafkaException as exc:
        error = exc.args[0] if exc.args else None
        if not isinstance(error, KafkaError) or error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    if args.create_topic_only:
        print(f"Topic ready: {topic}")
        return 0

    producer = Producer({"bootstrap.servers": brokers, "client.id": "cosmic-eye-live-generator"})
    event_id_prefix = uuid.uuid4().hex
    seed = args.seed
    events = build_events(
        count=0,
        seed=seed,
        source_mode="synthetic",
        dirty_every=0,
        live=True,
        stop_after=None,
    )

    produced = exposure_number = anomaly_count = 0
    next_emit = time.monotonic()
    print(f"Publishing clearly synthetic events to {topic} at {args.rate:g}/second. Ctrl+C stops.")
    try:
        for payload in events:
            payload["event_id"] = f"{event_id_prefix}-{produced:09d}"
            if payload.get("event_type") == "exposure":
                exposure_number += 1
                if args.inject_anomaly_every and exposure_number % args.inject_anomaly_every == 0:
                    metrics = payload.setdefault("quality_metrics", {})
                    metrics["noise_sigma"] = 0.79
                    metrics["saturated_pixel_fraction"] = 0.35
                    payload["anomaly_injected"] = True
                    payload.setdefault("dirty_flags", []).append("demo_quality_anomaly")
                    anomaly_count += 1

            encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            producer.produce(topic, key=payload["event_id"].encode("utf-8"), value=encoded)
            producer.poll(0)
            produced += 1
            if produced % 20 == 0:
                producer.flush(10)
                print(f"Published={produced}; injected anomalies={anomaly_count}")
            if args.count and produced >= args.count:
                break

            next_emit += 1.0 / args.rate
            delay = next_emit - time.monotonic()
            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        print("Stopping generator on Ctrl+C.")
    finally:
        remaining = producer.flush(30)
    if remaining:
        raise TimeoutError(f"{remaining} events did not flush to Redpanda")
    print(f"Live generator stopped: published={produced}; injected anomalies={anomaly_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
