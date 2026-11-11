"""Publish varied, clearly synthetic demo events to the live Redpanda topic."""

from __future__ import annotations

import argparse
import json
import os
import random
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
    parser.add_argument("--rate", type=float, default=50, help="synthetic events per second")
    parser.add_argument("--duration", type=float, default=120, help="maximum run time in seconds")
    parser.add_argument("--regions", type=int, default=12, help="number of sky regions to spread events across")
    parser.add_argument("--count", type=int, default=0, help="optional event limit; 0 uses the duration")
    parser.add_argument("--seed", type=int, default=42, help="repeatable event-value seed")
    parser.add_argument(
        "--inject-anomaly-every",
        type=int,
        default=0,
        help="also force a varied exposure anomaly every N exposures; 0 uses random anomalies only",
    )
    parser.add_argument("--create-topic-only", action="store_true", help="create the topic and exit")
    args = parser.parse_args()
    if args.rate <= 0 or args.duration <= 0 or args.count < 0:
        parser.error("rate and duration must be positive; count cannot be negative")
    if not 1 <= args.regions <= 648 or args.inject_anomaly_every < 0:
        parser.error("regions must be 1..648 and anomaly interval cannot be negative")

    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
    topic = args.topic.strip()
    admin = AdminClient({"bootstrap.servers": brokers})
    create = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1,
                  config={"retention.ms": "86400000", "retention.bytes": "104857600"})]
    )
    try:
        create[topic].result(timeout=20)
    except KafkaException as exc:
        error = exc.args[0] if exc.args else None
        if not isinstance(error, KafkaError) or error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    if args.create_topic_only:
        print(f"Topic ready: {topic}")
        return 0

    rng = random.Random(args.seed)
    candidates = [f"sky_{ra:02d}_{dec:02d}" for ra in range(36) for dec in range(18)]
    rng.shuffle(candidates)
    regions = candidates[: args.regions]
    bands = [f"SPHEREx-D{number}" for number in range(1, 5)]
    producer = Producer({"bootstrap.servers": brokers, "client.id": "cosmic-eye-live-generator"})
    run_id = uuid.uuid4().hex
    events = build_events(
        count=0,
        seed=args.seed,
        source_mode="synthetic",
        dirty_every=0,
        live=True,
        stop_after=None,
    )

    produced = exposure_number = anomaly_count = latency_burst_count = 0
    burst_region: str | None = None
    burst_remaining = 0
    start_time = time.monotonic()
    next_emit = start_time
    print(
        f"Publishing synthetic events to {topic}: rate={args.rate:g}/s, "
        f"duration={args.duration:g}s, regions={args.regions}, seed={args.seed}. Ctrl+C stops."
    )
    try:
        for payload in events:
            elapsed = time.monotonic() - start_time
            if elapsed >= args.duration or (args.count and produced >= args.count):
                break

            # Stable value generation from the seed; unique IDs keep separate runs distinct.
            payload["event_id"] = f"{run_id}-{produced:08d}"
            payload["region_key"] = rng.choice(regions)
            payload["band_id"] = rng.choice(bands)

            if payload.get("event_type") == "exposure":
                exposure_number += 1
                metrics = payload.setdefault("quality_metrics", {})
                force_anomaly = bool(
                    args.inject_anomaly_every
                    and exposure_number % args.inject_anomaly_every == 0
                )
                if force_anomaly or rng.random() < 0.08:
                    # Vary both values and location so alerts are not identical copies.
                    metrics["noise_sigma"] = round(min(1.4, max(0.78, rng.gauss(0.98, 0.16))), 3)
                    metrics["saturated_pixel_fraction"] = round(0.08 + 0.64 * rng.betavariate(2, 2), 3)
                    payload["anomaly_injected"] = True
                    anomaly_count += 1
                else:
                    metrics["noise_sigma"] = round(min(0.68, max(0.05, rng.gauss(0.34, 0.14))), 3)
                    metrics["saturated_pixel_fraction"] = round(0.18 * rng.betavariate(1.4, 8), 3)

            else:
                if burst_remaining == 0 and rng.random() < 0.025:
                    burst_region = rng.choice(regions)
                    burst_remaining = rng.randint(5, 12)
                if burst_remaining:
                    payload["region_key"] = burst_region
                    payload["latency_ms"] = rng.randint(3200, 6500)
                    burst_remaining -= 1
                    latency_burst_count += 1
                else:
                    payload["latency_ms"] = int(rng.lognormvariate(6.0, 0.55))
                    payload["latency_ms"] = min(payload["latency_ms"], 2800)
                payload["http_status"] = rng.choices(
                    [200, 400, 404, 500, 503], weights=[94, 2, 2, 1.5, 0.5], k=1
                )[0]

            payload["is_synthetic"] = True
            encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            producer.produce(topic, key=payload["event_id"].encode("utf-8"), value=encoded)
            producer.poll(0)
            produced += 1
            if produced % 500 == 0:
                producer.flush(10)
                print(
                    f"Published={produced}; exposure anomalies={anomaly_count}; "
                    f"high-latency burst events={latency_burst_count}"
                )
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
    print(
        f"Live generator stopped: published={produced}; exposure anomalies={anomaly_count}; "
        f"high-latency burst events={latency_burst_count}; elapsed={time.monotonic() - start_time:.1f}s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
