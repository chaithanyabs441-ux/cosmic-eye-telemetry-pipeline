"""Publish synthetic streaming events with a repeatable normal/incident scenario."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
sys.path.insert(0, str(ROOT / "pipeline"))
from ingest import build_events, iso_time  # noqa: E402

BANDS = ["SPHEREx-D1", "SPHEREx-D2", "SPHEREx-D3", "SPHEREx-D4"]
BAND_WEIGHTS = [0.40, 0.30, 0.20, 0.10]


def region_weights(regions: list[str]) -> list[float]:
    """Give three regions more traffic while spreading the remainder evenly."""
    if len(regions) == 1:
        return [1.0]
    if len(regions) == 2:
        return [0.65, 0.35]
    rest = 0.42 / (len(regions) - 3)
    return [0.28, 0.18, 0.12, *([rest] * (len(regions) - 3))]


def make_variant(
    template: dict[str, Any],
    region: str,
    event_id: str,
    rng: random.Random,
    in_incident: bool,
    force_quality_alert: bool,
) -> dict[str, Any]:
    """Copy the event shape, then give this synthetic event its own values and ID."""
    payload = json.loads(json.dumps(template))
    payload["event_id"] = event_id
    payload["event_ts"] = iso_time()
    payload["region_key"] = region
    payload["band_id"] = rng.choices(BANDS, weights=BAND_WEIGHTS, k=1)[0]
    payload["is_synthetic"] = True

    if payload.get("event_type") == "exposure":
        quality = payload.setdefault("quality_metrics", {})
        if force_quality_alert or rng.random() < 0.02:
            # Noise is bell-shaped; saturation has a separate beta distribution.
            quality["noise_sigma"] = round(min(1.4, max(0.78, rng.gauss(0.98, 0.16))), 3)
            quality["saturated_pixel_fraction"] = round(0.08 + 0.64 * rng.betavariate(2, 2), 3)
        else:
            quality["noise_sigma"] = round(min(0.68, max(0.05, rng.gauss(0.34, 0.14))), 3)
            quality["saturated_pixel_fraction"] = round(0.18 * rng.betavariate(1.4, 8), 3)
    else:
        payload["session_id"] = f"anon-{rng.getrandbits(64):016x}"
        if in_incident:
            multiplier = rng.uniform(3.0, 4.0)
            payload["latency_ms"] = round(payload["latency_ms"] * multiplier)
            payload["http_status"] = rng.choices(
                [200, 400, 404, 500, 503], weights=[75, 8, 7, 7, 3], k=1
            )[0]
        else:
            # Around 2.6% of normal app requests return an HTTP error.
            payload["http_status"] = rng.choices(
                [200, 400, 404, 500, 503], weights=[97.4, 1.0, 0.8, 0.6, 0.2], k=1
            )[0]
    return payload


def main() -> int:
    from confluent_kafka import KafkaError, KafkaException, Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default=os.getenv("KAFKA_LIVE_TOPIC", "cosmic-eye-live"))
    parser.add_argument("--rate", type=float, default=50, help="base synthetic events per second")
    parser.add_argument("--duration", type=float, default=120, help="seconds; 0 runs until Ctrl+C")
    parser.add_argument("--regions", type=int, default=12, help="number of sky regions")
    parser.add_argument("--scenario", choices=["normal", "incident"], default="normal")
    parser.add_argument("--incident-region", default="sky_23_00")
    parser.add_argument("--count", type=int, default=0, help="optional output event limit; 0 uses duration")
    parser.add_argument("--seed", type=int, default=42, help="repeatable event values and region weights")
    parser.add_argument(
        "--inject-anomaly-every",
        type=int,
        default=0,
        help="force a varied exposure anomaly every N exposures; normally leave at 0",
    )
    parser.add_argument("--create-topic-only", action="store_true", help="create the topic and exit")
    args = parser.parse_args()
    if args.rate <= 0 or args.duration < 0 or args.count < 0:
        parser.error("rate must be positive; duration and count cannot be negative")
    if not 1 <= args.regions <= 648 or args.inject_anomaly_every < 0:
        parser.error("regions must be 1..648 and anomaly interval cannot be negative")
    if not args.incident_region.startswith("sky_"):
        parser.error("incident region must look like sky_23_00")

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
    regions = [args.incident_region]
    regions.extend(region for region in candidates if region != args.incident_region)
    regions = regions[: args.regions]
    weights = region_weights(regions)

    producer = Producer({"bootstrap.servers": brokers, "client.id": "cosmic-eye-live-generator"})
    run_id = uuid.uuid4().hex
    templates = build_events(
        count=0,
        seed=args.seed,
        source_mode="synthetic",
        dirty_every=0,
        live=True,
        stop_after=None,
    )
    started_at = time.monotonic()
    next_emit = started_at
    emitted = exposure_number = exposure_alerts = incident_events = 0
    interval = 1.0 / args.rate
    print(
        f"Scenario={args.scenario}; base_rate={args.rate:g}/s; "
        f"duration={'until Ctrl+C' if args.duration == 0 else f'{args.duration:g}s'}; "
        f"regions={args.regions}; incident_region={args.incident_region}; seed={args.seed}"
    )

    def publish(payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        producer.produce(topic, key=payload["event_id"].encode("utf-8"), value=encoded)
        producer.poll(0)

    try:
        while True:
            elapsed = time.monotonic() - started_at
            if args.duration and elapsed >= args.duration:
                break
            if args.count and emitted >= args.count:
                break

            template = next(templates)
            base_region = rng.choices(regions, weights=weights, k=1)[0]
            incident_active = args.scenario == "incident" and 120 <= elapsed < 240
            region_in_incident = incident_active and base_region == args.incident_region
            copies = 3 if region_in_incident else 1
            for _ in range(copies):
                if args.count and emitted >= args.count:
                    break
                if template.get("event_type") == "exposure":
                    exposure_number += 1
                    force_alert = bool(
                        args.inject_anomaly_every
                        and exposure_number % args.inject_anomaly_every == 0
                    )
                    if force_alert:
                        exposure_alerts += 1
                payload = make_variant(
                    template,
                    base_region,
                    f"{run_id}-{emitted:09d}",
                    rng,
                    region_in_incident,
                    force_alert,
                )
                publish(payload)
                emitted += 1
                if region_in_incident:
                    incident_events += 1
                if emitted % 500 == 0:
                    producer.flush(10)
                    print(f"Published={emitted}; incident_region_events={incident_events}")

            # Keep a steady base cadence with small jitter; the incident adds only targeted events.
            next_emit += interval * rng.uniform(0.97, 1.03)
            delay = next_emit - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:
                next_emit = time.monotonic()
    except KeyboardInterrupt:
        print("Stopping generator on Ctrl+C.")
    finally:
        remaining = producer.flush(30)
    if remaining:
        raise TimeoutError(f"{remaining} events did not flush to Redpanda")
    print(
        f"Producer stopped: published={emitted}; incident_region_events={incident_events}; "
        f"forced_exposure_alerts={exposure_alerts}; elapsed={time.monotonic() - started_at:.1f}s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
