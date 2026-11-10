"""Generate synthetic/SPHEREx metadata events, pass them through Kafka, and load BigQuery.

No event files are written locally. Kafka is a bounded temporary transport
buffer; BigQuery load payloads are constructed in memory and flushed in small
batches.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"))

SIA_URL = "https://irsa.ipac.caltech.edu/SIA"
SIA_COLLECTION = "spherex_qr2"
REGION_RA_STEP = 10
REGION_DEC_STEP = 10


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_time(value: datetime | None = None) -> str:
    return (value or utc_now()).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def as_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or str(value).strip() in ("", "--", "null", "None"):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def field(row: dict[str, Any], *names: str) -> Any:
    lowered = {str(k).lower(): v for k, v in row.items()}
    for name in names:
        value = lowered.get(name.lower())
        if value not in (None, "", "--"):
            return value
    return None


def fetch_irsa_metadata(max_records: int, timeout: int = 45) -> list[dict[str, Any]]:
    """Fetch only a small SIA2 metadata result; never fetch access_url products."""
    import requests

    response = requests.get(
        SIA_URL,
        params={
            "COLLECTION": SIA_COLLECTION,
            "POS": "CIRCLE 150.1 2.2 0.1",
            "MAXREC": str(max_records),
            "RESPONSEFORMAT": "CSV",
        },
        headers={"User-Agent": "CosmicEyeCoursePipeline/1.0"},
        timeout=timeout,
    )
    response.raise_for_status()
    text = response.text.lstrip("\ufeff\r\n ")
    if not text or text.startswith("<"):
        raise RuntimeError("IRSA SIA2 did not return CSV metadata")
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows or not rows[0]:
        raise RuntimeError("IRSA SIA2 returned no metadata rows for the bounded sky query")
    return rows[:max_records]


def metadata_record(row: dict[str, Any], accessed_at: str, index: int) -> dict[str, Any]:
    ra = as_float(field(row, "s_ra", "s_ra_deg"))
    dec = as_float(field(row, "s_dec", "s_dec_deg"))
    mjd = as_float(field(row, "t_min"))
    observation_time = None
    if mjd is not None:
        observation_time = iso_time(datetime(1858, 11, 17, tzinfo=timezone.utc) + timedelta(days=mjd))
    band_name = field(row, "energy_bandpassname", "band", "band_name") or "unknown"
    return {
        "obs_id": str(field(row, "obs_id", "obsid") or f"IRSA-ROW-{index:05d}"),
        "em_min": as_float(field(row, "em_min", "em_min_m")),
        "em_max": as_float(field(row, "em_max", "em_max_m")),
        "s_ra": ra,
        "s_dec": dec,
        "t_min_mjd": mjd,
        "observation_time": observation_time,
        "calib_level": field(row, "calib_level"),
        "access_url": field(row, "access_url"),
        "obs_collection": field(row, "obs_collection", "collection") or SIA_COLLECTION,
        "band_name": str(band_name),
        "metadata_is_synthetic": False,
        "source": "NASA/IPAC IRSA SIA2 SPHEREx QR2",
        "source_accessed_at": accessed_at,
    }


def synthetic_metadata(index: int, rng: random.Random, accessed_at: str) -> dict[str, Any]:
    """Create clearly labeled synthetic metadata when --source synthetic is selected."""
    ra = rng.uniform(0.0, 360.0)
    dec = rng.uniform(-89.9, 89.9)
    band = rng.randint(1, 6)
    return {
        "obs_id": f"SYNTH-OBS-{index:09d}",
        "em_min": None,
        "em_max": None,
        "s_ra": round(ra, 6),
        "s_dec": round(dec, 6),
        "t_min_mjd": None,
        "observation_time": None,
        "calib_level": None,
        "access_url": None,
        "obs_collection": "synthetic",
        "band_name": f"SPHEREx-D{band}",
        "metadata_is_synthetic": True,
        "source": "seeded synthetic generator (not NASA data)",
        "source_accessed_at": accessed_at,
    }


def region_key(ra: float | None, dec: float | None) -> str:
    if ra is None or dec is None:
        return "unknown"
    ra_cell = int(ra % 360 // REGION_RA_STEP)
    dec_cell = int((dec + 90) // REGION_DEC_STEP)
    return f"sky_{ra_cell:02d}_{dec_cell:02d}"


def make_event(
    index: int,
    rng: random.Random,
    metadata: list[dict[str, Any]],
    base_time: datetime,
    source_mode: str,
    accessed_at: str,
    seed: int,
    live: bool,
) -> dict[str, Any]:
    exposure = index % 2 == 0
    base = metadata[(index // 2) % len(metadata)] if exposure else None
    event_time = utc_now() if live else base_time - timedelta(seconds=(index % 86_400))
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"cosmic-eye:{seed}:{index}"))
    if exposure:
        base = base or synthetic_metadata(index, rng, accessed_at)
        metrics: dict[str, Any] = {
            "background_level": round(rng.uniform(0.01, 4.5), 6),
            "noise_sigma": round(rng.uniform(0.001, 0.8), 6),
            "saturated_pixel_fraction": round(rng.uniform(0.0, 0.04), 6),
            "pointing_error_arcsec": round(rng.uniform(0.0, 5.0), 6),
            "processing_latency_ms": rng.randint(50, 8000),
        }
        payload = {
            "event_id": event_id,
            "event_type": "exposure",
            "event_ts": iso_time(event_time),
            "obs_id": base["obs_id"],
            "source_obs_id": base["obs_id"] if not base["metadata_is_synthetic"] else None,
            "band_id": base["band_name"],
            "region_key": region_key(base["s_ra"], base["s_dec"]),
            "is_synthetic": True,
            "metadata_is_synthetic": base["metadata_is_synthetic"],
            "quality_metrics_synthetic": True,
            "quality_metrics": metrics,
            "observation": {
                key: base[key]
                for key in (
                    "em_min", "em_max", "s_ra", "s_dec", "t_min_mjd",
                    "observation_time", "calib_level", "access_url", "obs_collection",
                )
            },
            "provenance": {
                "source": base["source"],
                "source_accessed_at": base["source_accessed_at"],
                "synthetic_fields": ["quality_metrics", "event_id", "event_ts"],
                "generator_seed": seed,
            },
        }
    else:
        ra = rng.uniform(0.0, 360.0)
        dec = rng.uniform(-89.9, 89.9)
        payload = {
            "event_id": event_id,
            "event_type": "usage",
            "event_ts": iso_time(event_time),
            "session_id": f"anon-{rng.getrandbits(64):016x}",
            "event_name": rng.choice(["search", "browse", "preview", "skymap"]),
            "endpoint": rng.choice(["/api/search", "/api/browse", "/api/preview", "/api/skymap"]),
            "target": rng.choice(["Orion", "Andromeda", "Cygnus", "synthetic-target"]),
            "band_id": f"SPHEREx-D{rng.randint(1, 6)}",
            "region_key": region_key(ra, dec),
            "latency_ms": rng.randint(30, 2500),
            "http_status": rng.choices([200, 400, 404, 500], weights=[92, 3, 3, 2])[0],
            "is_synthetic": True,
            "metadata_is_synthetic": True,
            "provenance": {
                "source": "seeded synthetic app-usage generator",
                "source_accessed_at": accessed_at,
                "synthetic_fields": ["all fields"],
                "generator_seed": seed,
            },
        }
    return payload


def inject_dirty(event: dict[str, Any], dirty_kind: str) -> dict[str, Any]:
    """Inject one explicit quality issue while retaining provenance."""
    payload = json.loads(json.dumps(event))
    flags = payload.setdefault("dirty_flags", [])
    flags.append(dirty_kind)
    if dirty_kind == "null_value":
        if payload["event_type"] == "usage":
            payload["target"] = None
        else:
            payload["quality_metrics"]["noise_sigma"] = None
    elif dirty_kind == "out_of_range":
        if payload["event_type"] == "usage":
            payload["latency_ms"] = -25
        else:
            payload["quality_metrics"]["saturated_pixel_fraction"] = 1.7
    elif dirty_kind == "wrong_type":
        if payload["event_type"] == "usage":
            payload["latency_ms"] = "slow"
        else:
            payload["quality_metrics"]["noise_sigma"] = "not-a-number"
    elif dirty_kind == "late_event":
        payload["event_ts"] = iso_time(utc_now() - timedelta(days=2))
    elif dirty_kind == "wrong_envelope_type":
        payload["event_type"] = 17
    return payload


def build_events(
    count: int,
    seed: int,
    source_mode: str,
    dirty_every: int,
    live: bool,
    stop_after: int | None,
) -> Iterator[dict[str, Any]]:
    rng = random.Random(seed)
    accessed_at = iso_time()
    metadata: list[dict[str, Any]] = []
    if source_mode == "irsa":
        fetched = fetch_irsa_metadata(max(1, min(100, (count + 1) // 2)))
        metadata = [metadata_record(row, accessed_at, i) for i, row in enumerate(fetched)]
    else:
        metadata = [synthetic_metadata(i, rng, accessed_at) for i in range(max(1, (count + 1) // 2))]

    index = 0
    previous: dict[str, Any] | None = None
    dirty_types = ["duplicate", "null_value", "out_of_range", "wrong_type", "late_event", "wrong_envelope_type"]
    # A stable history anchor makes batch timestamps reproducible across reruns.
    batch_anchor = datetime(2025, 1, 1, tzinfo=timezone.utc) if not live else utc_now()
    while live or index < count:
        if live and stop_after is not None and index >= stop_after:
            break
        if dirty_every > 0 and (index + 1) % dirty_every == 0:
            kind = dirty_types[((index + 1) // dirty_every - 1) % len(dirty_types)]
            if kind == "duplicate" and previous is not None:
                event = json.loads(json.dumps(previous))
                event.setdefault("dirty_flags", []).append("duplicate")
            else:
                event = make_event(index, rng, metadata, batch_anchor, source_mode, accessed_at, seed, live)
                event = inject_dirty(event, kind)
                previous = event
        else:
            event = make_event(index, rng, metadata, batch_anchor, source_mode, accessed_at, seed, live)
            previous = event
        yield event
        index += 1


def parse_event(payload: dict[str, Any]) -> str | None:
    if not isinstance(payload.get("event_id"), str) or not payload["event_id"]:
        return "event_id must be a non-empty string"
    if payload.get("event_type") not in ("exposure", "usage"):
        return "event_type must be exposure or usage"
    event_ts = payload.get("event_ts")
    if not isinstance(event_ts, str):
        return "event_ts must be a string timestamp"
    try:
        datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
    except ValueError:
        return "event_ts is not a parseable ISO timestamp"
    if not isinstance(payload.get("is_synthetic"), bool):
        return "is_synthetic must be boolean"
    return None


def bq_schema():
    from google.cloud import bigquery

    return {
        "events": [
            bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_id", "STRING"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("event_ts", "STRING"),
            bigquery.SchemaField("is_synthetic", "BOOL"),
            bigquery.SchemaField("region_key", "STRING"),
            bigquery.SchemaField("band_id", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("source_accessed_at", "STRING"),
            bigquery.SchemaField("dirty_flags", "STRING"),
            bigquery.SchemaField("raw_payload", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ],
        "rejected_events": [
            bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_id", "STRING"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("raw_payload", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rejection_reason", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    }


class BigQueryWriter:
    def __init__(self, run_id: str, batch_size: int):
        from google.cloud import bigquery

        project = os.getenv("GCP_PROJECT_ID", "").strip()
        self.location = os.getenv("BIGQUERY_LOCATION", "US").strip()
        self.dataset_name = os.getenv("BIGQUERY_RAW_DATASET", "raw").strip()
        if not project:
            raise RuntimeError("GCP_PROJECT_ID is empty in the local .env file")
        self.project = project
        self.dataset_id = f"{project}.{self.dataset_name}"
        self.run_id = run_id
        self.batch_size = batch_size
        self.client = bigquery.Client(project=project, location=self.location)
        self.schemas = bq_schema()
        self.tables: dict[str, str] = {}
        self.buffers: dict[str, list[dict[str, Any]]] = {"events": [], "rejected_events": []}
        self.job_batches = 0
        self.ensure_dataset()
        for table_name, schema in self.schemas.items():
            table_id = f"{self.dataset_id}.{table_name}"
            try:
                self.client.get_table(table_id)
            except Exception as exc:
                from google.api_core.exceptions import NotFound

                if not isinstance(exc, NotFound):
                    raise
                table = bigquery.Table(table_id, schema=schema)
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="ingested_at",
                )
                self.client.create_table(table)
            self.tables[table_name] = table_id

    def ensure_dataset(self) -> None:
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound

        try:
            self.client.get_dataset(self.dataset_id)
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_id)
            dataset.location = self.location
            self.client.create_dataset(dataset, timeout=30)

    def append(self, table: str, row: dict[str, Any]) -> None:
        self.buffers[table].append(row)
        if len(self.buffers[table]) >= self.batch_size:
            self.flush(table)

    def flush(self, table: str) -> int:
        from google.cloud import bigquery

        rows = self.buffers[table]
        if not rows:
            return 0
        # The newline-delimited JSON batch stays in RAM (BytesIO); no dataset
        # history is written to disk.
        body = "\n".join(json.dumps(row, separators=(",", ":"), ensure_ascii=False) for row in rows).encode("utf-8")
        source = io.BytesIO(body)
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=self.schemas[table],
        )
        job = self.client.load_table_from_file(
            source,
            self.tables[table],
            job_config=config,
            size=len(body),
            rewind=True,
            location=self.location,
        )
        job.result(timeout=120)
        loaded = len(rows)
        self.buffers[table] = []
        self.job_batches += 1
        return loaded

    def counts_for_run(self) -> tuple[int, int]:
        from google.cloud import bigquery

        query = f"""
          SELECT
            COUNTIF(event_type IS NOT NULL) AS landed,
            0 AS rejected
          FROM `{self.tables['events']}` WHERE run_id = @run_id
        """
        landed = next(iter(self.client.query(
            query,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", self.run_id)
            ]),
            location=self.location,
        ).result()))["landed"]
        query = f"SELECT COUNT(*) AS rejected FROM `{self.tables['rejected_events']}` WHERE run_id = @run_id"
        rejected = next(iter(self.client.query(
            query,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", self.run_id)
            ]),
            location=self.location,
        ).result()))["rejected"]
        return int(landed), int(rejected)


def event_row(run_id: str, payload: dict[str, Any], ingested_at: str) -> dict[str, Any]:
    provenance = payload.get("provenance") or {}
    return {
        "run_id": run_id,
        "event_id": payload.get("event_id"),
        "event_type": payload.get("event_type"),
        "event_ts": payload.get("event_ts"),
        "is_synthetic": payload.get("is_synthetic"),
        "region_key": payload.get("region_key"),
        "band_id": payload.get("band_id"),
        "source": provenance.get("source"),
        "source_accessed_at": provenance.get("source_accessed_at"),
        "dirty_flags": json.dumps(payload.get("dirty_flags", []), separators=(",", ":")),
        "raw_payload": json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        "ingested_at": ingested_at,
    }


def rejected_row(run_id: str, payload: dict[str, Any], reason: str, ingested_at: str) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "event_id": payload.get("event_id") if isinstance(payload.get("event_id"), str) else None,
        "event_type": payload.get("event_type") if isinstance(payload.get("event_type"), str) else None,
        "raw_payload": json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        "rejection_reason": reason,
        "ingested_at": ingested_at,
    }


def run(args: argparse.Namespace) -> int:
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    run_id = uuid.uuid4().hex
    topic = f"cosmic-eye-{run_id[:12]}"
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
    batch_size = args.load_batch_size
    writer = BigQueryWriter(run_id, batch_size)
    admin = AdminClient({"bootstrap.servers": brokers})
    admin.create_topics([
        NewTopic(topic, num_partitions=1, replication_factor=1,
                 config={"retention.ms": "3600000", "retention.bytes": "104857600"})
    ])[topic].result(timeout=20)
    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": f"cosmic-eye-{run_id}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": brokers})
    consumer.subscribe([topic])
    consumer.poll(0)  # Start subscription assignment before producing.

    produced = consumed = landed = rejected = 0
    ended = False
    error: str | None = None
    next_emit = time.monotonic()
    generation_started: float | None = None
    generation_elapsed: float | None = None

    def consume_one(timeout: float) -> bool:
        nonlocal consumed, landed, rejected, ended, error
        message = consumer.poll(timeout)
        if message is None:
            return False
        if message.error():
            error = str(message.error())
            return False
        try:
            payload = json.loads(message.value().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = {"raw_message": message.value().decode("utf-8", errors="replace")}
            writer.append("rejected_events", rejected_row(run_id, payload, "message is not valid JSON", iso_time()))
            rejected += 1
            consumed += 1
            return True
        if payload.get("control") == "batch_end":
            ended = True
            return True
        consumed += 1
        reason = parse_event(payload)
        timestamp = iso_time()
        if reason:
            writer.append("rejected_events", rejected_row(run_id, payload, reason, timestamp))
            rejected += 1
        else:
            writer.append("events", event_row(run_id, payload, timestamp))
            landed += 1
        return True

    try:
        pipeline_started = time.monotonic()
        if args.mode == "batch":
            event_iter = build_events(args.count, args.seed, args.source, args.dirty_every, False, None)
            for payload in event_iter:
                if generation_started is None:
                    generation_started = time.monotonic()
                producer.produce(topic, value=json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
                producer.poll(0)
                produced += 1
                while consume_one(0):
                    if error:
                        raise RuntimeError("Kafka consumer reported an error")
            generation_elapsed = max(time.monotonic() - generation_started, 0.001)
            if producer.flush(30):
                raise TimeoutError("Producer could not flush all event messages")
            producer.produce(topic, value=json.dumps({"control": "batch_end", "run_id": run_id}).encode("utf-8"))
            if producer.flush(30):
                raise TimeoutError("Producer could not flush the batch-end marker")
            deadline = time.monotonic() + 180
            while not ended and time.monotonic() < deadline:
                consume_one(0.5)
                if error:
                    raise RuntimeError("Kafka consumer reported an error")
            if not ended:
                raise TimeoutError("Did not receive the batch-end marker from Redpanda")
        else:
            start = time.monotonic()
            event_iter = build_events(args.count, args.seed, args.source, args.dirty_every, True, args.count if args.count > 0 else None)
            for payload in event_iter:
                if generation_started is None:
                    generation_started = time.monotonic()
                producer.produce(topic, value=json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
                producer.poll(0)
                produced += 1
                while consume_one(0):
                    if error:
                        raise RuntimeError("Kafka consumer reported an error")
                next_emit += 1.0 / args.rate
                delay = next_emit - time.monotonic()
                if delay > 0:
                    consume_one(min(delay, 0.05))
                elif produced % 100 == 0:
                    print(f"Live progress: produced={produced}, consumed={consumed}")
            generation_elapsed = max(time.monotonic() - generation_started, 0.001)
            if producer.flush(30):
                raise TimeoutError("Producer could not flush all live event messages")
            # Finite live runs use --count; 0 means run until Ctrl+C.
            if args.count > 0:
                producer.produce(topic, value=json.dumps({"control": "batch_end", "run_id": run_id}).encode("utf-8"))
                if producer.flush(30):
                    raise TimeoutError("Producer could not flush the live-run end marker")
                deadline = time.monotonic() + 60
                while not ended and time.monotonic() < deadline:
                    consume_one(0.5)

        writer.flush("events")
        writer.flush("rejected_events")
        elapsed_seconds = max(time.monotonic() - pipeline_started, 0.001)
        actual_landed, actual_rejected = writer.counts_for_run()
        if produced != consumed or actual_landed != landed or actual_rejected != rejected:
            raise RuntimeError(
                f"Count mismatch: produced={produced}, consumed={consumed}, "
                f"landed={landed}/{actual_landed}, rejected={rejected}/{actual_rejected}"
            )
        print(f"Run ID: {run_id}")
        print(f"Mode/source: {args.mode}/{args.source}; topic={topic}")
        print(f"Produced: {produced}")
        print(f"Consumed: {consumed}")
        print(f"Landed in BigQuery {writer.tables['events']}: {actual_landed}")
        print(f"Rejected in BigQuery {writer.tables['rejected_events']}: {actual_rejected}")
        print(f"BigQuery batch load jobs: {writer.job_batches}")
        print(f"Generator/broker publish elapsed: {generation_elapsed:.3f} seconds")
        print(f"Generator/broker publish rate: {produced / generation_elapsed:.2f} events/second")
        print(f"End-to-end ingest elapsed: {elapsed_seconds:.3f} seconds")
        print(f"End-to-end ingest rate: {produced / elapsed_seconds:.2f} events/second")
        print("Counts: PASS (produced = consumed = landed + rejected)")
        return 0
    except KeyboardInterrupt:
        # Preserve partial live batches on a normal Ctrl+C; the broker topic is
        # removed immediately afterward in finally.
        producer.flush(15)
        idle_polls = 0
        while idle_polls < 4:
            if consume_one(0.5):
                idle_polls = 0
            else:
                idle_polls += 1
        writer.flush("events")
        writer.flush("rejected_events")
        print(f"Stopped live run after produced={produced}, consumed={consumed}; buffered rows flushed.")
        raise
    finally:
        producer.flush(15)
        consumer.close()
        admin.delete_topics([topic], operation_timeout=20)[topic].result(timeout=20)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["batch", "live"], default="batch")
    parser.add_argument("--count", type=int, default=100,
                        help="batch row count; live count 0 runs until Ctrl+C")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--source", choices=["irsa", "synthetic"], default="irsa",
                        help="IRSA SIA2 metadata or clearly labeled synthetic metadata")
    parser.add_argument("--rate", type=float, default=50.0,
                        help="live events/second; target max 200, default is conservative")
    parser.add_argument("--dirty-every", type=int, default=10,
                        help="inject duplicate/null/out-of-range/wrong-type/late cases periodically; 0 disables")
    parser.add_argument("--load-batch-size", type=int, default=200,
                        help="maximum rows buffered for one in-memory BigQuery load job")
    args = parser.parse_args()
    if args.count < 0 or args.rate <= 0 or args.load_batch_size < 1 or args.dirty_every < 0:
        parser.error("count and dirty-every must be non-negative; rate and load-batch-size must be positive")
    if args.mode == "batch" and args.count == 0:
        parser.error("batch mode requires --count greater than zero")
    return args


if __name__ == "__main__":
    try:
        raise SystemExit(run(parse_args()))
    except KeyboardInterrupt:
        print("Stopped by user; buffered load batches are flushed before exit.")
        raise SystemExit(130)
    except Exception as exc:
        # Do not print environment variables, URIs, or credential contents.
        print(f"Pipeline error: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)
