from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from ws_scraper.app.config import APP_ROOT
from ws_scraper.pipeline.observability import WorkerTelemetry, log_event
from ws_scraper.pipeline.runtime import PipelineSettings, env_int
from ws_scraper.pipeline.silver import process_bronze_message
from ws_scraper.pipeline.storage import BlobLedger, BlobStore, build_queue

from app.worker_common import JsonCheckpoint, Shutdown


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform Bronze NDJSON blobs into typed Silver Parquet")
    parser.add_argument(
        "--checkpoint-path",
        default=str(APP_ROOT / "outputs" / "worker_state" / "transform_checkpoint.json"),
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> None:
    load_dotenv(APP_ROOT / ".env", override=True)
    args = parse_args(argv)
    settings = PipelineSettings.from_env()
    settings.blob.validate()
    telemetry = WorkerTelemetry("transform", settings.observability)
    telemetry.start()
    shutdown = Shutdown(logger=telemetry.logger)
    shutdown.install()

    transform_queue = build_queue(settings.queues.transform_queue_name, settings.queues, settings.identity)
    export_queue = build_queue(settings.queues.export_queue_name, settings.queues, settings.identity)
    blob_store = BlobStore(settings.blob, settings.identity)
    ledger = BlobLedger(blob_store, settings.blob.ledger_container, "ledger")
    checkpoint = JsonCheckpoint(Path(args.checkpoint_path))

    fetch_batch = env_int("TRANSFORM_FETCH_BATCH", 8, minimum=1)
    visibility_timeout = env_int("TRANSFORM_VISIBILITY_TIMEOUT_SECONDS", 300, minimum=30)

    telemetry.set_ready(True)
    try:
        while not shutdown.requested():
            telemetry.heartbeat()
            envelopes = transform_queue.receive(
                max_messages=fetch_batch,
                visibility_timeout=visibility_timeout,
                wait_seconds=2,
            )
            telemetry.metrics.set_gauge("transform_receive_batch_size", len(envelopes), {"worker": "transform"})
            if not envelopes:
                continue
            for envelope in envelopes:
                try:
                    intent = process_bronze_message(
                        blob_store=blob_store,
                        raw_container=settings.blob.raw_container,
                        silver_container=settings.blob.silver_container,
                        ledger=ledger,
                        export_queue=export_queue,
                        bronze_message=envelope.body,
                        include_hour_partition=settings.silver_partition_by_hour,
                        logger=telemetry.logger,
                        metrics=telemetry.metrics,
                    )
                    transform_queue.ack(envelope.receipt)
                    checkpoint.save(
                        {
                            "last_export_id": intent.export_id,
                            "last_silver_blob_path": intent.silver_blob_path,
                            "processed_at": intent.produced_at,
                        }
                    )
                except Exception as exc:
                    telemetry.metrics.inc_counter("transform_failures_total", labels={"worker": "transform"})
                    log_event(
                        telemetry.logger,
                        40,
                        "transform_batch_failed",
                        error=str(exc),
                        batch_id=str(envelope.body.get("batch_id") or ""),
                    )
    except Exception as exc:
        telemetry.set_unhealthy(str(exc))
        log_event(telemetry.logger, 40, "transform_worker_failed", error=str(exc))
        raise
    finally:
        transform_queue.close()
        export_queue.close()
        telemetry.close()


if __name__ == "__main__":
    run()
