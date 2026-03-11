from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from ws_scraper.app.config import APP_ROOT
from ws_scraper.auth.identity import build_local_graph_auth
from ws_scraper.export.local_onedrive import PersonalOneDriveRelayUploader
from ws_scraper.pipeline.observability import WorkerTelemetry, log_event
from ws_scraper.pipeline.relay import process_relay_intent
from ws_scraper.pipeline.runtime import PipelineSettings, env_int
from ws_scraper.pipeline.storage import BlobLedger, BlobStore, build_queue

from app.worker_common import JsonCheckpoint, Shutdown


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local-only relay from staged export blobs to personal OneDrive")
    parser.add_argument(
        "--checkpoint-path",
        default=str(APP_ROOT / "outputs" / "worker_state" / "relay_checkpoint.json"),
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> None:
    load_dotenv(APP_ROOT / ".env", override=True)
    args = parse_args(argv)
    settings = PipelineSettings.from_env()
    settings.blob.validate()
    if settings.export.export_mode != "personal_onedrive_local":
        raise RuntimeError("local_relay_worker_requires_EXPORT_MODE=personal_onedrive_local")

    telemetry = WorkerTelemetry("local_relay", settings.observability)
    telemetry.start()
    shutdown = Shutdown(logger=telemetry.logger)
    shutdown.install()

    relay_queue = build_queue(settings.queues.relay_queue_name, settings.queues, settings.identity)
    blob_store = BlobStore(settings.blob, settings.identity)
    ledger = BlobLedger(blob_store, settings.blob.ledger_container, "ledger")
    checkpoint = JsonCheckpoint(Path(args.checkpoint_path))
    graph_auth = build_local_graph_auth(settings.identity, logger=telemetry.logger)
    uploader = PersonalOneDriveRelayUploader(
        auth_manager=graph_auth,
        onedrive_folder=settings.export.onedrive_folder,
        timeout_seconds=settings.export.graph_timeout_seconds,
        chunk_size_bytes=settings.export.graph_chunk_size_bytes,
        logger=telemetry.logger,
    )

    fetch_batch = env_int("RELAY_FETCH_BATCH", 4, minimum=1)
    visibility_timeout = env_int("RELAY_VISIBILITY_TIMEOUT_SECONDS", 300, minimum=30)

    telemetry.set_ready(True)
    try:
        while not shutdown.requested():
            telemetry.heartbeat()
            envelopes = relay_queue.receive(
                max_messages=fetch_batch,
                visibility_timeout=visibility_timeout,
                wait_seconds=2,
            )
            telemetry.metrics.set_gauge("relay_receive_batch_size", len(envelopes), {"worker": "local_relay"})
            if not envelopes:
                continue
            for envelope in envelopes:
                try:
                    relay = process_relay_intent(
                        blob_store=blob_store,
                        export_container=settings.blob.export_container,
                        ledger=ledger,
                        relay_message=envelope.body,
                        uploader=uploader,
                        logger=telemetry.logger,
                        metrics=telemetry.metrics,
                    )
                    relay_queue.ack(envelope.receipt)
                    checkpoint.save(
                        {
                            "last_export_id": relay.export_id,
                            "last_target_relpath": relay.target_relpath,
                            "processed_at": relay.produced_at,
                        }
                    )
                except Exception as exc:
                    telemetry.metrics.inc_counter("relay_failures_total", labels={"worker": "local_relay"})
                    log_event(
                        telemetry.logger,
                        40,
                        "relay_upload_failed",
                        error=str(exc),
                        export_id=str(envelope.body.get("export_id") or ""),
                    )
    except Exception as exc:
        telemetry.set_unhealthy(str(exc))
        log_event(telemetry.logger, 40, "relay_worker_failed", error=str(exc))
        raise
    finally:
        relay_queue.close()
        telemetry.close()


if __name__ == "__main__":
    run()
