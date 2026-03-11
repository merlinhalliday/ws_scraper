from __future__ import annotations

import logging
import os
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ws_scraper.pipeline.bronze import persist_batch
from ws_scraper.pipeline.contracts import ExportIntent
from ws_scraper.pipeline.export_stage import process_export_intent, staged_blob_name
from ws_scraper.pipeline.observability import MetricRegistry
from ws_scraper.pipeline.relay import process_relay_intent
from ws_scraper.pipeline.runtime import PipelineSettings
from ws_scraper.pipeline.silver import SILVER_SCHEMA, normalize_rows, process_bronze_message, rows_to_parquet_bytes
from ws_scraper.pipeline.storage import BlobLedger, InMemoryBlobStore, MemoryQueue


def _logger() -> logging.Logger:
    logger = logging.getLogger("test")
    logger.handlers = [logging.NullHandler()]
    return logger


@contextmanager
def patched_env(values: dict[str, str]):
    previous = {key: os.environ.get(key) for key in values}
    try:
        for key, value in values.items():
            os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class FakeUploader:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, bytes]] = []

    def upload_bytes(self, relpath: str, payload: bytes) -> None:
        self.uploads.append((relpath, bytes(payload)))


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        MemoryQueue._state.clear()

    def test_pipeline_settings_rejects_delegated_identity_in_cloud(self) -> None:
        with patched_env(
            {
                "APP_ENV": "cloud",
                "IDENTITY_MODE": "delegated_local",
                "AZURE_BLOB_CONNECTION_STRING": "UseDevelopmentStorage=true",
                "AZURE_SERVICEBUS_CONNECTION_STRING": "Endpoint=sb://example/;SharedAccessKeyName=a;SharedAccessKey=b",
            }
        ):
            with self.assertRaisesRegex(RuntimeError, "delegated_local_identity_not_allowed_in_cloud"):
                PipelineSettings.from_env()

    def test_persist_batch_is_idempotent(self) -> None:
        blob_store = InMemoryBlobStore()
        ledger = BlobLedger(blob_store, "ledger", "ledger")
        transform_queue = MemoryQueue("transform")
        metrics = MetricRegistry()
        payload = {
            "batch_id": "batch-1",
            "source": "coinbase",
            "market": "cb-btcusd",
            "snapshot_date": "2026-03-11",
            "rows": [
                {
                    "ts_utc": "2026-03-11T12:00:00Z",
                    "market_key": "cb-btcusd",
                    "venue": "coinbase",
                    "snapshot_interval_s": 1,
                    "msg_count_1s": 1,
                    "trade_count_1s": 0,
                    "book_update_count_1s": 1,
                    "dropped_events_1s": 0,
                }
            ],
        }

        first = persist_batch(
            blob_store=blob_store,
            raw_container="raw",
            ledger=ledger,
            transform_queue=transform_queue,
            payload=payload,
            zstd_level=1,
            logger=_logger(),
            metrics=metrics,
        )
        second = persist_batch(
            blob_store=blob_store,
            raw_container="raw",
            ledger=ledger,
            transform_queue=transform_queue,
            payload=payload,
            zstd_level=1,
            logger=_logger(),
            metrics=metrics,
        )

        messages = transform_queue.receive(max_messages=10, visibility_timeout=30, wait_seconds=0)
        self.assertEqual(1, len(messages))
        self.assertEqual(first.batch_id, second.batch_id)
        self.assertTrue(blob_store.exists("raw", first.bronze_blob_path))

    def test_transform_normalizes_mixed_shape_rows_to_fixed_schema(self) -> None:
        rows = [
            {
                "ts_utc": "2026-03-11T12:00:00Z",
                "last_event_ts_utc": "2026-03-11T11:59:59Z",
                "bucket_start_ts": "",
                "snapshot_interval_s": 1,
                "msg_count_1s": 1,
                "trade_count_1s": 1,
                "book_update_count_1s": 1,
                "dropped_events_1s": 0,
                "stale_ms": "4.5",
                "best_bid": "100.1",
                "best_ask": "100.2",
                "mid": "100.15",
                "spread_bps": "9.98",
                "last_trade_px": "100.12",
                "last_trade_size": "0.05",
                "last_trade_side": "BUY",
                "slug": "",
                "condition_id": "",
                "yes_best_bid": "",
                "yes_best_ask": "",
                "yes_mid": "",
                "no_best_bid": "",
                "no_best_ask": "",
                "no_mid": "",
                "last_trade_px_yes": "",
                "last_trade_px_no": "",
            },
            {
                "ts_utc": "2026-03-11T12:00:01Z",
                "last_event_ts_utc": "2026-03-11T12:00:00Z",
                "bucket_start_ts": 1741694400,
                "snapshot_interval_s": 1,
                "msg_count_1s": 2,
                "trade_count_1s": 0,
                "book_update_count_1s": 2,
                "dropped_events_1s": 0,
                "stale_ms": "0",
                "best_bid": "",
                "best_ask": "",
                "mid": "",
                "spread_bps": "",
                "last_trade_px": "",
                "last_trade_size": "",
                "last_trade_side": "",
                "slug": "btc-updown-5m-123",
                "condition_id": "abc",
                "yes_best_bid": "0.51",
                "yes_best_ask": "0.52",
                "yes_mid": "0.515",
                "no_best_bid": "0.48",
                "no_best_ask": "0.49",
                "no_mid": "0.485",
                "last_trade_px_yes": "0.51",
                "last_trade_px_no": "0.49",
            },
        ]

        normalized = normalize_rows(rows, snapshot_date="2026-03-11", source="mixed", market="mixed", include_hour=True)
        parquet_bytes = rows_to_parquet_bytes(normalized)
        table = pq.read_table(pa.BufferReader(parquet_bytes))

        self.assertEqual(SILVER_SCHEMA.names, table.schema.names)
        self.assertEqual("timestamp[us, tz=UTC]", str(table.schema.field("ts_utc").type))
        self.assertEqual([12, 12], table.column("snapshot_hour").to_pylist())
        self.assertIsNone(table.column("best_bid").to_pylist()[1])
        self.assertAlmostEqual(0.51, table.column("yes_best_bid").to_pylist()[1], places=6)

    def test_export_stage_deduplicates_and_local_relay_uploads_once(self) -> None:
        blob_store = InMemoryBlobStore()
        ledger = BlobLedger(blob_store, "ledger", "ledger")
        export_queue = MemoryQueue("export")
        relay_queue = MemoryQueue("relay")
        metrics = MetricRegistry()

        silver_path = "silver/parquet/date=2026-03-11/source=coinbase/market=cb-btcusd/hour=12/batch-1.parquet"
        blob_store.upload_bytes("silver", silver_path, b"silver-payload", overwrite=False)
        intent = ExportIntent(
            export_id="batch-1",
            silver_blob_path=silver_path,
            target_relpath="2026-03-11/coinbase/cb-btcusd/12/batch-1.parquet",
            content_sha256="",
            content_length=14,
            produced_at="2026-03-11T12:00:00Z",
            snapshot_date="2026-03-11",
            source="coinbase",
            market="cb-btcusd",
        )

        first = process_export_intent(
            blob_store=blob_store,
            silver_container="silver",
            export_container="export",
            ledger=ledger,
            relay_queue=relay_queue,
            export_message=intent.to_dict(),
            logger=_logger(),
            metrics=metrics,
        )
        second = process_export_intent(
            blob_store=blob_store,
            silver_container="silver",
            export_container="export",
            ledger=ledger,
            relay_queue=relay_queue,
            export_message=intent.to_dict(),
            logger=_logger(),
            metrics=metrics,
        )

        relay_messages = relay_queue.receive(max_messages=10, visibility_timeout=30, wait_seconds=0)
        self.assertEqual(1, len(relay_messages))
        self.assertEqual(first.export_id, second.export_id)
        self.assertTrue(blob_store.exists("export", staged_blob_name(intent.target_relpath)))

        uploader = FakeUploader()
        process_relay_intent(
            blob_store=blob_store,
            export_container="export",
            ledger=ledger,
            relay_message=first.to_dict(),
            uploader=uploader,
            logger=_logger(),
            metrics=metrics,
        )
        process_relay_intent(
            blob_store=blob_store,
            export_container="export",
            ledger=ledger,
            relay_message=first.to_dict(),
            uploader=uploader,
            logger=_logger(),
            metrics=metrics,
        )
        self.assertEqual(1, len(uploader.uploads))

    def test_in_memory_end_to_end_pipeline(self) -> None:
        blob_store = InMemoryBlobStore()
        ledger = BlobLedger(blob_store, "ledger", "ledger")
        metrics = MetricRegistry()
        ingest_payload = {
            "batch_id": "batch-2",
            "source": "coinbase",
            "market": "cb-ethusd",
            "snapshot_date": "2026-03-11",
            "rows": [
                {
                    "ts_utc": "2026-03-11T13:00:00Z",
                    "market_key": "cb-ethusd",
                    "venue": "coinbase",
                    "snapshot_interval_s": 1,
                    "msg_count_1s": 2,
                    "trade_count_1s": 1,
                    "book_update_count_1s": 1,
                    "dropped_events_1s": 0,
                    "last_event_ts_utc": "2026-03-11T12:59:59Z",
                    "stale_ms": 2.0,
                    "best_bid": 2000.1,
                    "best_ask": 2000.4,
                    "mid": 2000.25,
                    "spread_bps": 1.5,
                    "last_trade_px": 2000.2,
                    "last_trade_size": 0.01,
                    "last_trade_side": "BUY",
                    "slug": "",
                    "bucket_start_ts": "",
                    "condition_id": "",
                    "yes_best_bid": "",
                    "yes_best_ask": "",
                    "yes_mid": "",
                    "no_best_bid": "",
                    "no_best_ask": "",
                    "no_mid": "",
                    "last_trade_px_yes": "",
                    "last_trade_px_no": "",
                }
            ],
        }

        transform_queue = MemoryQueue("transform")
        export_queue = MemoryQueue("export")
        relay_queue = MemoryQueue("relay")

        bronze = persist_batch(
            blob_store=blob_store,
            raw_container="raw",
            ledger=ledger,
            transform_queue=transform_queue,
            payload=ingest_payload,
            zstd_level=1,
            logger=_logger(),
            metrics=metrics,
        )
        intent = process_bronze_message(
            blob_store=blob_store,
            raw_container="raw",
            silver_container="silver",
            ledger=ledger,
            export_queue=export_queue,
            bronze_message=bronze.to_dict(),
            include_hour_partition=True,
            logger=_logger(),
            metrics=metrics,
        )
        relay = process_export_intent(
            blob_store=blob_store,
            silver_container="silver",
            export_container="export",
            ledger=ledger,
            relay_queue=relay_queue,
            export_message=intent.to_dict(),
            logger=_logger(),
            metrics=metrics,
        )

        self.assertTrue(blob_store.exists("raw", bronze.bronze_blob_path))
        self.assertTrue(blob_store.exists("silver", intent.silver_blob_path))
        self.assertTrue(blob_store.exists("export", relay.staged_blob_path))


if __name__ == "__main__":
    unittest.main()
