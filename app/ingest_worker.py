from __future__ import annotations

import argparse
import queue
import time
import uuid
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from ws_scraper.app.config import APP_ROOT, RuntimeConfig
from ws_scraper.app.worker import build_collector_config, build_retrying_session, parse_args as parse_collector_args, ts_floor
from ws_scraper.ingest.coinbase_ws import CoinbaseWsDriver
from ws_scraper.ingest.polymarket_ws import PolymarketMarketResolver, PolymarketWsDriver
from ws_scraper.pipeline.observability import WorkerTelemetry, log_event
from ws_scraper.pipeline.runtime import PipelineSettings, env_int
from ws_scraper.pipeline.snapshot_aggregator import WsSnapshotAggregator
from ws_scraper.pipeline.storage import build_queue

from app.worker_common import JsonCheckpoint, Shutdown


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest websocket events and publish per-market snapshot batches")
    parser.add_argument("--output-root", default=str(APP_ROOT / "outputs" / "ws_snapshots"))
    parser.add_argument("--duration-seconds", type=int, default=0)
    parser.add_argument("--snapshot-interval-seconds", type=int, default=1)
    parser.add_argument("--log-every-seconds", type=int, default=300)
    parser.add_argument(
        "--checkpoint-path",
        default=str(APP_ROOT / "outputs" / "worker_state" / "ingest_checkpoint.json"),
    )
    return parser.parse_args(argv)


def _collector_namespace(args: argparse.Namespace) -> argparse.Namespace:
    return parse_collector_args(
        [
            "--output-root",
            args.output_root,
            "--duration-seconds",
            str(args.duration_seconds),
            "--snapshot-interval-seconds",
            str(args.snapshot_interval_seconds),
            "--log-every-seconds",
            str(args.log_every_seconds),
        ]
    )


def _snapshot_date(row: dict[str, Any]) -> str:
    ts_utc = str(row.get("ts_utc") or "")
    return ts_utc[:10] if len(ts_utc) >= 10 else time.strftime("%Y-%m-%d", time.gmtime())


def _publish_chunk(
    queue_client,
    checkpoint: JsonCheckpoint,
    telemetry: WorkerTelemetry,
    rows: list[dict[str, Any]],
    source: str,
    market: str,
    next_snapshot_ts: int,
    batches_published: int,
) -> int:
    batch_id = str(uuid.uuid4())
    payload = {
        "batch_id": batch_id,
        "created_at": int(time.time()),
        "snapshot_date": _snapshot_date(rows[0]),
        "source": source,
        "market": market,
        "rows": rows,
    }
    queue_client.publish(payload, message_id=f"ingest-{batch_id}")
    batches_published += 1
    checkpoint.save(
        {
            "next_snapshot_ts": next_snapshot_ts,
            "batches_published": batches_published,
            "last_batch_id": batch_id,
            "last_market": market,
            "last_source": source,
        }
    )
    telemetry.metrics.inc_counter("ingest_batches_published_total", labels={"market": market, "source": source})
    telemetry.metrics.set_gauge("ingest_last_batch_rows", len(rows), {"market": market, "source": source})
    return batches_published


def run(argv: list[str] | None = None) -> None:
    load_dotenv(APP_ROOT / ".env", override=True)
    args = parse_args(argv)
    runtime_cfg = RuntimeConfig.from_env()
    cfg = build_collector_config(_collector_namespace(args), runtime_cfg)
    pipeline = PipelineSettings.from_env()
    telemetry = WorkerTelemetry("ingest", pipeline.observability)
    telemetry.start()
    shutdown = Shutdown(logger=telemetry.logger)
    shutdown.install()

    queue_client = build_queue(pipeline.queues.ingest_queue_name, pipeline.queues, pipeline.identity)
    batch_size = env_int("INGEST_BATCH_SIZE", 250, minimum=1)
    checkpoint = JsonCheckpoint(Path(args.checkpoint_path))

    event_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=cfg.queue_maxsize)
    aggregator = WsSnapshotAggregator()
    pending_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}

    session = build_retrying_session()
    resolver = PolymarketMarketResolver(cfg, session)
    cb_driver = CoinbaseWsDriver(cfg, event_queue)
    pm_driver = PolymarketWsDriver(cfg, event_queue)

    state = checkpoint.load()
    next_snapshot_ts = int(
        state.get("next_snapshot_ts")
        or (ts_floor(int(time.time()), cfg.snapshot_interval_seconds) + cfg.snapshot_interval_seconds)
    )
    batches_published = int(state.get("batches_published") or 0)

    try:
        cb_driver.connect()
        pm_driver.start()
        changed, metas, bindings, errors = resolver.refresh(int(time.time()))
        aggregator.update_polymarket_meta(metas)
        if changed:
            pm_driver.set_asset_bindings(bindings)
        for error in errors:
            log_event(telemetry.logger, 30, "resolver_warning", detail=error)

        telemetry.set_ready(True)
        started = time.monotonic()
        next_log_ts = int(time.time()) + cfg.log_every_seconds
        while not shutdown.requested():
            telemetry.heartbeat()
            if cfg.duration_seconds > 0 and (time.monotonic() - started) >= cfg.duration_seconds:
                break

            now_ts = int(time.time())
            drained = 0
            while drained < 20_000:
                try:
                    event = event_queue.get_nowait()
                except queue.Empty:
                    break
                aggregator.apply_event(event)
                drained += 1

            while next_snapshot_ts <= now_ts:
                rows = aggregator.build_snapshots(float(next_snapshot_ts), cfg.snapshot_interval_seconds)
                max_stale_ms = 0.0
                for row in rows:
                    source = str(row.get("venue") or "unknown").strip() or "unknown"
                    market = str(row.get("market_key") or "unknown").strip() or "unknown"
                    key = (source, market)
                    pending_rows.setdefault(key, []).append(row)
                    try:
                        max_stale_ms = max(max_stale_ms, float(row.get("stale_ms") or 0.0))
                    except (TypeError, ValueError):
                        pass
                telemetry.metrics.set_gauge("ws_stale_ms", max_stale_ms, {"worker": "ingest"})
                for (source, market), bucket in list(pending_rows.items()):
                    while len(bucket) >= batch_size:
                        chunk = list(bucket[:batch_size])
                        del bucket[:batch_size]
                        batches_published = _publish_chunk(
                            queue_client,
                            checkpoint,
                            telemetry,
                            chunk,
                            source,
                            market,
                            next_snapshot_ts,
                            batches_published,
                        )
                    if not bucket:
                        pending_rows.pop((source, market), None)
                next_snapshot_ts += cfg.snapshot_interval_seconds

            telemetry.metrics.set_gauge(
                "ingest_pending_rows",
                sum(len(rows) for rows in pending_rows.values()),
                {"worker": "ingest"},
            )

            if now_ts >= next_log_ts:
                log_event(
                    telemetry.logger,
                    20,
                    "ingest_progress",
                    batches_published=batches_published,
                    pending_markets=len(pending_rows),
                    pending_rows=sum(len(rows) for rows in pending_rows.values()),
                )
                next_log_ts = now_ts + cfg.log_every_seconds
            time.sleep(0.05)
    except Exception as exc:
        telemetry.set_unhealthy(str(exc))
        log_event(telemetry.logger, 40, "ingest_worker_failed", error=str(exc))
        raise
    finally:
        try:
            for (source, market), rows in list(pending_rows.items()):
                if rows:
                    batches_published = _publish_chunk(
                        queue_client,
                        checkpoint,
                        telemetry,
                        list(rows),
                        source,
                        market,
                        next_snapshot_ts,
                        batches_published,
                    )
        finally:
            cb_driver.close()
            pm_driver.close()
            queue_client.close()
            telemetry.close()


if __name__ == "__main__":
    run()
