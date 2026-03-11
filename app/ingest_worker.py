from __future__ import annotations

import argparse
import queue
import time
import uuid
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from polygale.config import APP_ROOT, RuntimeConfig
from ws_scraper import (
    CoinbaseWsDriver,
    PolymarketMarketResolver,
    PolymarketWsDriver,
    WsSnapshotAggregator,
    build_collector_config,
    build_retrying_session,
    parse_args as parse_collector_args,
    ts_floor,
)

from app.worker_common import JsonCheckpoint, Shutdown, build_queue_from_env, env_int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest websocket events and publish snapshot batches to queue")
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


def run(argv: list[str] | None = None) -> None:
    load_dotenv(APP_ROOT / ".env", override=True)
    args = parse_args(argv)
    runtime_cfg = RuntimeConfig.from_env()
    cfg = build_collector_config(_collector_namespace(args), runtime_cfg)

    queue_client = build_queue_from_env()
    batch_size = env_int("INGEST_BATCH_SIZE", 250, minimum=1)
    checkpoint = JsonCheckpoint(Path(args.checkpoint_path))
    shutdown = Shutdown()
    shutdown.install()

    event_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=cfg.queue_maxsize)
    aggregator = WsSnapshotAggregator()
    pending_rows: list[dict[str, Any]] = []

    session = build_retrying_session()
    resolver = PolymarketMarketResolver(cfg, session)
    cb_driver = CoinbaseWsDriver(cfg, event_queue)
    pm_driver = PolymarketWsDriver(cfg, event_queue)

    state = checkpoint.load()
    next_snapshot_ts = int(state.get("next_snapshot_ts") or (ts_floor(int(time.time()), cfg.snapshot_interval_seconds) + cfg.snapshot_interval_seconds))
    batches_published = int(state.get("batches_published") or 0)

    cb_driver.connect()
    pm_driver.start()
    changed, metas, bindings, _errors = resolver.refresh(int(time.time()))
    aggregator.update_polymarket_meta(metas)
    if changed:
        pm_driver.set_asset_bindings(bindings)

    started = time.monotonic()
    try:
        while not shutdown.requested():
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
                if rows:
                    pending_rows.extend(rows)
                while len(pending_rows) >= batch_size:
                    chunk = pending_rows[:batch_size]
                    pending_rows = pending_rows[batch_size:]
                    payload = {
                        "batch_id": str(uuid.uuid4()),
                        "created_at": now_ts,
                        "rows": chunk,
                    }
                    queue_client.publish(payload)
                    batches_published += 1
                    checkpoint.save(
                        {
                            "next_snapshot_ts": next_snapshot_ts,
                            "batches_published": batches_published,
                            "last_batch_id": payload["batch_id"],
                        }
                    )
                next_snapshot_ts += cfg.snapshot_interval_seconds

            time.sleep(0.05)
    finally:
        if pending_rows:
            payload = {"batch_id": str(uuid.uuid4()), "created_at": int(time.time()), "rows": pending_rows}
            queue_client.publish(payload)
            batches_published += 1
            checkpoint.save(
                {
                    "next_snapshot_ts": next_snapshot_ts,
                    "batches_published": batches_published,
                    "last_batch_id": payload["batch_id"],
                }
            )
        cb_driver.close()
        pm_driver.close()


if __name__ == "__main__":
    run()
