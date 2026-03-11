from __future__ import annotations

import argparse
import io
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from polygale.config import APP_ROOT

from app.worker_common import (
    BlobSink,
    JsonCheckpoint,
    Shutdown,
    build_queue_from_env,
    env_int,
)

try:
    import zstandard as zstd
except Exception:
    zstd = None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume queued batches and persist immutable blobs")
    parser.add_argument(
        "--checkpoint-path",
        default=str(APP_ROOT / "outputs" / "worker_state" / "persist_checkpoint.json"),
    )
    return parser.parse_args(argv)


def _date_for_rows(rows: list[dict[str, Any]]) -> str:
    ts = str((rows[0] if rows else {}).get("ts_utc") or "")
    if not ts:
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).strftime("%Y-%m-%d")


def _market_for_rows(rows: list[dict[str, Any]]) -> str:
    return str((rows[0] if rows else {}).get("market_key") or "unknown").strip() or "unknown"


def _encode_rows_zstd(rows: list[dict[str, Any]], level: int) -> bytes:
    if zstd is None:
        raise RuntimeError("missing_dependency:zstandard")
    out = io.BytesIO()
    cctx = zstd.ZstdCompressor(level=level)
    with cctx.stream_writer(out, closefd=False) as writer:
        for row in rows:
            writer.write((json.dumps(row, separators=(",", ":"), default=str) + "\n").encode("utf-8"))
    return out.getvalue()


def run(argv: list[str] | None = None) -> None:
    load_dotenv(APP_ROOT / ".env", override=True)
    args = parse_args(argv)

    blob_conn = (os.getenv("AZURE_BLOB_CONNECTION_STRING") or "").strip()
    container = (os.getenv("AZURE_BLOB_CONTAINER") or "ws-snapshots").strip()
    if not blob_conn:
        raise RuntimeError("missing AZURE_BLOB_CONNECTION_STRING")

    queue_client = build_queue_from_env()
    sink = BlobSink(blob_conn, container)
    checkpoint = JsonCheckpoint(Path(args.checkpoint_path))
    shutdown = Shutdown()
    shutdown.install()

    batch_fetch = env_int("PERSIST_FETCH_BATCH", 8, minimum=1)
    visibility_timeout = env_int("PERSIST_VISIBILITY_TIMEOUT_SECONDS", 180, minimum=30)
    zstd_level = env_int("PERSIST_ZSTD_LEVEL", 3, minimum=1)

    while not shutdown.requested():
        envelopes = queue_client.receive(
            max_messages=batch_fetch,
            visibility_timeout=visibility_timeout,
            wait_seconds=2,
        )
        if not envelopes:
            continue

        for envelope in envelopes:
            body = envelope.body
            rows = list(body.get("rows") or [])
            if not rows:
                queue_client.ack(envelope.receipt)
                continue

            batch_id = str(body.get("batch_id") or f"batch-{int(time.time() * 1000)}")
            date_str = _date_for_rows(rows)
            market = _market_for_rows(rows)
            blob_name = f"raw/ndjson-zstd/date={date_str}/market={market}/{batch_id}.ndjson.zst"
            payload = _encode_rows_zstd(rows, level=zstd_level)

            # checkpoint only after successful immutable write
            sink.write_bytes(blob_name, payload)
            queue_client.ack(envelope.receipt)
            checkpoint.save(
                {
                    "last_batch_id": batch_id,
                    "last_blob_path": blob_name,
                    "last_row_count": len(rows),
                    "processed_at": int(time.time()),
                }
            )


if __name__ == "__main__":
    run()
