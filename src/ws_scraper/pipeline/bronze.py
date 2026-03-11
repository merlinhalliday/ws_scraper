from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from typing import Any

from ws_scraper.pipeline.contracts import BronzeWritten, sha256_hex, utcnow_iso

try:
    import zstandard as zstd
except Exception:
    zstd = None


def _require_zstd():
    if zstd is None:
        raise RuntimeError("missing_dependency:zstandard")


def snapshot_date_for_rows(rows: list[dict[str, Any]]) -> str:
    ts_value = str((rows[0] if rows else {}).get("ts_utc") or "")
    if not ts_value:
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    return datetime.fromisoformat(ts_value.replace("Z", "+00:00")).astimezone(timezone.utc).strftime("%Y-%m-%d")


def source_for_rows(rows: list[dict[str, Any]]) -> str:
    return str((rows[0] if rows else {}).get("venue") or "unknown").strip() or "unknown"


def market_for_rows(rows: list[dict[str, Any]]) -> str:
    return str((rows[0] if rows else {}).get("market_key") or "unknown").strip() or "unknown"


def validate_rows(rows: list[dict[str, Any]], expected_source: str, expected_market: str) -> None:
    for row in rows:
        source = str(row.get("venue") or "").strip()
        market = str(row.get("market_key") or "").strip()
        if source != expected_source or market != expected_market:
            raise RuntimeError(
                f"mixed_snapshot_batch_detected:expected_source={expected_source},expected_market={expected_market},"
                f"actual_source={source},actual_market={market}"
            )


def bronze_blob_name(snapshot_date: str, source: str, market: str, batch_id: str) -> str:
    return f"raw/ndjson-zstd/date={snapshot_date}/source={source}/market={market}/{batch_id}.ndjson.zst"


def encode_rows_zstd(rows: list[dict[str, Any]], level: int) -> bytes:
    _require_zstd()
    out = io.BytesIO()
    cctx = zstd.ZstdCompressor(level=int(level))
    with cctx.stream_writer(out, closefd=False) as writer:
        for row in rows:
            writer.write((json.dumps(row, separators=(",", ":"), default=str) + "\n").encode("utf-8"))
    return out.getvalue()


def decode_rows_zstd(payload: bytes) -> list[dict[str, Any]]:
    _require_zstd()
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(io.BytesIO(payload)) as reader:
        raw = reader.read().decode("utf-8")
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def persist_batch(
    *,
    blob_store,
    raw_container: str,
    ledger,
    transform_queue,
    payload: dict[str, Any],
    zstd_level: int,
    logger,
    metrics,
) -> BronzeWritten | None:
    rows = list(payload.get("rows") or [])
    if not rows:
        return None

    batch_id = str(payload.get("batch_id") or "").strip()
    if not batch_id:
        raise RuntimeError("batch_id_required")

    source = str(payload.get("source") or source_for_rows(rows)).strip() or "unknown"
    market = str(payload.get("market") or market_for_rows(rows)).strip() or "unknown"
    snapshot_date = str(payload.get("snapshot_date") or snapshot_date_for_rows(rows)).strip() or snapshot_date_for_rows(rows)
    validate_rows(rows, expected_source=source, expected_market=market)

    bronze_path = bronze_blob_name(snapshot_date, source, market, batch_id)
    ledger_key = f"persist/{batch_id}"
    if ledger.exists(ledger_key):
        metrics.inc_counter("persist_duplicate_batch_total", labels={"market": market, "source": source})
        return BronzeWritten.from_dict(ledger.read(ledger_key) or {})

    payload_bytes = encode_rows_zstd(rows, level=zstd_level)
    blob_store.upload_bytes_if_absent(raw_container, bronze_path, payload_bytes, content_type="application/zstd")
    content_sha256 = sha256_hex(payload_bytes)
    message = BronzeWritten(
        batch_id=batch_id,
        bronze_blob_path=bronze_path,
        source=source,
        market=market,
        snapshot_date=snapshot_date,
        content_sha256=content_sha256,
        row_count=len(rows),
        produced_at=utcnow_iso(),
    )
    transform_queue.publish(message.to_dict(), message_id=f"bronze-{batch_id}")
    ledger.claim(ledger_key, message.to_dict())
    metrics.inc_counter("persist_batches_total", labels={"market": market, "source": source})
    metrics.set_gauge("persist_last_batch_rows", len(rows), {"market": market, "source": source})
    logger.info(
        "persist_batch_written",
        extra={
            "event": "persist_batch_written",
            "batch_id": batch_id,
            "source": source,
            "market": market,
            "bronze_blob_path": bronze_path,
            "row_count": len(rows),
        },
    )
    return message
