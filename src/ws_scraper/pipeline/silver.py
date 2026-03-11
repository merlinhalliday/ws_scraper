from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ws_scraper.pipeline.bronze import decode_rows_zstd
from ws_scraper.pipeline.contracts import BronzeWritten, ExportIntent, sha256_hex, utcnow_iso


SILVER_SCHEMA = pa.schema(
    [
        pa.field("snapshot_date", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("market", pa.string(), nullable=False),
        pa.field("snapshot_hour", pa.int32(), nullable=True),
        pa.field("ts_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("last_event_ts_utc", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("bucket_start_ts_utc", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("snapshot_interval_s", pa.int32(), nullable=False),
        pa.field("msg_count_1s", pa.int64(), nullable=False),
        pa.field("trade_count_1s", pa.int64(), nullable=False),
        pa.field("book_update_count_1s", pa.int64(), nullable=False),
        pa.field("dropped_events_1s", pa.int64(), nullable=False),
        pa.field("stale_ms", pa.float64(), nullable=True),
        pa.field("best_bid", pa.float64(), nullable=True),
        pa.field("best_ask", pa.float64(), nullable=True),
        pa.field("mid", pa.float64(), nullable=True),
        pa.field("spread_bps", pa.float64(), nullable=True),
        pa.field("last_trade_px", pa.float64(), nullable=True),
        pa.field("last_trade_size", pa.float64(), nullable=True),
        pa.field("last_trade_side", pa.string(), nullable=True),
        pa.field("slug", pa.string(), nullable=True),
        pa.field("condition_id", pa.string(), nullable=True),
        pa.field("yes_best_bid", pa.float64(), nullable=True),
        pa.field("yes_best_ask", pa.float64(), nullable=True),
        pa.field("yes_mid", pa.float64(), nullable=True),
        pa.field("no_best_bid", pa.float64(), nullable=True),
        pa.field("no_best_ask", pa.float64(), nullable=True),
        pa.field("no_mid", pa.float64(), nullable=True),
        pa.field("last_trade_px_yes", pa.float64(), nullable=True),
        pa.field("last_trade_px_no", pa.float64(), nullable=True),
    ]
)


def _blank_to_none(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _parse_timestamp(value: Any) -> datetime | None:
    value = _blank_to_none(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_epoch_seconds(value: Any) -> datetime | None:
    value = _blank_to_none(value)
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _parse_float(value: Any) -> float | None:
    value = _blank_to_none(value)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any, default: int | None = None) -> int | None:
    value = _blank_to_none(value)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_text(value: Any) -> str | None:
    value = _blank_to_none(value)
    if value is None:
        return None
    return str(value).strip() or None


def _normalize_row(row: dict[str, Any], snapshot_date: str, source: str, market: str, include_hour: bool) -> dict[str, Any]:
    ts_utc = _parse_timestamp(row.get("ts_utc"))
    if ts_utc is None:
        raise RuntimeError("silver_row_missing_ts_utc")
    snapshot_hour = ts_utc.hour if include_hour else None
    return {
        "snapshot_date": snapshot_date,
        "source": source,
        "market": market,
        "snapshot_hour": snapshot_hour,
        "ts_utc": ts_utc,
        "last_event_ts_utc": _parse_timestamp(row.get("last_event_ts_utc")),
        "bucket_start_ts_utc": _parse_epoch_seconds(row.get("bucket_start_ts")),
        "snapshot_interval_s": int(_parse_int(row.get("snapshot_interval_s"), default=0) or 0),
        "msg_count_1s": int(_parse_int(row.get("msg_count_1s"), default=0) or 0),
        "trade_count_1s": int(_parse_int(row.get("trade_count_1s"), default=0) or 0),
        "book_update_count_1s": int(_parse_int(row.get("book_update_count_1s"), default=0) or 0),
        "dropped_events_1s": int(_parse_int(row.get("dropped_events_1s"), default=0) or 0),
        "stale_ms": _parse_float(row.get("stale_ms")),
        "best_bid": _parse_float(row.get("best_bid")),
        "best_ask": _parse_float(row.get("best_ask")),
        "mid": _parse_float(row.get("mid")),
        "spread_bps": _parse_float(row.get("spread_bps")),
        "last_trade_px": _parse_float(row.get("last_trade_px")),
        "last_trade_size": _parse_float(row.get("last_trade_size")),
        "last_trade_side": _parse_text(row.get("last_trade_side")),
        "slug": _parse_text(row.get("slug")),
        "condition_id": _parse_text(row.get("condition_id")),
        "yes_best_bid": _parse_float(row.get("yes_best_bid")),
        "yes_best_ask": _parse_float(row.get("yes_best_ask")),
        "yes_mid": _parse_float(row.get("yes_mid")),
        "no_best_bid": _parse_float(row.get("no_best_bid")),
        "no_best_ask": _parse_float(row.get("no_best_ask")),
        "no_mid": _parse_float(row.get("no_mid")),
        "last_trade_px_yes": _parse_float(row.get("last_trade_px_yes")),
        "last_trade_px_no": _parse_float(row.get("last_trade_px_no")),
    }


def normalize_rows(rows: list[dict[str, Any]], *, snapshot_date: str, source: str, market: str, include_hour: bool) -> list[dict[str, Any]]:
    return [_normalize_row(row, snapshot_date=snapshot_date, source=source, market=market, include_hour=include_hour) for row in rows]


def rows_to_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    columns = {field.name: [row.get(field.name) for row in rows] for field in SILVER_SCHEMA}
    table = pa.Table.from_pydict(columns, schema=SILVER_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def silver_blob_name(batch_id: str, snapshot_date: str, source: str, market: str, *, hour: int | None) -> str:
    parts = [
        f"silver/parquet/date={snapshot_date}",
        f"source={source}",
        f"market={market}",
    ]
    if hour is not None:
        parts.append(f"hour={hour:02d}")
    parts.append(f"{batch_id}.parquet")
    return "/".join(parts)


def export_target_relpath(batch_id: str, snapshot_date: str, source: str, market: str, *, hour: int | None) -> str:
    parts = [snapshot_date, source, market]
    if hour is not None:
        parts.append(f"{hour:02d}")
    parts.append(f"{batch_id}.parquet")
    return "/".join(parts)


def process_bronze_message(
    *,
    blob_store,
    raw_container: str,
    silver_container: str,
    ledger,
    export_queue,
    bronze_message: dict[str, Any],
    include_hour_partition: bool,
    logger,
    metrics,
) -> ExportIntent:
    bronze = BronzeWritten.from_dict(bronze_message)
    if not bronze.batch_id:
        raise RuntimeError("bronze_message_missing_batch_id")

    ledger_key = f"transform/{bronze.batch_id}"
    if ledger.exists(ledger_key):
        metrics.inc_counter("transform_duplicate_batch_total", labels={"market": bronze.market, "source": bronze.source})
        return ExportIntent.from_dict(ledger.read(ledger_key) or {})

    bronze_bytes = blob_store.download_bytes(raw_container, bronze.bronze_blob_path)
    rows = decode_rows_zstd(bronze_bytes)
    normalized_rows = normalize_rows(
        rows,
        snapshot_date=bronze.snapshot_date,
        source=bronze.source,
        market=bronze.market,
        include_hour=include_hour_partition,
    )
    hour = normalized_rows[0]["snapshot_hour"] if normalized_rows else None
    silver_bytes = rows_to_parquet_bytes(normalized_rows)
    silver_path = silver_blob_name(
        bronze.batch_id,
        bronze.snapshot_date,
        bronze.source,
        bronze.market,
        hour=hour,
    )
    blob_store.upload_bytes_if_absent(
        silver_container,
        silver_path,
        silver_bytes,
        content_type="application/octet-stream",
    )
    content_sha256 = sha256_hex(silver_bytes)
    intent = ExportIntent(
        export_id=bronze.batch_id,
        silver_blob_path=silver_path,
        target_relpath=export_target_relpath(
            bronze.batch_id,
            bronze.snapshot_date,
            bronze.source,
            bronze.market,
            hour=hour,
        ),
        content_sha256=content_sha256,
        content_length=len(silver_bytes),
        produced_at=utcnow_iso(),
        snapshot_date=bronze.snapshot_date,
        source=bronze.source,
        market=bronze.market,
    )
    export_queue.publish(intent.to_dict(), message_id=f"export-{intent.export_id}")
    ledger.claim(ledger_key, intent.to_dict())
    lag_seconds = 0.0
    bronze_ts = _parse_timestamp(bronze.produced_at)
    if bronze_ts is not None:
        lag_seconds = max(0.0, (datetime.now(timezone.utc) - bronze_ts).total_seconds())
    metrics.inc_counter("transform_batches_total", labels={"market": bronze.market, "source": bronze.source})
    metrics.set_gauge("transform_lag_seconds", lag_seconds, {"worker": "transform"})
    logger.info(
        "transform_batch_written",
        extra={
            "event": "transform_batch_written",
            "batch_id": bronze.batch_id,
            "source": bronze.source,
            "market": bronze.market,
            "silver_blob_path": silver_path,
            "row_count": len(normalized_rows),
        },
    )
    return intent
