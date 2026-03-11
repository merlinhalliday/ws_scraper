#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


try:
    import zstandard as zstd
except Exception:
    zstd = None


@dataclass(frozen=True)
class TransformConfig:
    bronze_root: Path
    silver_root: Path
    checkpoint_path: Path
    poll_interval_seconds: int
    partition_by_hour: bool
    compression: str


def _read_json_lines(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if path.suffixes[-2:] == [".ndjson", ".zst"]:
        if zstd is None:
            raise RuntimeError("missing_dependency:zstandard (pip install zstandard)")
        with path.open("rb") as fh:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(fh) as reader:
                text_stream = reader.read().decode("utf-8", errors="replace")
                for line in text_stream.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    payload = json.loads(line)
                    if isinstance(payload, dict):
                        records.append(payload)
    else:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if isinstance(payload, dict):
                    records.append(payload)
    return records


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _parse_ts(ts_utc: str) -> datetime:
    text = str(ts_utc or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_record(raw: dict[str, Any]) -> dict[str, Any]:
    ts_utc = str(raw.get("ts_utc") or "")
    if not ts_utc:
        return {}
    dt = _parse_ts(ts_utc)
    source = str(raw.get("venue") or raw.get("source") or "unknown").strip().lower()
    market_key = str(raw.get("market_key") or "unknown").strip().lower()

    return {
        "ts_utc": dt,
        "date": dt.strftime("%Y-%m-%d"),
        "hour": dt.strftime("%H"),
        "source": source,
        "market_key": market_key,
        "snapshot_interval_s": _as_int(raw.get("snapshot_interval_s")),
        "msg_count_1s": _as_int(raw.get("msg_count_1s")),
        "trade_count_1s": _as_int(raw.get("trade_count_1s")),
        "book_update_count_1s": _as_int(raw.get("book_update_count_1s")),
        "dropped_events_1s": _as_int(raw.get("dropped_events_1s")),
        "last_event_ts_utc": str(raw.get("last_event_ts_utc") or ""),
        "stale_ms": _as_float(raw.get("stale_ms")),
        "best_bid": _as_float(raw.get("best_bid")),
        "best_ask": _as_float(raw.get("best_ask")),
        "mid": _as_float(raw.get("mid")),
        "spread_bps": _as_float(raw.get("spread_bps")),
        "last_trade_px": _as_float(raw.get("last_trade_px")),
        "last_trade_size": _as_float(raw.get("last_trade_size")),
        "last_trade_side": str(raw.get("last_trade_side") or ""),
        "slug": str(raw.get("slug") or ""),
        "bucket_start_ts": _as_int(raw.get("bucket_start_ts")),
        "condition_id": str(raw.get("condition_id") or ""),
        "yes_best_bid": _as_float(raw.get("yes_best_bid")),
        "yes_best_ask": _as_float(raw.get("yes_best_ask")),
        "yes_mid": _as_float(raw.get("yes_mid")),
        "no_best_bid": _as_float(raw.get("no_best_bid")),
        "no_best_ask": _as_float(raw.get("no_best_ask")),
        "no_mid": _as_float(raw.get("no_mid")),
        "last_trade_px_yes": _as_float(raw.get("last_trade_px_yes")),
        "last_trade_px_no": _as_float(raw.get("last_trade_px_no")),
    }


def _schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("ts_utc", pa.timestamp("us", tz="UTC")),
            pa.field("date", pa.string()),
            pa.field("hour", pa.string()),
            pa.field("source", pa.string()),
            pa.field("market_key", pa.string()),
            pa.field("snapshot_interval_s", pa.int32()),
            pa.field("msg_count_1s", pa.int64()),
            pa.field("trade_count_1s", pa.int64()),
            pa.field("book_update_count_1s", pa.int64()),
            pa.field("dropped_events_1s", pa.int64()),
            pa.field("last_event_ts_utc", pa.string()),
            pa.field("stale_ms", pa.float64()),
            pa.field("best_bid", pa.float64()),
            pa.field("best_ask", pa.float64()),
            pa.field("mid", pa.float64()),
            pa.field("spread_bps", pa.float64()),
            pa.field("last_trade_px", pa.float64()),
            pa.field("last_trade_size", pa.float64()),
            pa.field("last_trade_side", pa.string()),
            pa.field("slug", pa.string()),
            pa.field("bucket_start_ts", pa.int64()),
            pa.field("condition_id", pa.string()),
            pa.field("yes_best_bid", pa.float64()),
            pa.field("yes_best_ask", pa.float64()),
            pa.field("yes_mid", pa.float64()),
            pa.field("no_best_bid", pa.float64()),
            pa.field("no_best_ask", pa.float64()),
            pa.field("no_mid", pa.float64()),
            pa.field("last_trade_px_yes", pa.float64()),
            pa.field("last_trade_px_no", pa.float64()),
        ]
    )


def _load_checkpoint(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    if not isinstance(payload, dict):
        return set()
    items = payload.get("processed", [])
    if not isinstance(items, list):
        return set()
    return {str(x) for x in items if str(x).strip()}


def _save_checkpoint(path: Path, processed: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps({"processed": sorted(processed)}, indent=2), encoding="utf-8")
    tmp.replace(path)


def _discover_bronze_files(bronze_root: Path) -> list[Path]:
    return sorted(bronze_root.rglob("*.ndjson.zst"))


def run_once(cfg: TransformConfig) -> tuple[int, int]:
    processed = _load_checkpoint(cfg.checkpoint_path)
    import pyarrow as pa
    import pyarrow.dataset as ds

    schema = _schema()
    partition_fields = ["date", "source", "market_key"] + (["hour"] if cfg.partition_by_hour else [])

    ingested_files = 0
    written_rows = 0
    for path in _discover_bronze_files(cfg.bronze_root):
        marker = str(path.resolve())
        if marker in processed:
            continue

        rows = []
        for raw in _read_json_lines(path):
            normalized = _normalize_record(raw)
            if normalized:
                rows.append(normalized)

        if rows:
            table = pa.Table.from_pylist(rows, schema=schema)
            ds.write_dataset(
                table,
                base_dir=str(cfg.silver_root / "parquet"),
                format="parquet",
                partitioning=partition_fields,
                existing_data_behavior="overwrite_or_ignore",
                compression=cfg.compression,
            )
            written_rows += table.num_rows

        processed.add(marker)
        ingested_files += 1

    _save_checkpoint(cfg.checkpoint_path, processed)
    return ingested_files, written_rows


def parse_args() -> tuple[TransformConfig, bool]:
    parser = argparse.ArgumentParser(description="Transform Bronze ndjson.zst snapshots to Silver Parquet")
    parser.add_argument("--bronze-root", default="outputs/ws_snapshots", help="Bronze root (raw immutable ndjson.zst)")
    parser.add_argument("--silver-root", default="outputs/silver", help="Silver root")
    parser.add_argument("--checkpoint-path", default="outputs/silver/.silver_transform_checkpoint.json")
    parser.add_argument("--poll-interval-seconds", type=int, default=300)
    parser.add_argument("--partition-by-hour", action="store_true", help="Enable hour as a partition key")
    parser.add_argument("--compression", default="zstd", choices=["zstd", "snappy", "gzip", "brotli", "none"])
    parser.add_argument("--run-once", action="store_true", help="Run one batch and exit")
    args = parser.parse_args()

    cfg = TransformConfig(
        bronze_root=Path(args.bronze_root),
        silver_root=Path(args.silver_root),
        checkpoint_path=Path(args.checkpoint_path),
        poll_interval_seconds=max(30, int(args.poll_interval_seconds)),
        partition_by_hour=bool(args.partition_by_hour),
        compression="NONE" if args.compression == "none" else args.compression,
    )
    cfg_run_once = bool(args.run_once)
    return cfg, cfg_run_once


def main() -> None:
    cfg, run_once_only = parse_args()
    while True:
        files, rows = run_once(cfg)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"[{now}] silver_transform files_ingested={files} rows_written={rows}")
        if run_once_only:
            break
        time.sleep(cfg.poll_interval_seconds)


if __name__ == "__main__":
    main()
