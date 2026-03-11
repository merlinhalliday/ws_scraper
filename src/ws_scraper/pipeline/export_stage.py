from __future__ import annotations

from datetime import datetime, timezone

from ws_scraper.pipeline.contracts import ExportIntent, RelayIntent, sha256_hex, utcnow_iso


def staged_blob_name(target_relpath: str) -> str:
    cleaned = str(target_relpath).strip("/").replace("\\", "/")
    return f"relay/payloads/{cleaned}"


def manifest_blob_name(export_id: str) -> str:
    return f"relay/manifests/{export_id}.json"


def process_export_intent(
    *,
    blob_store,
    silver_container: str,
    export_container: str,
    ledger,
    relay_queue,
    export_message: dict[str, object],
    logger,
    metrics,
) -> RelayIntent:
    intent = ExportIntent.from_dict(dict(export_message))
    if not intent.export_id:
        raise RuntimeError("export_intent_missing_export_id")

    ledger_key = f"export/{intent.export_id}"
    if ledger.exists(ledger_key):
        metrics.inc_counter("export_dispatch_duplicate_total", labels={"market": intent.market, "source": intent.source})
        return RelayIntent.from_dict(ledger.read(ledger_key) or {})

    silver_bytes = blob_store.download_bytes(silver_container, intent.silver_blob_path)
    staged_path = staged_blob_name(intent.target_relpath)
    blob_store.upload_bytes_if_absent(export_container, staged_path, silver_bytes, content_type="application/octet-stream")
    relay = RelayIntent(
        export_id=intent.export_id,
        staged_blob_path=staged_path,
        target_relpath=intent.target_relpath,
        content_sha256=sha256_hex(silver_bytes),
        content_length=len(silver_bytes),
        produced_at=utcnow_iso(),
    )
    manifest = {
        "export_id": intent.export_id,
        "silver_blob_path": intent.silver_blob_path,
        "staged_blob_path": staged_path,
        "target_relpath": intent.target_relpath,
        "content_sha256": relay.content_sha256,
        "content_length": relay.content_length,
        "snapshot_date": intent.snapshot_date,
        "source": intent.source,
        "market": intent.market,
        "produced_at": relay.produced_at,
    }
    blob_store.upload_json(export_container, manifest_blob_name(intent.export_id), manifest, overwrite=True)
    relay_queue.publish(relay.to_dict(), message_id=f"relay-{relay.export_id}")
    ledger.claim(ledger_key, relay.to_dict())
    lag_seconds = 0.0
    try:
        produced_at = datetime.fromisoformat(intent.produced_at.replace("Z", "+00:00"))
        if produced_at.tzinfo is None:
            produced_at = produced_at.replace(tzinfo=timezone.utc)
        lag_seconds = max(0.0, (datetime.now(timezone.utc) - produced_at.astimezone(timezone.utc)).total_seconds())
    except ValueError:
        lag_seconds = 0.0
    metrics.inc_counter("export_dispatch_total", labels={"market": intent.market, "source": intent.source})
    metrics.set_gauge("export_stage_lag_seconds", lag_seconds, {"worker": "export_dispatch"})
    logger.info(
        "export_intent_staged",
        extra={
            "event": "export_intent_staged",
            "export_id": intent.export_id,
            "target_relpath": intent.target_relpath,
            "staged_blob_path": staged_path,
            "content_length": relay.content_length,
        },
    )
    return relay
