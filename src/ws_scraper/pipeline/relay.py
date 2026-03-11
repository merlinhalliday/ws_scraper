from __future__ import annotations

from datetime import datetime, timezone

from ws_scraper.pipeline.contracts import RelayIntent, sha256_hex


def process_relay_intent(
    *,
    blob_store,
    export_container: str,
    ledger,
    relay_message: dict[str, object],
    uploader,
    logger,
    metrics,
) -> RelayIntent:
    relay = RelayIntent.from_dict(dict(relay_message))
    if not relay.export_id:
        raise RuntimeError("relay_intent_missing_export_id")

    ledger_key = f"relay/{relay.export_id}"
    if ledger.exists(ledger_key):
        metrics.inc_counter("relay_duplicate_total")
        return RelayIntent.from_dict(ledger.read(ledger_key) or {})

    payload = blob_store.download_bytes(export_container, relay.staged_blob_path)
    content_sha256 = sha256_hex(payload)
    if relay.content_sha256 and relay.content_sha256 != content_sha256:
        raise RuntimeError(
            f"relay_checksum_mismatch:expected={relay.content_sha256},actual={content_sha256},export_id={relay.export_id}"
        )
    uploader.upload_bytes(relay.target_relpath, payload)
    ledger.claim(
        ledger_key,
        {
            **relay.to_dict(),
            "uploaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        },
    )
    lag_seconds = 0.0
    try:
        produced_at = datetime.fromisoformat(relay.produced_at.replace("Z", "+00:00"))
        if produced_at.tzinfo is None:
            produced_at = produced_at.replace(tzinfo=timezone.utc)
        lag_seconds = max(0.0, (datetime.now(timezone.utc) - produced_at.astimezone(timezone.utc)).total_seconds())
    except ValueError:
        lag_seconds = 0.0
    metrics.inc_counter("relay_upload_total")
    metrics.set_gauge("relay_lag_seconds", lag_seconds, {"worker": "local_relay"})
    logger.info(
        "relay_upload_complete",
        extra={
            "event": "relay_upload_complete",
            "export_id": relay.export_id,
            "target_relpath": relay.target_relpath,
            "content_length": relay.content_length,
        },
    )
    return relay
