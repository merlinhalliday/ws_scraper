from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class BronzeWritten:
    batch_id: str
    bronze_blob_path: str
    source: str
    market: str
    snapshot_date: str
    content_sha256: str
    row_count: int
    produced_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BronzeWritten":
        return cls(
            batch_id=str(payload.get("batch_id") or "").strip(),
            bronze_blob_path=str(payload.get("bronze_blob_path") or "").strip(),
            source=str(payload.get("source") or "").strip(),
            market=str(payload.get("market") or "").strip(),
            snapshot_date=str(payload.get("snapshot_date") or "").strip(),
            content_sha256=str(payload.get("content_sha256") or "").strip(),
            row_count=int(payload.get("row_count") or 0),
            produced_at=str(payload.get("produced_at") or "").strip(),
        )


@dataclass(frozen=True)
class ExportIntent:
    export_id: str
    silver_blob_path: str
    target_relpath: str
    content_sha256: str
    content_length: int
    produced_at: str
    snapshot_date: str
    source: str
    market: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ExportIntent":
        return cls(
            export_id=str(payload.get("export_id") or "").strip(),
            silver_blob_path=str(payload.get("silver_blob_path") or "").strip(),
            target_relpath=str(payload.get("target_relpath") or "").strip(),
            content_sha256=str(payload.get("content_sha256") or "").strip(),
            content_length=int(payload.get("content_length") or 0),
            produced_at=str(payload.get("produced_at") or "").strip(),
            snapshot_date=str(payload.get("snapshot_date") or "").strip(),
            source=str(payload.get("source") or "").strip(),
            market=str(payload.get("market") or "").strip(),
        )


@dataclass(frozen=True)
class RelayIntent:
    export_id: str
    staged_blob_path: str
    target_relpath: str
    content_sha256: str
    content_length: int
    produced_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RelayIntent":
        return cls(
            export_id=str(payload.get("export_id") or "").strip(),
            staged_blob_path=str(payload.get("staged_blob_path") or "").strip(),
            target_relpath=str(payload.get("target_relpath") or "").strip(),
            content_sha256=str(payload.get("content_sha256") or "").strip(),
            content_length=int(payload.get("content_length") or 0),
            produced_at=str(payload.get("produced_at") or "").strip(),
        )
