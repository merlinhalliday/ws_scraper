from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ws_scraper.app.config import APP_ROOT


def env_text(name: str, default: str = "") -> str:
    return str(os.getenv(name) or default).strip()


def env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = env_text(name)
    try:
        value = int(raw) if raw else int(default)
    except ValueError:
        value = int(default)
    return max(minimum, value)


def env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = env_text(name)
    try:
        value = float(raw) if raw else float(default)
    except ValueError:
        value = float(default)
    return max(minimum, value)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def env_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = env_text(name)
    if not raw:
        return default
    parts = [part.strip() for part in raw.replace(",", " ").split() if part.strip()]
    return tuple(parts) if parts else default


@dataclass(frozen=True)
class IdentitySettings:
    app_env: str
    identity_mode: str
    tenant_id: str
    client_id: str
    client_secret: str
    graph_client_id: str
    graph_authority: str
    graph_scopes: tuple[str, ...]
    graph_token_cache_path: Path

    def validate_cloud_safe(self) -> None:
        if self.app_env == "cloud" and self.identity_mode == "delegated_local":
            raise RuntimeError("delegated_local_identity_not_allowed_in_cloud")
        if self.identity_mode == "client_secret":
            missing = [
                name
                for name, value in (
                    ("AZURE_TENANT_ID", self.tenant_id),
                    ("AZURE_CLIENT_ID", self.client_id),
                    ("AZURE_CLIENT_SECRET", self.client_secret),
                )
                if not value
            ]
            if missing:
                raise RuntimeError(f"missing_client_secret_identity_env:{','.join(missing)}")
        if self.identity_mode == "delegated_local":
            missing = [
                name
                for name, value in (
                    ("GRAPH_CLIENT_ID", self.graph_client_id),
                    ("GRAPH_AUTHORITY", self.graph_authority),
                    ("GRAPH_SCOPES", " ".join(self.graph_scopes)),
                )
                if not value
            ]
            if missing:
                raise RuntimeError(f"missing_graph_local_identity_env:{','.join(missing)}")


@dataclass(frozen=True)
class BlobSettings:
    connection_string: str
    account_url: str
    raw_container: str
    silver_container: str
    export_container: str
    ledger_container: str

    def validate(self) -> None:
        if not (self.connection_string or self.account_url):
            raise RuntimeError("missing_blob_storage_connection_or_account_url")


@dataclass(frozen=True)
class QueueSettings:
    backend: str
    servicebus_connection_string: str
    servicebus_fqdn: str
    storage_queue_connection_string: str
    ingest_queue_name: str
    transform_queue_name: str
    export_queue_name: str
    relay_queue_name: str

    def validate(self) -> None:
        if self.backend == "servicebus" and not (self.servicebus_connection_string or self.servicebus_fqdn):
            raise RuntimeError("missing_servicebus_connection_or_fqdn")
        if self.backend == "azure-storage" and not self.storage_queue_connection_string:
            raise RuntimeError("missing_azure_storage_queue_connection_string")


@dataclass(frozen=True)
class ObservabilitySettings:
    enable_http: bool
    host: str
    port: int
    heartbeat_timeout_seconds: int
    log_level: str


@dataclass(frozen=True)
class ExportSettings:
    export_mode: str
    onedrive_folder: str
    graph_timeout_seconds: float
    graph_chunk_size_bytes: int

    def validate(self, identity: IdentitySettings) -> None:
        if self.export_mode == "personal_onedrive_local":
            if identity.app_env != "local":
                raise RuntimeError("personal_onedrive_local_requires_app_env_local")
            if identity.identity_mode != "delegated_local":
                raise RuntimeError("personal_onedrive_local_requires_delegated_local_identity")
            if not self.onedrive_folder:
                raise RuntimeError("missing_onedrive_folder")


@dataclass(frozen=True)
class PipelineSettings:
    identity: IdentitySettings
    blob: BlobSettings
    queues: QueueSettings
    observability: ObservabilitySettings
    export: ExportSettings
    checkpoint_root: Path
    output_root: Path
    silver_partition_by_hour: bool

    @classmethod
    def from_env(cls) -> "PipelineSettings":
        app_env = env_text("APP_ENV", "cloud").lower() or "cloud"
        identity_mode = env_text("IDENTITY_MODE", "managed_identity").lower() or "managed_identity"
        if identity_mode not in {"managed_identity", "client_secret", "delegated_local"}:
            raise RuntimeError(f"invalid_identity_mode:{identity_mode}")

        queue_backend = env_text("QUEUE_BACKEND", env_text("WORKER_QUEUE_BACKEND", "servicebus")).lower()
        if queue_backend not in {"servicebus", "azure-storage", "memory"}:
            raise RuntimeError(f"invalid_queue_backend:{queue_backend}")

        export_mode = env_text("EXPORT_MODE", "blob_relay").lower() or "blob_relay"
        if export_mode not in {"blob_relay", "personal_onedrive_local"}:
            raise RuntimeError(f"invalid_export_mode:{export_mode}")

        identity = IdentitySettings(
            app_env=app_env,
            identity_mode=identity_mode,
            tenant_id=env_text("AZURE_TENANT_ID"),
            client_id=env_text("AZURE_CLIENT_ID"),
            client_secret=env_text("AZURE_CLIENT_SECRET"),
            graph_client_id=env_text("GRAPH_CLIENT_ID"),
            graph_authority=env_text("GRAPH_AUTHORITY", "https://login.microsoftonline.com/consumers"),
            graph_scopes=env_list("GRAPH_SCOPES", ("Files.ReadWrite",)),
            graph_token_cache_path=Path(env_text("GRAPH_TOKEN_CACHE_PATH", str(APP_ROOT / ".graph_token_cache.bin"))),
        )
        blob = BlobSettings(
            connection_string=env_text("AZURE_BLOB_CONNECTION_STRING"),
            account_url=env_text("AZURE_BLOB_ACCOUNT_URL"),
            raw_container=env_text("AZURE_BLOB_RAW_CONTAINER", env_text("AZURE_BLOB_CONTAINER", "ws-snapshots-raw")),
            silver_container=env_text("AZURE_BLOB_SILVER_CONTAINER", "ws-snapshots-silver"),
            export_container=env_text("AZURE_BLOB_EXPORT_CONTAINER", "ws-snapshots-export"),
            ledger_container=env_text("AZURE_BLOB_LEDGER_CONTAINER", "ws-snapshots-ledger"),
        )
        queues = QueueSettings(
            backend=queue_backend,
            servicebus_connection_string=env_text("AZURE_SERVICEBUS_CONNECTION_STRING"),
            servicebus_fqdn=env_text("AZURE_SERVICEBUS_FQDN"),
            storage_queue_connection_string=env_text("AZURE_STORAGE_QUEUE_CONNECTION_STRING"),
            ingest_queue_name=env_text("INGEST_QUEUE_NAME", env_text("WORKER_QUEUE_NAME", "ws-snapshot-batches")),
            transform_queue_name=env_text("TRANSFORM_QUEUE_NAME", "ws-bronze-written"),
            export_queue_name=env_text("EXPORT_QUEUE_NAME", "ws-export-intents"),
            relay_queue_name=env_text("RELAY_QUEUE_NAME", "ws-relay-intents"),
        )
        observability = ObservabilitySettings(
            enable_http=env_bool("OBS_ENABLE_HTTP", True),
            host=env_text("OBS_HOST", "0.0.0.0"),
            port=env_int("OBS_PORT", 8080, minimum=1),
            heartbeat_timeout_seconds=env_int("WORKER_HEARTBEAT_TIMEOUT_SECONDS", 120, minimum=5),
            log_level=env_text("LOG_LEVEL", "INFO").upper(),
        )
        export = ExportSettings(
            export_mode=export_mode,
            onedrive_folder=env_text("ONEDRIVE_FOLDER", "/ws-snapshots"),
            graph_timeout_seconds=env_float("GRAPH_UPLOAD_TIMEOUT_SECONDS", 60.0, minimum=3.0),
            graph_chunk_size_bytes=env_int("GRAPH_UPLOAD_CHUNK_SIZE_BYTES", 5 * 1024 * 1024, minimum=320 * 1024),
        )

        settings = cls(
            identity=identity,
            blob=blob,
            queues=queues,
            observability=observability,
            export=export,
            checkpoint_root=Path(env_text("WORKER_CHECKPOINT_ROOT", str(APP_ROOT / "outputs" / "worker_state"))),
            output_root=Path(env_text("OUTPUT_ROOT", str(APP_ROOT / "outputs" / "ws_snapshots"))),
            silver_partition_by_hour=env_bool("SILVER_PARTITION_BY_HOUR", True),
        )
        settings.identity.validate_cloud_safe()
        settings.queues.validate()
        settings.export.validate(settings.identity)
        return settings
