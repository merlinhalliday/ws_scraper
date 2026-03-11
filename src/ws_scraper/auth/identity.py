from __future__ import annotations

import threading
from pathlib import Path
from typing import Optional

from ws_scraper.pipeline.runtime import IdentitySettings

try:
    from azure.identity import ClientSecretCredential, DefaultAzureCredential
except Exception:
    ClientSecretCredential = None
    DefaultAzureCredential = None

try:
    import msal
except Exception:
    msal = None


class AzureCredentialFactory:
    @staticmethod
    def build(settings: IdentitySettings):
        settings.validate_cloud_safe()
        if settings.identity_mode == "managed_identity":
            if DefaultAzureCredential is None:
                raise RuntimeError("missing_dependency:azure-identity")
            return DefaultAzureCredential(exclude_interactive_browser_credential=True)
        if settings.identity_mode == "client_secret":
            if ClientSecretCredential is None:
                raise RuntimeError("missing_dependency:azure-identity")
            return ClientSecretCredential(
                tenant_id=settings.tenant_id,
                client_id=settings.client_id,
                client_secret=settings.client_secret,
            )
        raise RuntimeError("delegated_local_identity_has_no_azure_service_credential")


class LocalGraphAuthManager:
    def __init__(
        self,
        client_id: str,
        authority: str,
        scopes: tuple[str, ...],
        cache_path: Path,
        logger=None,
    ):
        if msal is None:
            raise RuntimeError("missing_dependency:msal")
        self.client_id = str(client_id or "").strip()
        self.authority = str(authority or "").strip()
        self.scopes = tuple(scopes)
        self.cache_path = Path(cache_path)
        self.logger = logger
        self._lock = threading.Lock()
        self._cache = msal.SerializableTokenCache()
        if self.cache_path.exists():
            try:
                payload = self.cache_path.read_text(encoding="utf-8")
                if payload.strip():
                    self._cache.deserialize(payload)
            except Exception:
                pass
        self._app = msal.PublicClientApplication(
            client_id=self.client_id,
            authority=self.authority,
            token_cache=self._cache,
        )

    def _persist_cache_unlocked(self) -> None:
        if not self._cache.has_state_changed:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._cache.serialize()
        temp_path = self.cache_path.with_suffix(self.cache_path.suffix + ".tmp")
        temp_path.write_text(payload, encoding="utf-8")
        temp_path.replace(self.cache_path)

    def _log(self, message: str) -> None:
        if self.logger is not None:
            self.logger.info(message, extra={"event": "graph_device_flow"})
        else:
            print(message)

    def acquire_access_token(self) -> str:
        with self._lock:
            accounts = self._app.get_accounts()
            for account in accounts:
                result = self._app.acquire_token_silent(scopes=list(self.scopes), account=account)
                if isinstance(result, dict) and result.get("access_token"):
                    self._persist_cache_unlocked()
                    return str(result.get("access_token"))
            flow = self._app.initiate_device_flow(scopes=list(self.scopes))
            if not isinstance(flow, dict) or not flow.get("user_code"):
                raise RuntimeError(f"graph_device_flow_init_failed:{flow}")
            message = str(flow.get("message") or "").strip()
        if message:
            self._log(message)
        result = self._app.acquire_token_by_device_flow(flow)
        token = str((result or {}).get("access_token") or "").strip()
        if not token:
            detail = str((result or {}).get("error_description") or (result or {}).get("error") or "unknown_error")
            raise RuntimeError(f"graph_device_flow_failed:{detail}")
        with self._lock:
            self._persist_cache_unlocked()
        return token


def build_local_graph_auth(settings: IdentitySettings, logger=None) -> LocalGraphAuthManager:
    settings.validate_cloud_safe()
    if settings.identity_mode != "delegated_local":
        raise RuntimeError("local_graph_auth_requires_delegated_local_identity")
    return LocalGraphAuthManager(
        client_id=settings.graph_client_id,
        authority=settings.graph_authority,
        scopes=settings.graph_scopes,
        cache_path=settings.graph_token_cache_path,
        logger=logger,
    )
