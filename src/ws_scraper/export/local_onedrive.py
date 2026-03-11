from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from ws_scraper.auth.identity import LocalGraphAuthManager


GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"


def normalize_onedrive_folder(value: str) -> str:
    raw = str(value or "").replace("\\", "/").strip()
    parts = [part.strip() for part in raw.split("/") if part.strip()]
    return "/" + "/".join(parts) if parts else ""


class PersonalOneDriveRelayUploader:
    def __init__(
        self,
        auth_manager: LocalGraphAuthManager,
        onedrive_folder: str,
        timeout_seconds: float = 60.0,
        chunk_size_bytes: int = 5 * 1024 * 1024,
        logger=None,
    ):
        self.auth_manager = auth_manager
        self.onedrive_folder = normalize_onedrive_folder(onedrive_folder)
        self.timeout_seconds = max(3.0, float(timeout_seconds))
        self.chunk_size_bytes = max(320 * 1024, int(chunk_size_bytes))
        self.logger = logger

    def _request(
        self,
        method: str,
        url: str,
        *,
        expected: tuple[int, ...],
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        data=None,
    ):
        token = self.auth_manager.acquire_access_token()
        req_headers = {"Authorization": f"Bearer {token}"}
        if headers:
            req_headers.update(headers)
        response = requests.request(
            method=method,
            url=url,
            headers=req_headers,
            json=json_body,
            data=data,
            timeout=self.timeout_seconds,
        )
        if response.status_code not in expected:
            detail = str(response.text or "").strip().replace("\n", " ")
            if len(detail) > 300:
                detail = detail[:300] + "..."
            raise RuntimeError(f"graph_http_{response.status_code}:{detail}")
        return response

    def _drive_path(self, relpath: str) -> str:
        cleaned = str(relpath or "").replace("\\", "/").strip("/")
        base = self.onedrive_folder.strip("/")
        return "/".join(part for part in (base, cleaned) if part)

    def _ensure_folder(self, rel_folder: str) -> None:
        current = ""
        for segment in [part for part in rel_folder.split("/") if part]:
            parent = current
            current = "/".join(part for part in (current, segment) if part)
            encoded_current = quote(current, safe="/")
            get_url = f"{GRAPH_API_BASE}/me/drive/root:/{encoded_current}"
            response = self._request("GET", get_url, expected=(200, 404))
            if response.status_code == 200:
                continue
            if parent:
                encoded_parent = quote(parent, safe="/")
                create_url = f"{GRAPH_API_BASE}/me/drive/root:/{encoded_parent}:/children"
            else:
                create_url = f"{GRAPH_API_BASE}/me/drive/root/children"
            self._request(
                "POST",
                create_url,
                expected=(200, 201),
                json_body={
                    "name": segment,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "replace",
                },
            )

    def _upload_small(self, drive_path: str, payload: bytes) -> None:
        encoded = quote(drive_path, safe="/")
        url = f"{GRAPH_API_BASE}/me/drive/root:/{encoded}:/content"
        self._request(
            "PUT",
            url,
            expected=(200, 201),
            headers={"Content-Type": "application/octet-stream"},
            data=payload,
        )

    def _upload_large(self, drive_path: str, payload: bytes) -> None:
        encoded = quote(drive_path, safe="/")
        session_url = f"{GRAPH_API_BASE}/me/drive/root:/{encoded}:/createUploadSession"
        response = self._request(
            "POST",
            session_url,
            expected=(200,),
            json_body={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
        )
        upload_url = str(response.json().get("uploadUrl") or "").strip()
        if not upload_url:
            raise RuntimeError("graph_upload_session_missing_upload_url")
        total = len(payload)
        offset = 0
        while offset < total:
            chunk = payload[offset : offset + self.chunk_size_bytes]
            start = offset
            end = offset + len(chunk) - 1
            headers = {
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {start}-{end}/{total}",
            }
            response = requests.put(upload_url, headers=headers, data=chunk, timeout=self.timeout_seconds)
            if response.status_code not in (200, 201, 202):
                detail = str(response.text or "").strip().replace("\n", " ")
                if len(detail) > 300:
                    detail = detail[:300] + "..."
                raise RuntimeError(f"graph_upload_chunk_failed:{response.status_code}:{detail}")
            offset += len(chunk)

    def upload_bytes(self, relpath: str, payload: bytes) -> None:
        drive_path = self._drive_path(relpath)
        parent = str(Path(drive_path).parent).replace("\\", "/").strip(".")
        if parent and parent != "/":
            self._ensure_folder(parent.strip("/"))
        if len(payload) <= 4 * 1024 * 1024:
            self._upload_small(drive_path, payload)
        else:
            self._upload_large(drive_path, payload)
