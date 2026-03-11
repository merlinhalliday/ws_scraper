from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

from ws_scraper.auth.identity import AzureCredentialFactory
from ws_scraper.pipeline.runtime import BlobSettings, IdentitySettings, QueueSettings

try:
    from azure.core.exceptions import ResourceExistsError
except Exception:
    ResourceExistsError = Exception


@dataclass
class QueueEnvelope:
    body: dict[str, Any]
    receipt: Any


class AbstractBatchQueue:
    def publish(self, payload: dict[str, Any], message_id: str | None = None) -> None:
        raise NotImplementedError

    def receive(self, max_messages: int, visibility_timeout: int, wait_seconds: int) -> list[QueueEnvelope]:
        raise NotImplementedError

    def ack(self, receipt: Any) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return None


class MemoryQueue(AbstractBatchQueue):
    _state: dict[str, list[dict[str, Any]]] = {}
    _lock = threading.Lock()

    def __init__(self, name: str):
        self.name = name
        with self._lock:
            self._state.setdefault(self.name, [])

    def publish(self, payload: dict[str, Any], message_id: str | None = None) -> None:
        del message_id
        with self._lock:
            self._state[self.name].append(dict(payload))

    def receive(self, max_messages: int, visibility_timeout: int, wait_seconds: int) -> list[QueueEnvelope]:
        del visibility_timeout
        deadline = time.time() + max(0, wait_seconds)
        while True:
            with self._lock:
                items = self._state[self.name][:max_messages]
                self._state[self.name] = self._state[self.name][len(items) :]
            if items:
                return [QueueEnvelope(body=item, receipt=item) for item in items]
            if time.time() >= deadline:
                return []
            time.sleep(0.05)

    def ack(self, receipt: Any) -> None:
        del receipt


class AzureStorageQueue(AbstractBatchQueue):
    def __init__(self, connection_string: str, queue_name: str) -> None:
        from azure.storage.queue import QueueClient

        self._client = QueueClient.from_connection_string(connection_string, queue_name)
        try:
            self._client.create_queue()
        except Exception:
            pass

    def publish(self, payload: dict[str, Any], message_id: str | None = None) -> None:
        del message_id
        self._client.send_message(json.dumps(payload, separators=(",", ":"), default=str))

    def receive(self, max_messages: int, visibility_timeout: int, wait_seconds: int) -> list[QueueEnvelope]:
        messages = self._client.receive_messages(
            messages_per_page=max_messages,
            visibility_timeout=visibility_timeout,
        ).by_page(results_per_page=max_messages)
        out: list[QueueEnvelope] = []
        for page in messages:
            for message in page:
                out.append(QueueEnvelope(body=json.loads(message.content), receipt=message))
                if len(out) >= max_messages:
                    return out
            break
        if not out and wait_seconds > 0:
            time.sleep(wait_seconds)
        return out

    def ack(self, receipt: Any) -> None:
        self._client.delete_message(receipt.id, receipt.pop_receipt)


class ServiceBusQueue(AbstractBatchQueue):
    def __init__(self, queue_name: str, settings: QueueSettings, identity: IdentitySettings) -> None:
        from azure.servicebus import ServiceBusClient

        if settings.servicebus_connection_string:
            self._client = ServiceBusClient.from_connection_string(settings.servicebus_connection_string)
        else:
            credential = AzureCredentialFactory.build(identity)
            self._client = ServiceBusClient(
                fully_qualified_namespace=settings.servicebus_fqdn,
                credential=credential,
            )
        self._sender = self._client.get_queue_sender(queue_name=queue_name)
        self._receiver = self._client.get_queue_receiver(queue_name=queue_name)

    def publish(self, payload: dict[str, Any], message_id: str | None = None) -> None:
        from azure.servicebus import ServiceBusMessage

        kwargs = {"message_id": message_id} if message_id else {}
        self._sender.send_messages(
            ServiceBusMessage(
                json.dumps(payload, separators=(",", ":"), default=str),
                content_type="application/json",
                **kwargs,
            )
        )

    def receive(self, max_messages: int, visibility_timeout: int, wait_seconds: int) -> list[QueueEnvelope]:
        del visibility_timeout
        messages = self._receiver.receive_messages(max_message_count=max_messages, max_wait_time=wait_seconds)
        out: list[QueueEnvelope] = []
        for message in messages:
            body = b"".join(bytes(section) for section in message.body)
            out.append(QueueEnvelope(body=json.loads(body.decode("utf-8")), receipt=message))
        return out

    def ack(self, receipt: Any) -> None:
        self._receiver.complete_message(receipt)

    def close(self) -> None:
        try:
            self._sender.close()
        finally:
            try:
                self._receiver.close()
            finally:
                self._client.close()


def build_queue(queue_name: str, settings: QueueSettings, identity: IdentitySettings) -> AbstractBatchQueue:
    if settings.backend == "memory":
        return MemoryQueue(queue_name)
    if settings.backend == "azure-storage":
        return AzureStorageQueue(settings.storage_queue_connection_string, queue_name)
    return ServiceBusQueue(queue_name, settings=settings, identity=identity)


class BlobStore:
    def __init__(self, settings: BlobSettings, identity: IdentitySettings) -> None:
        from azure.storage.blob import BlobServiceClient

        if settings.connection_string:
            self._service = BlobServiceClient.from_connection_string(settings.connection_string)
        else:
            credential = AzureCredentialFactory.build(identity)
            self._service = BlobServiceClient(account_url=settings.account_url, credential=credential)
        self._containers: dict[str, Any] = {}

    def container(self, name: str):
        if name not in self._containers:
            client = self._service.get_container_client(name)
            try:
                client.create_container()
            except Exception:
                pass
            self._containers[name] = client
        return self._containers[name]

    def upload_bytes(
        self,
        container: str,
        blob_name: str,
        payload: bytes,
        *,
        overwrite: bool = False,
        content_type: str | None = None,
    ) -> None:
        kwargs: dict[str, Any] = {"overwrite": overwrite}
        if content_type:
            from azure.storage.blob import ContentSettings

            kwargs["content_settings"] = ContentSettings(content_type=content_type)
        self.container(container).upload_blob(name=blob_name, data=payload, **kwargs)

    def upload_bytes_if_absent(
        self,
        container: str,
        blob_name: str,
        payload: bytes,
        *,
        content_type: str | None = None,
    ) -> bool:
        try:
            self.upload_bytes(container, blob_name, payload, overwrite=False, content_type=content_type)
            return True
        except ResourceExistsError:
            return False

    def upload_json_if_absent(self, container: str, blob_name: str, payload: dict[str, Any]) -> bool:
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        return self.upload_bytes_if_absent(container, blob_name, body, content_type="application/json")

    def upload_json(self, container: str, blob_name: str, payload: dict[str, Any], *, overwrite: bool = False) -> None:
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        self.upload_bytes(container, blob_name, body, overwrite=overwrite, content_type="application/json")

    def exists(self, container: str, blob_name: str) -> bool:
        try:
            return self.container(container).get_blob_client(blob_name).exists()
        except Exception:
            return False

    def download_bytes(self, container: str, blob_name: str) -> bytes:
        return self.container(container).download_blob(blob_name).readall()

    def download_json(self, container: str, blob_name: str) -> dict[str, Any]:
        raw = self.download_bytes(container, blob_name)
        return json.loads(raw.decode("utf-8"))

    def list_blob_names(self, container: str, prefix: str = "") -> list[str]:
        names: list[str] = []
        for blob in self.container(container).list_blobs(name_starts_with=prefix):
            names.append(str(blob.name))
        return sorted(names)


class InMemoryBlobStore:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], bytes] = {}

    def upload_bytes(
        self,
        container: str,
        blob_name: str,
        payload: bytes,
        *,
        overwrite: bool = False,
        content_type: str | None = None,
    ) -> None:
        del content_type
        key = (container, blob_name)
        if not overwrite and key in self._items:
            raise ResourceExistsError("blob exists")
        self._items[key] = bytes(payload)

    def upload_bytes_if_absent(
        self,
        container: str,
        blob_name: str,
        payload: bytes,
        *,
        content_type: str | None = None,
    ) -> bool:
        try:
            self.upload_bytes(container, blob_name, payload, overwrite=False, content_type=content_type)
            return True
        except ResourceExistsError:
            return False

    def upload_json_if_absent(self, container: str, blob_name: str, payload: dict[str, Any]) -> bool:
        return self.upload_bytes_if_absent(container, blob_name, json.dumps(payload).encode("utf-8"))

    def upload_json(self, container: str, blob_name: str, payload: dict[str, Any], *, overwrite: bool = False) -> None:
        self.upload_bytes(container, blob_name, json.dumps(payload).encode("utf-8"), overwrite=overwrite)

    def exists(self, container: str, blob_name: str) -> bool:
        return (container, blob_name) in self._items

    def download_bytes(self, container: str, blob_name: str) -> bytes:
        return self._items[(container, blob_name)]

    def download_json(self, container: str, blob_name: str) -> dict[str, Any]:
        return json.loads(self.download_bytes(container, blob_name).decode("utf-8"))

    def list_blob_names(self, container: str, prefix: str = "") -> list[str]:
        names = [name for current_container, name in self._items if current_container == container and name.startswith(prefix)]
        return sorted(names)


class BlobLedger:
    def __init__(self, blob_store, container: str, prefix: str):
        self.blob_store = blob_store
        self.container = container
        self.prefix = prefix.strip("/").replace("\\", "/")

    def _name(self, key: str) -> str:
        cleaned = str(key or "").strip().replace("\\", "/").strip("/")
        return f"{self.prefix}/{cleaned}.json" if self.prefix else f"{cleaned}.json"

    def exists(self, key: str) -> bool:
        return self.blob_store.exists(self.container, self._name(key))

    def claim(self, key: str, payload: dict[str, Any]) -> bool:
        return self.blob_store.upload_json_if_absent(self.container, self._name(key), payload)

    def read(self, key: str) -> Optional[dict[str, Any]]:
        if not self.exists(key):
            return None
        return self.blob_store.download_json(self.container, self._name(key))
