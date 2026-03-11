from __future__ import annotations

import json
import os
import queue
import signal
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(minimum, value)


def env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = (os.getenv(name) or "").strip()
    try:
        value = float(raw) if raw else default
    except ValueError:
        value = default
    return max(minimum, value)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


class Shutdown:
    """Signal-aware shutdown helper for graceful worker exits."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def install(self) -> None:
        def _handler(signum: int, _frame: Any) -> None:
            print(f"received_signal={signum}; requesting shutdown")
            self._event.set()

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def requested(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout_s: float) -> bool:
        return self._event.wait(timeout_s)


class JsonCheckpoint:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save(self, payload: dict[str, Any]) -> None:
        temp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, separators=(",", ":"), default=str), encoding="utf-8")
        temp_path.replace(self.path)


@dataclass
class QueueEnvelope:
    body: dict[str, Any]
    receipt: Any


class AbstractBatchQueue:
    def publish(self, payload: dict[str, Any]) -> None:
        raise NotImplementedError

    def receive(self, max_messages: int, visibility_timeout: int, wait_seconds: int) -> list[QueueEnvelope]:
        raise NotImplementedError

    def ack(self, receipt: Any) -> None:
        raise NotImplementedError


class LocalInMemoryQueue(AbstractBatchQueue):
    """Simple queue for local dev/testing when Azure is not configured."""

    def __init__(self) -> None:
        self._q: queue.Queue[dict[str, Any]] = queue.Queue()

    def publish(self, payload: dict[str, Any]) -> None:
        self._q.put(payload)

    def receive(self, max_messages: int, visibility_timeout: int, wait_seconds: int) -> list[QueueEnvelope]:
        del visibility_timeout
        out: list[QueueEnvelope] = []
        deadline = time.time() + max(0, wait_seconds)
        while len(out) < max_messages:
            timeout = max(0.0, deadline - time.time())
            if wait_seconds <= 0 and out:
                timeout = 0.0
            try:
                msg = self._q.get(timeout=timeout if out else max(0.05, timeout))
            except queue.Empty:
                break
            out.append(QueueEnvelope(body=msg, receipt=None))
            if wait_seconds <= 0:
                continue
        return out

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

    def publish(self, payload: dict[str, Any]) -> None:
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


class AzureServiceBusQueue(AbstractBatchQueue):
    def __init__(self, connection_string: str, queue_name: str) -> None:
        from azure.servicebus import ServiceBusClient

        self._client = ServiceBusClient.from_connection_string(connection_string)
        self._sender = self._client.get_queue_sender(queue_name=queue_name)
        self._receiver = self._client.get_queue_receiver(queue_name=queue_name)

    def publish(self, payload: dict[str, Any]) -> None:
        from azure.servicebus import ServiceBusMessage

        self._sender.send_messages(ServiceBusMessage(json.dumps(payload, separators=(",", ":"), default=str)))

    def receive(self, max_messages: int, visibility_timeout: int, wait_seconds: int) -> list[QueueEnvelope]:
        del visibility_timeout
        messages = self._receiver.receive_messages(max_message_count=max_messages, max_wait_time=wait_seconds)
        return [QueueEnvelope(body=json.loads(str(m)), receipt=m) for m in messages]

    def ack(self, receipt: Any) -> None:
        self._receiver.complete_message(receipt)


def build_queue_from_env() -> AbstractBatchQueue:
    backend = (os.getenv("WORKER_QUEUE_BACKEND") or "azure-storage").strip().lower()
    if backend == "memory":
        return LocalInMemoryQueue()

    queue_name = (os.getenv("WORKER_QUEUE_NAME") or "ws-snapshot-batches").strip()
    if backend == "servicebus":
        conn = (os.getenv("AZURE_SERVICEBUS_CONNECTION_STRING") or "").strip()
        if not conn:
            raise RuntimeError("missing AZURE_SERVICEBUS_CONNECTION_STRING for servicebus backend")
        return AzureServiceBusQueue(conn, queue_name)

    conn = (os.getenv("AZURE_STORAGE_QUEUE_CONNECTION_STRING") or "").strip()
    if not conn:
        raise RuntimeError("missing AZURE_STORAGE_QUEUE_CONNECTION_STRING for azure-storage backend")
    return AzureStorageQueue(conn, queue_name)


class BlobSink:
    def __init__(self, connection_string: str, container: str) -> None:
        from azure.storage.blob import BlobServiceClient

        svc = BlobServiceClient.from_connection_string(connection_string)
        self._container = svc.get_container_client(container)
        try:
            self._container.create_container()
        except Exception:
            pass

    def write_bytes(self, blob_path: str, payload: bytes) -> None:
        self._container.upload_blob(name=blob_path, data=payload, overwrite=False)
