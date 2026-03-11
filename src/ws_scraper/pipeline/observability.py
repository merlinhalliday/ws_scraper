from __future__ import annotations

import json
import logging
import re
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from ws_scraper.pipeline.runtime import ObservabilitySettings


_BASE_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__.keys())
_METRIC_NAME_RE = re.compile(r"[^a-zA-Z0-9_:]")
_LABEL_NAME_RE = re.compile(r"[^a-zA-Z0-9_]")


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _BASE_RECORD_FIELDS or key in {"message", "asctime"}:
                continue
            payload[key] = value
        return json.dumps(payload, separators=(",", ":"), default=str)


def configure_json_logging(worker_name: str, level: str = "INFO") -> logging.Logger:
    root = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root.handlers = [handler]
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger = logging.getLogger(worker_name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger.log(level, event, extra={"event": event, **fields})


def _metric_key(name: str, labels: dict[str, str] | None) -> tuple[str, tuple[tuple[str, str], ...]]:
    safe_name = _METRIC_NAME_RE.sub("_", name)
    label_items = tuple(sorted(((_LABEL_NAME_RE.sub("_", key), str(value)) for key, value in (labels or {}).items())))
    return safe_name, label_items


class MetricRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: dict[tuple[str, tuple[tuple[str, str], ...]], tuple[str, float]] = {}

    def inc_counter(self, name: str, amount: float = 1.0, labels: dict[str, str] | None = None) -> None:
        key = _metric_key(name, labels)
        with self._lock:
            kind, current = self._values.get(key, ("counter", 0.0))
            self._values[key] = (kind, current + float(amount))

    def set_gauge(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        key = _metric_key(name, labels)
        with self._lock:
            self._values[key] = ("gauge", float(value))

    def render_prometheus(self) -> str:
        lines: list[str] = []
        with self._lock:
            items = sorted(self._values.items())
        for (name, labels), (kind, value) in items:
            if kind == "counter":
                lines.append(f"# TYPE {name} counter")
            else:
                lines.append(f"# TYPE {name} gauge")
            if labels:
                encoded = ",".join(f'{key}="{value}"' for key, value in labels)
                lines.append(f"{name}{{{encoded}}} {value}")
            else:
                lines.append(f"{name} {value}")
        return "\n".join(lines) + ("\n" if lines else "")


class HealthState:
    def __init__(self, worker_name: str, stale_after_seconds: int):
        self.worker_name = worker_name
        self.stale_after_seconds = max(1, int(stale_after_seconds))
        self._lock = threading.Lock()
        self._ready = False
        self._healthy = True
        self._reason = ""
        self._last_heartbeat = time.time()

    def heartbeat(self) -> None:
        with self._lock:
            self._last_heartbeat = time.time()

    def set_ready(self, ready: bool = True) -> None:
        with self._lock:
            self._ready = bool(ready)

    def set_unhealthy(self, reason: str) -> None:
        with self._lock:
            self._healthy = False
            self._reason = str(reason)

    def set_healthy(self) -> None:
        with self._lock:
            self._healthy = True
            self._reason = ""

    def health_payload(self) -> tuple[int, dict[str, Any]]:
        with self._lock:
            last_heartbeat = self._last_heartbeat
            ready = self._ready
            healthy = self._healthy
            reason = self._reason
        age_seconds = max(0.0, time.time() - last_heartbeat)
        timed_out = age_seconds > self.stale_after_seconds
        ok = healthy and not timed_out
        status = HTTPStatus.OK if ok else HTTPStatus.SERVICE_UNAVAILABLE
        payload = {
            "worker": self.worker_name,
            "healthy": ok,
            "ready": ready,
            "reason": "heartbeat_timeout" if timed_out else reason,
            "heartbeat_age_seconds": round(age_seconds, 3),
        }
        return int(status), payload

    def ready_payload(self) -> tuple[int, dict[str, Any]]:
        with self._lock:
            ready = self._ready
        status = HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE
        return int(status), {"worker": self.worker_name, "ready": ready}


class HealthHttpServer:
    def __init__(self, settings: ObservabilitySettings, metrics: MetricRegistry, health: HealthState):
        self.settings = settings
        self.metrics = metrics
        self.health = health
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not self.settings.enable_http or self._server is not None:
            return

        metrics = self.metrics
        health = self.health

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/healthz":
                    status, payload = health.health_payload()
                    self._write_json(status, payload)
                    return
                if self.path == "/readyz":
                    status, payload = health.ready_payload()
                    self._write_json(status, payload)
                    return
                if self.path == "/metrics":
                    payload = metrics.render_prometheus().encode("utf-8")
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "text/plain; version=0.0.4")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                self.send_response(HTTPStatus.NOT_FOUND)
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                del format, args

            def _write_json(self, status: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer((self.settings.host, self.settings.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name=f"{health.worker_name}-health")
        self._thread.start()

    def close(self) -> None:
        if self._server is None:
            return
        try:
            self._server.shutdown()
        finally:
            self._server.server_close()
            if self._thread is not None:
                self._thread.join(timeout=2.0)
        self._server = None
        self._thread = None


class WorkerTelemetry:
    def __init__(self, worker_name: str, settings: ObservabilitySettings):
        self.worker_name = worker_name
        self.logger = configure_json_logging(worker_name, settings.log_level)
        self.metrics = MetricRegistry()
        self.health = HealthState(worker_name, stale_after_seconds=settings.heartbeat_timeout_seconds)
        self.server = HealthHttpServer(settings, self.metrics, self.health)

    def start(self) -> None:
        self.server.start()
        self.health.heartbeat()
        self.metrics.set_gauge("worker_ready", 0.0, {"worker": self.worker_name})

    def heartbeat(self) -> None:
        self.health.heartbeat()
        self.metrics.set_gauge("worker_last_heartbeat_unix", time.time(), {"worker": self.worker_name})

    def set_ready(self, ready: bool = True) -> None:
        self.health.set_ready(ready)
        self.metrics.set_gauge("worker_ready", 1.0 if ready else 0.0, {"worker": self.worker_name})

    def set_unhealthy(self, reason: str) -> None:
        self.health.set_unhealthy(reason)

    def close(self) -> None:
        self.server.close()
