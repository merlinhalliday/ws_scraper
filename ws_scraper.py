#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import os
import queue
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote

import requests
import websockets
from coinbase.websocket import WSClient
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from polygale.config import APP_ROOT, RuntimeConfig
from polygale.utils import coerce_float, coerce_int, parse_coinbase_heartbeat_time, parse_iso8601

try:
    import zstandard as zstd
except Exception:
    zstd = None

try:
    import msal
except Exception:
    msal = None


COINBASE_PRODUCTS = ("BTC-USD", "ETH-USD")
COINBASE_MARKET_KEY = {
    "BTC-USD": "cb-btcusd",
    "ETH-USD": "cb-ethusd",
}
POLY_SYMBOLS = ("btc", "eth")
POLY_MARKET_KEY = {
    "btc": "pm-btc",
    "eth": "pm-eth",
}
POLY_SLUG_PREFIX = {
    "btc": "btc-updown-5m",
    "eth": "eth-updown-5m",
}
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
DATE_DIR_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ROTATED_FILE_PATTERN = re.compile(
    r"^(?P<market>[a-z0-9-]+)__start_(?P<start>\d{8}T\d{6}Z)__part_(?P<part>\d{4})\.ndjson\.zst$"
)
GRAPH_RESERVED_SCOPES = frozenset({"offline_access", "openid", "profile"})


@dataclass(frozen=True)
class CollectorConfig:
    output_root: Path
    duration_seconds: int
    snapshot_interval_seconds: int
    log_every_seconds: int
    ws_stale_timeout_seconds: float
    ws_reconnect_backoff_seconds: tuple[float, ...]
    size_target_mb_per_day: float
    zstd_level: int
    queue_maxsize: int
    gamma_markets_url: str
    polymarket_ws_url: str
    http_timeout_seconds: int
    coinbase_products: tuple[str, ...]
    graph_upload_enabled: bool
    graph_client_id: str
    graph_authority: str
    graph_scopes: tuple[str, ...]
    onedrive_folder: str
    rotate_upload_threshold_bytes: int
    graph_max_single_upload_bytes: int
    graph_upload_backoff_seconds: tuple[float, ...]
    graph_upload_timeout_seconds: float
    graph_token_cache_path: Path
    health_host: str
    health_port: int
    azure_log_analytics_workspace_id: str
    azure_log_analytics_shared_key: str
    azure_log_analytics_log_type: str
    alert_no_data_minutes: int
    alert_reconnect_storm_count: int
    alert_queue_growth_minutes: int
    alert_export_failure_threshold: float


@dataclass(frozen=True)
class PolymarketMarketMeta:
    symbol: str
    market_key: str
    slug: str
    bucket_start_ts: int
    condition_id: str
    yes_asset_id: str
    no_asset_id: str


@dataclass(frozen=True)
class PolymarketTokenBinding:
    symbol: str
    market_key: str
    side: str
    slug: str
    bucket_start_ts: int
    condition_id: str
    asset_id: str


@dataclass
class _WriterEntry:
    path: Path
    file_handle: Any
    stream: Any
    date_str: str
    market_key: str
    start_ts_iso: str
    part: int
    opened_monotonic: float
    bytes_at_open: int


@dataclass(frozen=True)
class UploadJob:
    local_path: Path
    date_str: str
    market_key: str
    filename: str
    size_bytes: int


def log_event(event_type: str, **fields: Any) -> None:
    payload: dict[str, Any] = {
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "event_type": str(event_type),
    }
    payload.update(fields)
    print(json.dumps(payload, separators=(",", ":"), default=str), flush=True)


@dataclass
class WorkerHeartbeat:
    worker: str
    ws_freshness_seconds: float = 0.0
    queue_backlog: int = 0
    blob_write_success_rate: float = 1.0
    export_lag_seconds: float = 0.0
    reconnect_count: int = 0
    last_update_unix: float = 0.0


class HealthServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = int(port)
        self._lock = threading.Lock()
        self._workers: dict[str, WorkerHeartbeat] = {}
        self._thread: Optional[threading.Thread] = None
        self._httpd: Optional[ThreadingHTTPServer] = None

    def update(self, heartbeat: WorkerHeartbeat) -> None:
        with self._lock:
            self._workers[heartbeat.worker] = heartbeat

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            workers = {k: vars(v).copy() for k, v in self._workers.items()}
        ready = bool(workers) and all(v.get("ws_freshness_seconds", 9999) < 120 for v in workers.values())
        return {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "ready": ready,
            "workers": workers,
        }

    def start(self) -> None:
        if self.port <= 0 or self._thread is not None:
            return
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                snap = parent.snapshot()
                if self.path in {"/health", "/healthz", "/metrics"}:
                    status = 200
                    body = snap
                elif self.path in {"/ready", "/readyz"}:
                    status = 200 if snap.get("ready") else 503
                    body = snap
                else:
                    status = 404
                    body = {"error": "not_found"}
                data = json.dumps(body, separators=(",", ":")).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, fmt: str, *args: Any) -> None:
                return

        self._httpd = ThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="health-server")
        self._thread.start()

    def close(self) -> None:
        if self._httpd is None:
            return
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)


class AzureLogAnalyticsClient:
    def __init__(self, workspace_id: str, shared_key: str, log_type: str = "WsScraperEvents"):
        self.workspace_id = normalize_env_scalar(workspace_id)
        self.shared_key = normalize_env_scalar(shared_key)
        self.log_type = normalize_env_scalar(log_type) or "WsScraperEvents"

    @property
    def enabled(self) -> bool:
        return bool(self.workspace_id and self.shared_key)

    def post_event(self, event: dict[str, Any]) -> tuple[bool, str]:
        if not self.enabled:
            return False, "disabled"
        body = json.dumps([event], separators=(",", ":"))
        rfc1123date = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        content_length = len(body)
        string_to_hash = f"POST\n{content_length}\napplication/json\nx-ms-date:{rfc1123date}\n/api/logs"
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(hmac.new(decoded_key, string_to_hash.encode("utf-8"), hashlib.sha256).digest())
        signature = f"SharedKey {self.workspace_id}:{encoded_hash.decode('utf-8')}"
        uri = f"https://{self.workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01"
        headers = {
            "content-type": "application/json",
            "Authorization": signature,
            "Log-Type": self.log_type,
            "x-ms-date": rfc1123date,
        }
        resp = requests.post(uri, data=body, headers=headers, timeout=10)
        return (resp.status_code in (200, 202), f"http_{resp.status_code}")


def to_iso_utc(ts: float | int | None) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""


def ts_floor(ts: int, window_seconds: int) -> int:
    if window_seconds <= 0:
        return ts
    return ts - (ts % window_seconds)


def parse_backoff_seconds(value: str, default: tuple[float, ...]) -> tuple[float, ...]:
    text = str(value or "").strip()
    if not text:
        return default
    out: list[float] = []
    for part in text.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(max(0.1, float(p)))
        except ValueError:
            continue
    return tuple(out) if out else default


def parse_env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def normalize_env_scalar(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1].strip()
    return text


def parse_graph_scopes(value: Any) -> tuple[str, ...]:
    raw = normalize_env_scalar(value)
    if not raw:
        return ()
    if raw.startswith("[") and raw.endswith("]"):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                out = [normalize_env_scalar(x) for x in parsed if normalize_env_scalar(x)]
                return tuple(out)
        except Exception:
            pass
    tokens = raw.replace(",", " ").split()
    out: list[str] = []
    for token in tokens:
        cleaned = token.strip().strip("[]").strip(",").strip().strip("'").strip('"')
        if cleaned:
            out.append(cleaned)
    return tuple(out)


def sanitize_graph_scopes(scopes: tuple[str, ...]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    keep: list[str] = []
    removed: list[str] = []
    seen_keep: set[str] = set()
    seen_removed: set[str] = set()

    for raw_scope in scopes:
        scope = normalize_env_scalar(raw_scope).strip()
        if not scope:
            continue
        scope_norm = scope.lower()
        if scope_norm in GRAPH_RESERVED_SCOPES:
            if scope_norm not in seen_removed:
                removed.append(scope_norm)
                seen_removed.add(scope_norm)
            continue
        if scope_norm in seen_keep:
            continue
        seen_keep.add(scope_norm)
        keep.append(scope)

    return tuple(keep), tuple(removed)


def normalize_onedrive_folder(value: Any) -> str:
    raw = normalize_env_scalar(value).replace("\\", "/")
    parts = [part.strip() for part in raw.split("/") if part.strip()]
    if not parts:
        return ""
    return "/" + "/".join(parts)


def is_date_dir_name(value: str) -> bool:
    return bool(DATE_DIR_PATTERN.match(str(value or "").strip()))


def build_retrying_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "polygale-ws-snapshot/1.0"})
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=0.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def normalize_listish(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            return [text]
    return []


def market_slug(symbol: str, bucket_start_ts: int) -> str:
    return f"{POLY_SLUG_PREFIX[symbol]}-{bucket_start_ts}"


def projected_daily_bytes(bytes_so_far: int, elapsed_seconds: float) -> float:
    if elapsed_seconds <= 0:
        return float(bytes_so_far)
    return float(bytes_so_far) / float(elapsed_seconds) * 86400.0


def compute_spread_bps(bid: float | None, ask: float | None) -> tuple[float | None, float | None]:
    if bid is None or ask is None:
        return None, None
    mid = (float(bid) + float(ask)) / 2.0
    if mid <= 0:
        return mid, None
    spread_bps = ((float(ask) - float(bid)) / mid) * 10000.0
    return mid, spread_bps


def _extract_book_top(levels: Any, want_max: bool) -> float | None:
    vals: list[float] = []
    if not isinstance(levels, list):
        return None
    for level in levels:
        price: float | None = None
        if isinstance(level, dict):
            price = coerce_float(level.get("price") or level.get("price_level") or level.get("px"))
        elif isinstance(level, list) and level:
            price = coerce_float(level[0])
        if price is None:
            continue
        vals.append(float(price))
    if not vals:
        return None
    return max(vals) if want_max else min(vals)


class CoinbaseWsDriver:
    def __init__(self, config: CollectorConfig, event_queue: queue.Queue[dict[str, Any]]):
        self.config = config
        self.event_queue = event_queue
        self.products = tuple(config.coinbase_products)
        self.ws: Optional[WSClient] = None
        self.last_message_monotonic = time.monotonic()
        self._dropped_lock = threading.Lock()
        self._dropped_counts: dict[str, int] = defaultdict(int)
        self.reconnect_count = 0
        self._book_lock = threading.Lock()
        self._books: dict[str, dict[str, dict[float, float]]] = {
            product: {"bid": {}, "ask": {}} for product in self.products
        }

    def _mark_dropped(self, market_key: str) -> None:
        key = market_key or "unknown"
        with self._dropped_lock:
            self._dropped_counts[key] += 1

    def _push_event(self, event: dict[str, Any]) -> None:
        market_key = str(event.get("market_key") or "")
        try:
            self.event_queue.put_nowait(event)
            return
        except queue.Full:
            pass
        try:
            self.event_queue.get_nowait()
        except queue.Empty:
            pass
        try:
            self.event_queue.put_nowait(event)
        except queue.Full:
            self._mark_dropped(market_key)

    @staticmethod
    def _market_key(product_id: str) -> str:
        return COINBASE_MARKET_KEY.get(str(product_id or "").strip().upper(), "")

    def _apply_level2_updates(self, product_id: str, event: dict[str, Any]) -> tuple[float | None, float | None]:
        product = str(product_id or "").strip().upper()
        if not product:
            return None, None
        with self._book_lock:
            side_books = self._books.setdefault(product, {"bid": {}, "ask": {}})
            bids = side_books["bid"]
            asks = side_books["ask"]

            if isinstance(event.get("bids"), list):
                bids.clear()
                for level in event.get("bids", []):
                    if isinstance(level, dict):
                        price = coerce_float(level.get("price") or level.get("price_level"))
                        size = coerce_float(level.get("size") or level.get("new_quantity") or level.get("qty"))
                    elif isinstance(level, list) and len(level) >= 2:
                        price = coerce_float(level[0])
                        size = coerce_float(level[1])
                    else:
                        continue
                    if price is None or size is None or size <= 0.0:
                        continue
                    bids[float(price)] = float(size)

            if isinstance(event.get("asks"), list):
                asks.clear()
                for level in event.get("asks", []):
                    if isinstance(level, dict):
                        price = coerce_float(level.get("price") or level.get("price_level"))
                        size = coerce_float(level.get("size") or level.get("new_quantity") or level.get("qty"))
                    elif isinstance(level, list) and len(level) >= 2:
                        price = coerce_float(level[0])
                        size = coerce_float(level[1])
                    else:
                        continue
                    if price is None or size is None or size <= 0.0:
                        continue
                    asks[float(price)] = float(size)

            updates = event.get("updates")
            if isinstance(updates, list):
                for upd in updates:
                    if not isinstance(upd, dict):
                        continue
                    side_raw = str(upd.get("side") or upd.get("book_side") or "").strip().lower()
                    side = "bid" if side_raw in {"bid", "buy", "bids"} else "ask"
                    px = coerce_float(upd.get("price_level") or upd.get("price") or upd.get("px"))
                    qty = coerce_float(upd.get("new_quantity") or upd.get("size") or upd.get("qty"))
                    if px is None:
                        continue
                    book = bids if side == "bid" else asks
                    if qty is None or qty <= 0.0:
                        book.pop(float(px), None)
                    else:
                        book[float(px)] = float(qty)

            best_bid = max(bids.keys()) if bids else None
            best_ask = min(asks.keys()) if asks else None
            return best_bid, best_ask

    def _on_message(self, raw_message: str) -> None:
        self.last_message_monotonic = time.monotonic()
        try:
            msg = json.loads(raw_message)
        except json.JSONDecodeError:
            return

        channel = str(msg.get("channel") or "").strip().lower()
        message_dt = parse_iso8601(msg.get("timestamp"))
        fallback_ts = float(message_dt.timestamp()) if message_dt else float(time.time())
        events = msg.get("events", [])
        if not isinstance(events, list):
            events = []

        if channel == "heartbeats":
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                product_id = str(ev.get("product_id") or ev.get("productId") or "").strip().upper()
                hb_dt = parse_coinbase_heartbeat_time(ev.get("current_time"))
                event_ts = float(hb_dt.timestamp()) if hb_dt else fallback_ts
                product_ids = [product_id] if product_id else list(self.products)
                for product in product_ids:
                    market_key = self._market_key(product)
                    if not market_key:
                        continue
                    self._push_event(
                        {
                            "venue": "coinbase",
                            "market_key": market_key,
                            "kind": "heartbeat",
                            "event_ts": event_ts,
                            "product_id": product,
                        }
                    )
            return

        if channel == "ticker":
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                tickers = ev.get("tickers")
                ticker_events = tickers if isinstance(tickers, list) else [ev]
                for tk in ticker_events:
                    if not isinstance(tk, dict):
                        continue
                    product_id = str(tk.get("product_id") or tk.get("productId") or "").strip().upper()
                    market_key = self._market_key(product_id)
                    if not market_key:
                        continue
                    bid = coerce_float(tk.get("best_bid") or tk.get("bestBid"))
                    ask = coerce_float(tk.get("best_ask") or tk.get("bestAsk"))
                    last_px = coerce_float(tk.get("price"))
                    self._push_event(
                        {
                            "venue": "coinbase",
                            "market_key": market_key,
                            "kind": "quote",
                            "event_ts": fallback_ts,
                            "product_id": product_id,
                            "bid": float(bid) if bid is not None else None,
                            "ask": float(ask) if ask is not None else None,
                            "last_trade_px": float(last_px) if last_px is not None else None,
                        }
                    )
            return

        if channel == "market_trades":
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                trades = ev.get("trades", [])
                if not isinstance(trades, list):
                    continue
                for tr in trades:
                    if not isinstance(tr, dict):
                        continue
                    product_id = str(tr.get("product_id") or tr.get("productId") or "").strip().upper()
                    market_key = self._market_key(product_id)
                    if not market_key:
                        continue
                    px = coerce_float(tr.get("price"))
                    sz = coerce_float(tr.get("size"))
                    if px is None:
                        continue
                    side = str(tr.get("side") or "").strip().upper()
                    trade_dt = parse_iso8601(tr.get("time"))
                    event_ts = float(trade_dt.timestamp()) if trade_dt else fallback_ts
                    self._push_event(
                        {
                            "venue": "coinbase",
                            "market_key": market_key,
                            "kind": "trade",
                            "event_ts": event_ts,
                            "product_id": product_id,
                            "price": float(px),
                            "size": float(sz) if sz is not None else None,
                            "side": side,
                        }
                    )
            return

        if channel == "level2":
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                product_id = str(ev.get("product_id") or ev.get("productId") or "").strip().upper()
                market_key = self._market_key(product_id)
                if not market_key:
                    continue
                best_bid, best_ask = self._apply_level2_updates(product_id, ev)
                if best_bid is None and best_ask is None:
                    continue
                self._push_event(
                    {
                        "venue": "coinbase",
                        "market_key": market_key,
                        "kind": "book_update",
                        "event_ts": fallback_ts,
                        "product_id": product_id,
                        "bid": float(best_bid) if best_bid is not None else None,
                        "ask": float(best_ask) if best_ask is not None else None,
                    }
                )

    def connect(self) -> None:
        self.close()
        self.ws = WSClient(on_message=self._on_message, retry=False, verbose=False)
        self.ws.open()
        self.ws.heartbeats()
        self.ws.ticker(list(self.products))
        self.ws.market_trades(list(self.products))
        self.ws.level2(list(self.products))
        self.last_message_monotonic = time.monotonic()

    def close(self) -> None:
        if self.ws is None:
            return
        try:
            self.ws.close()
        except Exception:
            pass
        self.ws = None

    def reconnect(self) -> bool:
        self.close()
        for wait_s in self.config.ws_reconnect_backoff_seconds:
            try:
                self.connect()
                self.reconnect_count += 1
                return True
            except Exception:
                time.sleep(wait_s)
        return False

    def seconds_since_message(self) -> float:
        return time.monotonic() - self.last_message_monotonic

    def collect_and_reset_dropped_counts(self) -> dict[str, int]:
        with self._dropped_lock:
            out = dict(self._dropped_counts)
            self._dropped_counts.clear()
            return out


class PolymarketWsDriver:
    def __init__(self, config: CollectorConfig, event_queue: queue.Queue[dict[str, Any]]):
        self.config = config
        self.event_queue = event_queue
        self.url = config.polymarket_ws_url
        self._stop_event = threading.Event()
        self._reconnect_requested = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws: Any = None

        self._desired_lock = threading.Lock()
        self._desired_bindings: dict[str, PolymarketTokenBinding] = {}
        self._desired_version = 0
        self._applied_version = -1
        self._subscribed_assets: set[str] = set()

        self.last_message_monotonic = time.monotonic()
        self._dropped_lock = threading.Lock()
        self._dropped_counts: dict[str, int] = defaultdict(int)
        self.reconnect_count = 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._reconnect_requested.clear()
        self._thread = threading.Thread(target=self._run_thread, daemon=True, name="polymarket-ws")
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        self._reconnect_requested.set()
        loop = self._loop
        if loop and loop.is_running():
            try:
                loop.call_soon_threadsafe(lambda: None)
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)

    def request_reconnect(self) -> None:
        self._reconnect_requested.set()
        loop = self._loop
        if loop and loop.is_running() and self._ws is not None:
            try:
                asyncio.run_coroutine_threadsafe(self._ws.close(), loop)
            except Exception:
                pass

    def seconds_since_message(self) -> float:
        return time.monotonic() - self.last_message_monotonic

    def collect_and_reset_dropped_counts(self) -> dict[str, int]:
        with self._dropped_lock:
            out = dict(self._dropped_counts)
            self._dropped_counts.clear()
            return out

    @staticmethod
    def compute_subscription_delta(desired_assets: set[str], subscribed_assets: set[str]) -> tuple[list[str], list[str]]:
        to_subscribe = sorted(desired_assets - subscribed_assets)
        to_unsubscribe = sorted(subscribed_assets - desired_assets)
        return to_subscribe, to_unsubscribe

    def set_asset_bindings(self, bindings: dict[str, PolymarketTokenBinding]) -> None:
        with self._desired_lock:
            clean = {str(k): v for k, v in bindings.items() if str(k)}
            if clean == self._desired_bindings:
                return
            self._desired_bindings = clean
            self._desired_version += 1

    def _snapshot_desired(self) -> tuple[dict[str, PolymarketTokenBinding], int]:
        with self._desired_lock:
            return dict(self._desired_bindings), int(self._desired_version)

    def _mark_dropped(self, market_key: str) -> None:
        key = market_key or "unknown"
        with self._dropped_lock:
            self._dropped_counts[key] += 1

    def _push_event(self, event: dict[str, Any]) -> None:
        market_key = str(event.get("market_key") or "")
        try:
            self.event_queue.put_nowait(event)
            return
        except queue.Full:
            pass
        try:
            self.event_queue.get_nowait()
        except queue.Empty:
            pass
        try:
            self.event_queue.put_nowait(event)
        except queue.Full:
            self._mark_dropped(market_key)

    def _parse_polymarket_payload(self, payload: Any, binding_lookup: Callable[[str], Optional[PolymarketTokenBinding]]) -> None:
        messages = payload if isinstance(payload, list) else [payload]
        for item in messages:
            if not isinstance(item, dict):
                continue
            event_type = str(item.get("event_type") or item.get("type") or "").strip().lower()
            if event_type in {"", "subscribed", "unsubscribed", "pong", "status"}:
                continue

            if event_type == "price_change":
                changes = item.get("price_changes")
                if not isinstance(changes, list):
                    continue
                for change in changes:
                    if not isinstance(change, dict):
                        continue
                    asset_id = str(
                        change.get("asset_id")
                        or change.get("assetId")
                        or change.get("asset")
                        or change.get("token_id")
                        or ""
                    )
                    binding = binding_lookup(asset_id)
                    if binding is None:
                        continue
                    bid = coerce_float(change.get("best_bid") or change.get("bid"))
                    ask = coerce_float(change.get("best_ask") or change.get("ask"))
                    px = coerce_float(change.get("price"))
                    event_ts = coerce_float(change.get("timestamp")) or float(time.time())
                    self._push_event(
                        {
                            "venue": "polymarket",
                            "market_key": binding.market_key,
                            "kind": "book_update",
                            "event_ts": float(event_ts),
                            "side": binding.side,
                            "asset_id": binding.asset_id,
                            "slug": binding.slug,
                            "bucket_start_ts": binding.bucket_start_ts,
                            "condition_id": binding.condition_id,
                            "bid": float(bid) if bid is not None else None,
                            "ask": float(ask) if ask is not None else None,
                            "price": float(px) if px is not None else None,
                        }
                    )
                continue

            asset_id = str(
                item.get("asset_id")
                or item.get("assetId")
                or item.get("asset")
                or item.get("token_id")
                or ""
            )
            binding = binding_lookup(asset_id)
            if binding is None:
                continue

            if event_type == "best_bid_ask":
                bid = coerce_float(item.get("best_bid") or item.get("bid"))
                ask = coerce_float(item.get("best_ask") or item.get("ask"))
                event_ts = coerce_float(item.get("timestamp")) or float(time.time())
                self._push_event(
                    {
                        "venue": "polymarket",
                        "market_key": binding.market_key,
                        "kind": "quote",
                        "event_ts": float(event_ts),
                        "side": binding.side,
                        "asset_id": binding.asset_id,
                        "slug": binding.slug,
                        "bucket_start_ts": binding.bucket_start_ts,
                        "condition_id": binding.condition_id,
                        "bid": float(bid) if bid is not None else None,
                        "ask": float(ask) if ask is not None else None,
                    }
                )
                continue

            if event_type == "last_trade_price":
                px = coerce_float(item.get("price") or item.get("last_trade_price"))
                sz = coerce_float(item.get("size"))
                side = str(item.get("side") or "").strip().upper()
                event_ts = coerce_float(item.get("timestamp")) or float(time.time())
                self._push_event(
                    {
                        "venue": "polymarket",
                        "market_key": binding.market_key,
                        "kind": "trade",
                        "event_ts": float(event_ts),
                        "side": binding.side,
                        "asset_id": binding.asset_id,
                        "slug": binding.slug,
                        "bucket_start_ts": binding.bucket_start_ts,
                        "condition_id": binding.condition_id,
                        "price": float(px) if px is not None else None,
                        "size": float(sz) if sz is not None else None,
                        "trade_side": side,
                    }
                )
                continue

            if event_type == "book":
                bids = item.get("bids")
                asks = item.get("asks")
                bid = _extract_book_top(bids, want_max=True)
                ask = _extract_book_top(asks, want_max=False)
                event_ts = coerce_float(item.get("timestamp")) or float(time.time())
                self._push_event(
                    {
                        "venue": "polymarket",
                        "market_key": binding.market_key,
                        "kind": "book_update",
                        "event_ts": float(event_ts),
                        "side": binding.side,
                        "asset_id": binding.asset_id,
                        "slug": binding.slug,
                        "bucket_start_ts": binding.bucket_start_ts,
                        "condition_id": binding.condition_id,
                        "bid": float(bid) if bid is not None else None,
                        "ask": float(ask) if ask is not None else None,
                    }
                )

    async def _send_initial_subscription(self, ws: Any, desired_assets: set[str]) -> None:
        if not desired_assets:
            return
        payload = {
            "type": "market",
            "assets_ids": sorted(desired_assets),
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(payload))
        self._subscribed_assets = set(desired_assets)

    async def _send_delta_subscription(self, ws: Any, desired_assets: set[str]) -> None:
        to_subscribe, to_unsubscribe = self.compute_subscription_delta(desired_assets, self._subscribed_assets)
        if to_unsubscribe:
            payload = {
                "assets_ids": to_unsubscribe,
                "operation": "unsubscribe",
                "custom_feature_enabled": True,
            }
            await ws.send(json.dumps(payload))
            self._subscribed_assets.difference_update(to_unsubscribe)
        if to_subscribe:
            payload = {
                "assets_ids": to_subscribe,
                "operation": "subscribe",
                "custom_feature_enabled": True,
            }
            await ws.send(json.dumps(payload))
            self._subscribed_assets.update(to_subscribe)

    async def _run_async(self) -> None:
        self._loop = asyncio.get_running_loop()
        backoff_idx = 0
        while not self._stop_event.is_set():
            desired_bindings, desired_version = self._snapshot_desired()
            desired_assets = {asset_id for asset_id in desired_bindings.keys() if asset_id}
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=4_000_000,
                ) as ws:
                    self._ws = ws
                    self.last_message_monotonic = time.monotonic()
                    self._subscribed_assets.clear()
                    self._reconnect_requested.clear()
                    self._applied_version = -1
                    if desired_assets:
                        await self._send_initial_subscription(ws, desired_assets)
                        self._applied_version = desired_version

                    while not self._stop_event.is_set():
                        if self._reconnect_requested.is_set():
                            raise RuntimeError("polymarket_reconnect_requested")

                        desired_bindings, desired_version = self._snapshot_desired()
                        desired_assets = {asset_id for asset_id in desired_bindings.keys() if asset_id}
                        if desired_version != self._applied_version:
                            if not self._subscribed_assets:
                                await self._send_initial_subscription(ws, desired_assets)
                            else:
                                await self._send_delta_subscription(ws, desired_assets)
                            self._applied_version = desired_version

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue

                        self.last_message_monotonic = time.monotonic()
                        try:
                            payload = json.loads(raw)
                        except Exception:
                            continue
                        self._parse_polymarket_payload(payload, lambda aid: desired_bindings.get(str(aid)))

                backoff_idx = 0
            except Exception as exc:
                self._ws = None
                self._subscribed_assets.clear()
                if self._stop_event.is_set():
                    break
                wait_s = self.config.ws_reconnect_backoff_seconds[
                    min(backoff_idx, len(self.config.ws_reconnect_backoff_seconds) - 1)
                ]
                backoff_idx += 1
                self.reconnect_count += 1
                log_event("ws_reconnect", worker="polymarket", wait_seconds=wait_s, reconnect_count=self.reconnect_count, reason=f"{type(exc).__name__}:{exc}")
                await asyncio.sleep(wait_s)

    def _run_thread(self) -> None:
        try:
            asyncio.run(self._run_async())
        except Exception as exc:
            log_event("worker_stopped", worker="polymarket", reason=f"{type(exc).__name__}:{exc}")


class PolymarketMarketResolver:
    def __init__(self, config: CollectorConfig, session: requests.Session):
        self.config = config
        self.session = session
        self.current_meta: dict[str, PolymarketMarketMeta] = {}
        self.current_bindings: dict[str, PolymarketTokenBinding] = {}

    @staticmethod
    def _pick_yes_no_assets(token_ids: list[str], outcomes: list[str]) -> tuple[str, str]:
        yes_asset = ""
        no_asset = ""
        lowered = [str(out).strip().lower() for out in outcomes]

        for idx, outcome in enumerate(lowered):
            token = token_ids[idx] if idx < len(token_ids) else ""
            if not token:
                continue
            if outcome in {"yes", "up"} and not yes_asset:
                yes_asset = token
            if outcome in {"no", "down"} and not no_asset:
                no_asset = token

        if not yes_asset and token_ids:
            yes_asset = token_ids[0]
        if not no_asset and len(token_ids) >= 2:
            no_asset = token_ids[1]
        return yes_asset, no_asset

    def _fetch_market_meta(self, symbol: str, bucket_start_ts: int) -> tuple[Optional[PolymarketMarketMeta], str]:
        slug = market_slug(symbol, bucket_start_ts)
        try:
            resp = self.session.get(
                self.config.gamma_markets_url,
                params={"slug": slug, "limit": 1},
                timeout=self.config.http_timeout_seconds,
            )
        except Exception as exc:
            return None, f"gamma_fetch_failed:{symbol}:{type(exc).__name__}:{exc}"
        if not resp.ok:
            return None, f"gamma_fetch_failed:{symbol}:{resp.status_code}"
        try:
            payload = resp.json()
        except Exception as exc:
            return None, f"gamma_bad_json:{symbol}:{type(exc).__name__}:{exc}"
        if not isinstance(payload, list) or not payload:
            return None, f"gamma_market_not_found:{slug}"

        market = payload[0] if isinstance(payload[0], dict) else {}
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "")
        token_ids_raw = normalize_listish(market.get("clobTokenIds") or market.get("clob_token_ids") or [])
        token_ids = [str(x).strip() for x in token_ids_raw if str(x).strip()]
        outcomes_raw = normalize_listish(market.get("outcomes") or [])
        outcomes = [str(x).strip() for x in outcomes_raw]

        if len(token_ids) < 2:
            return None, f"gamma_missing_token_ids:{symbol}:{slug}"

        yes_asset, no_asset = self._pick_yes_no_assets(token_ids, outcomes)
        if not yes_asset or not no_asset:
            return None, f"gamma_missing_yes_no_assets:{symbol}:{slug}"

        return (
            PolymarketMarketMeta(
                symbol=symbol,
                market_key=POLY_MARKET_KEY[symbol],
                slug=slug,
                bucket_start_ts=bucket_start_ts,
                condition_id=condition_id,
                yes_asset_id=yes_asset,
                no_asset_id=no_asset,
            ),
            "",
        )

    @staticmethod
    def _meta_to_bindings(meta: PolymarketMarketMeta) -> list[PolymarketTokenBinding]:
        return [
            PolymarketTokenBinding(
                symbol=meta.symbol,
                market_key=meta.market_key,
                side="yes",
                slug=meta.slug,
                bucket_start_ts=meta.bucket_start_ts,
                condition_id=meta.condition_id,
                asset_id=meta.yes_asset_id,
            ),
            PolymarketTokenBinding(
                symbol=meta.symbol,
                market_key=meta.market_key,
                side="no",
                slug=meta.slug,
                bucket_start_ts=meta.bucket_start_ts,
                condition_id=meta.condition_id,
                asset_id=meta.no_asset_id,
            ),
        ]

    def refresh(self, now_ts: int) -> tuple[bool, dict[str, PolymarketMarketMeta], dict[str, PolymarketTokenBinding], list[str]]:
        bucket_start_ts = ts_floor(int(now_ts), 300)
        next_meta = dict(self.current_meta)
        errors: list[str] = []

        for symbol in POLY_SYMBOLS:
            meta, err = self._fetch_market_meta(symbol, bucket_start_ts)
            if meta is None:
                if err:
                    errors.append(err)
                continue
            next_meta[meta.market_key] = meta

        next_bindings: dict[str, PolymarketTokenBinding] = {}
        for market_key in (POLY_MARKET_KEY["btc"], POLY_MARKET_KEY["eth"]):
            meta = next_meta.get(market_key)
            if meta is None:
                continue
            for binding in self._meta_to_bindings(meta):
                next_bindings[binding.asset_id] = binding

        changed = next_bindings != self.current_bindings
        self.current_meta = next_meta
        self.current_bindings = next_bindings
        return changed, dict(self.current_meta), dict(self.current_bindings), errors


@dataclass
class SnapshotMarketState:
    market_key: str
    venue: str
    msg_count_1s: int = 0
    trade_count_1s: int = 0
    book_update_count_1s: int = 0
    dropped_events_1s: int = 0
    last_event_ts: Optional[float] = None

    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    mid: Optional[float] = None
    spread_bps: Optional[float] = None
    last_trade_px: Optional[float] = None
    last_trade_size: Optional[float] = None
    last_trade_side: str = ""

    slug: str = ""
    bucket_start_ts: Optional[int] = None
    condition_id: str = ""

    yes_best_bid: Optional[float] = None
    yes_best_ask: Optional[float] = None
    yes_mid: Optional[float] = None
    no_best_bid: Optional[float] = None
    no_best_ask: Optional[float] = None
    no_mid: Optional[float] = None
    last_trade_px_yes: Optional[float] = None
    last_trade_px_no: Optional[float] = None


class WsSnapshotAggregator:
    def __init__(self):
        self.states: dict[str, SnapshotMarketState] = {
            "cb-btcusd": SnapshotMarketState(market_key="cb-btcusd", venue="coinbase"),
            "cb-ethusd": SnapshotMarketState(market_key="cb-ethusd", venue="coinbase"),
            "pm-btc": SnapshotMarketState(market_key="pm-btc", venue="polymarket"),
            "pm-eth": SnapshotMarketState(market_key="pm-eth", venue="polymarket"),
        }

    def update_polymarket_meta(self, metas: dict[str, PolymarketMarketMeta]) -> None:
        for market_key, meta in metas.items():
            state = self.states.get(market_key)
            if state is None:
                continue
            state.slug = meta.slug
            state.bucket_start_ts = meta.bucket_start_ts
            state.condition_id = meta.condition_id

    def apply_dropped_counts(self, dropped_counts: dict[str, int]) -> None:
        for market_key, count in dropped_counts.items():
            state = self.states.get(market_key)
            if state is None:
                continue
            state.dropped_events_1s += int(max(0, count))

    def apply_event(self, event: dict[str, Any]) -> None:
        market_key = str(event.get("market_key") or "")
        state = self.states.get(market_key)
        if state is None:
            return

        event_ts = coerce_float(event.get("event_ts"))
        if event_ts is None:
            event_ts = float(time.time())

        state.last_event_ts = float(event_ts)
        state.msg_count_1s += 1

        kind = str(event.get("kind") or "").strip().lower()
        if kind == "trade":
            state.trade_count_1s += 1
        if kind in {"book_update", "book_snapshot", "quote"}:
            state.book_update_count_1s += 1

        if state.venue == "coinbase":
            bid = coerce_float(event.get("bid"))
            ask = coerce_float(event.get("ask"))
            if bid is not None:
                state.best_bid = float(bid)
            if ask is not None:
                state.best_ask = float(ask)
            mid, spread_bps = compute_spread_bps(state.best_bid, state.best_ask)
            state.mid = mid
            state.spread_bps = spread_bps

            if kind == "trade":
                px = coerce_float(event.get("price"))
                size = coerce_float(event.get("size"))
                side = str(event.get("side") or "").strip().upper()
                if px is not None:
                    state.last_trade_px = float(px)
                if size is not None:
                    state.last_trade_size = float(size)
                state.last_trade_side = side
            else:
                px = coerce_float(event.get("last_trade_px"))
                if px is not None:
                    state.last_trade_px = float(px)
            return

        side = str(event.get("side") or "").strip().lower()
        if event.get("slug"):
            state.slug = str(event.get("slug") or "")
        bucket_ts = coerce_int(event.get("bucket_start_ts"))
        if bucket_ts is not None:
            state.bucket_start_ts = int(bucket_ts)
        if event.get("condition_id"):
            state.condition_id = str(event.get("condition_id") or "")

        bid = coerce_float(event.get("bid"))
        ask = coerce_float(event.get("ask"))
        px = coerce_float(event.get("price"))

        if side == "yes":
            if bid is not None:
                state.yes_best_bid = float(bid)
            if ask is not None:
                state.yes_best_ask = float(ask)
            yes_mid, _ = compute_spread_bps(state.yes_best_bid, state.yes_best_ask)
            state.yes_mid = yes_mid
            if kind == "trade" and px is not None:
                state.last_trade_px_yes = float(px)
        elif side == "no":
            if bid is not None:
                state.no_best_bid = float(bid)
            if ask is not None:
                state.no_best_ask = float(ask)
            no_mid, _ = compute_spread_bps(state.no_best_bid, state.no_best_ask)
            state.no_mid = no_mid
            if kind == "trade" and px is not None:
                state.last_trade_px_no = float(px)

    def build_snapshots(self, snapshot_ts: float, snapshot_interval_seconds: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        ts_iso = to_iso_utc(snapshot_ts)
        for market_key in sorted(self.states.keys()):
            s = self.states[market_key]
            stale_ms = ""
            if s.last_event_ts is not None:
                stale_ms = max(0.0, (float(snapshot_ts) - float(s.last_event_ts)) * 1000.0)

            row: dict[str, Any] = {
                "ts_utc": ts_iso,
                "market_key": s.market_key,
                "venue": s.venue,
                "snapshot_interval_s": int(snapshot_interval_seconds),
                "msg_count_1s": int(s.msg_count_1s),
                "trade_count_1s": int(s.trade_count_1s),
                "book_update_count_1s": int(s.book_update_count_1s),
                "dropped_events_1s": int(s.dropped_events_1s),
                "last_event_ts_utc": to_iso_utc(s.last_event_ts),
                "stale_ms": stale_ms,
                "best_bid": s.best_bid if s.venue == "coinbase" else "",
                "best_ask": s.best_ask if s.venue == "coinbase" else "",
                "mid": s.mid if s.venue == "coinbase" else "",
                "spread_bps": s.spread_bps if s.venue == "coinbase" else "",
                "last_trade_px": s.last_trade_px if s.venue == "coinbase" else "",
                "last_trade_size": s.last_trade_size if s.venue == "coinbase" else "",
                "last_trade_side": s.last_trade_side if s.venue == "coinbase" else "",
                "slug": s.slug if s.venue == "polymarket" else "",
                "bucket_start_ts": s.bucket_start_ts if s.venue == "polymarket" else "",
                "condition_id": s.condition_id if s.venue == "polymarket" else "",
                "yes_best_bid": s.yes_best_bid if s.venue == "polymarket" else "",
                "yes_best_ask": s.yes_best_ask if s.venue == "polymarket" else "",
                "yes_mid": s.yes_mid if s.venue == "polymarket" else "",
                "no_best_bid": s.no_best_bid if s.venue == "polymarket" else "",
                "no_best_ask": s.no_best_ask if s.venue == "polymarket" else "",
                "no_mid": s.no_mid if s.venue == "polymarket" else "",
                "last_trade_px_yes": s.last_trade_px_yes if s.venue == "polymarket" else "",
                "last_trade_px_no": s.last_trade_px_no if s.venue == "polymarket" else "",
            }
            out.append(row)

            s.msg_count_1s = 0
            s.trade_count_1s = 0
            s.book_update_count_1s = 0
            s.dropped_events_1s = 0

        return out


class GraphAuthManager:
    def __init__(self, client_id: str, authority: str, scopes: tuple[str, ...], cache_path: Path):
        if msal is None:
            raise RuntimeError("missing_dependency:msal (pip install msal)")
        self.client_id = normalize_env_scalar(client_id)
        self.authority = normalize_env_scalar(authority)
        self.scopes = tuple(scopes)
        self.cache_path = Path(cache_path)
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
        tmp_path = self.cache_path.with_suffix(self.cache_path.suffix + ".tmp")
        tmp_path.write_text(payload, encoding="utf-8")
        tmp_path.replace(self.cache_path)

    def _try_silent_unlocked(self) -> str:
        accounts = self._app.get_accounts()
        for account in accounts:
            result = self._app.acquire_token_silent(scopes=list(self.scopes), account=account)
            if isinstance(result, dict) and result.get("access_token"):
                self._persist_cache_unlocked()
                return str(result.get("access_token"))
        return ""

    def acquire_access_token(self, allow_device_flow: bool) -> str:
        with self._lock:
            token = self._try_silent_unlocked()
            if token:
                return token
            if not allow_device_flow:
                raise RuntimeError("graph_token_unavailable_silent")
            flow = self._app.initiate_device_flow(scopes=list(self.scopes))
            if not isinstance(flow, dict) or not flow.get("user_code"):
                raise RuntimeError(f"graph_device_flow_init_failed:{flow}")
            message = str(flow.get("message") or "").strip()

        if message:
            log_event("graph_device_flow", message=message)
        result = self._app.acquire_token_by_device_flow(flow)
        token = str((result or {}).get("access_token") or "").strip()
        if not token:
            detail = str((result or {}).get("error_description") or (result or {}).get("error") or "unknown_error")
            raise RuntimeError(f"graph_device_flow_failed:{detail}")
        with self._lock:
            self._persist_cache_unlocked()
        return token


class GraphUploader:
    def __init__(
        self,
        auth_manager: GraphAuthManager,
        onedrive_folder: str,
        timeout_seconds: float,
        backoff_seconds: tuple[float, ...],
        max_single_upload_bytes: int,
    ):
        self.auth_manager = auth_manager
        self.onedrive_folder = normalize_onedrive_folder(onedrive_folder)
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.backoff_seconds = tuple(backoff_seconds) if backoff_seconds else (2.0, 5.0, 15.0, 30.0, 60.0)
        self.max_single_upload_bytes = max(1_000_000, int(max_single_upload_bytes))
        self._queue: queue.Queue[UploadJob] = queue.Queue(maxsize=100_000)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._stats_lock = threading.Lock()
        self._uploaded_ok = 0
        self._failed_attempts = 0
        self._retry_attempts = 0
        self._attempted_uploads = 0
        self._last_success_unix = 0.0

    @staticmethod
    def _upload_url(onedrive_folder: str, date_str: str, filename: str) -> str:
        segments = [s for s in onedrive_folder.split("/") if s]
        segments.extend([date_str, filename])
        encoded = "/".join(quote(seg, safe="") for seg in segments)
        return f"{GRAPH_API_BASE}/me/drive/root:/{encoded}:/content"

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="graph-upload-worker")
        self._thread.start()

    def enqueue(self, job: UploadJob) -> None:
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            # Queue saturation should be highly unlikely; fallback to blocking put to avoid dropping files.
            self._queue.put(job)

    def pending_count(self) -> int:
        return int(self._queue.qsize())

    def stats_snapshot(self) -> dict[str, Any]:
        with self._stats_lock:
            attempted = max(1, int(self._attempted_uploads))
            success_rate = float(self._uploaded_ok) / float(attempted)
            export_lag = 0.0 if self._last_success_unix <= 0 else max(0.0, time.time() - self._last_success_unix)
            return {
                "uploaded_ok": int(self._uploaded_ok),
                "failed_attempts": int(self._failed_attempts),
                "retry_attempts": int(self._retry_attempts),
                "pending_queue": int(self._queue.qsize()),
                "success_rate": float(success_rate),
                "export_lag_seconds": float(export_lag),
            }

    def _upload_once(self, job: UploadJob) -> tuple[bool, str]:
        if not job.local_path.exists():
            return True, "missing_local_file"
        if int(job.size_bytes) > self.max_single_upload_bytes:
            return False, (
                f"file_too_large_for_single_upload:size={job.size_bytes},"
                f"max={self.max_single_upload_bytes}"
            )
        token = self.auth_manager.acquire_access_token(allow_device_flow=False)
        url = self._upload_url(self.onedrive_folder, job.date_str, job.filename)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        }
        with job.local_path.open("rb") as fh:
            resp = requests.put(url, headers=headers, data=fh, timeout=self.timeout_seconds)
        if resp.status_code in (200, 201):
            try:
                job.local_path.unlink()
            except FileNotFoundError:
                pass
            return True, f"http_{resp.status_code}"
        text = str(resp.text or "").strip().replace("\n", " ")
        if len(text) > 200:
            text = text[:200] + "..."
        return False, f"http_{resp.status_code}:{text}"

    def _run(self) -> None:
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                job = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            attempt = 0
            while True:
                attempt += 1
                ok = False
                detail = ""
                try:
                    ok, detail = self._upload_once(job)
                except Exception as exc:
                    ok = False
                    detail = f"exception:{exc}"

                if ok:
                    with self._stats_lock:
                        self._uploaded_ok += 1
                        self._attempted_uploads += 1
                        self._last_success_unix = time.time()
                    log_event("graph_upload", market=job.market_key, lag=None, reconnect_count=None, queue_depth=self.pending_count(), upload_status="success", date=job.date_str, file=job.filename, detail=detail)
                    break

                delay = float(self.backoff_seconds[min(attempt - 1, len(self.backoff_seconds) - 1)])
                with self._stats_lock:
                    self._failed_attempts += 1
                    self._retry_attempts += 1
                    self._attempted_uploads += 1
                log_event("graph_upload", market=job.market_key, lag=None, reconnect_count=None, queue_depth=self.pending_count(), upload_status="retry", date=job.date_str, file=job.filename, attempt=attempt, retry_in_s=delay, reason=detail)
                # Retry forever while preserving FIFO order.
                if self._stop_event.wait(delay):
                    break
            self._queue.task_done()

    def close(self, wait_seconds: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is None:
            return
        self._thread.join(timeout=max(0.0, float(wait_seconds)))
        if self._thread.is_alive():
            log_event("graph_upload", market="all", lag=None, reconnect_count=None, queue_depth=self.pending_count(), upload_status="shutdown_pending")


def infer_market_from_filename(filename: str) -> str:
    name = str(filename or "").strip()
    match = ROTATED_FILE_PATTERN.match(name)
    if match:
        return str(match.group("market"))
    if name.endswith(".ndjson.zst"):
        return name[: -len(".ndjson.zst")]
    return ""


def discover_pending_upload_jobs(output_root: Path) -> list[UploadJob]:
    root = Path(output_root)
    if not root.exists():
        return []
    out: list[UploadJob] = []
    for path in sorted(root.rglob("*.ndjson.zst")):
        if not path.is_file():
            continue
        market_key = infer_market_from_filename(path.name)
        if not market_key:
            continue
        try:
            size_bytes = int(path.stat().st_size)
        except Exception:
            size_bytes = 0
        parent_name = path.parent.name
        if is_date_dir_name(parent_name):
            date_str = parent_name
        else:
            try:
                date_str = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%d")
            except Exception:
                date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out.append(
            UploadJob(
                local_path=path,
                date_str=date_str,
                market_key=market_key,
                filename=path.name,
                size_bytes=size_bytes,
            )
        )
    return out


class NdjsonZstdWriter:
    def __init__(
        self,
        output_root: Path,
        zstd_level: int = 3,
        rotate_upload_threshold_bytes: int = 3_800_000,
        max_single_upload_bytes: int = 4_000_000,
        on_file_closed: Optional[Callable[[UploadJob], None]] = None,
    ):
        if zstd is None:
            raise RuntimeError("missing_dependency:zstandard (pip install zstandard)")
        self.output_root = Path(output_root)
        self.zstd_level = int(zstd_level)
        self.rotate_upload_threshold_bytes = max(100_000, int(rotate_upload_threshold_bytes))
        self.max_single_upload_bytes = max(200_000, int(max_single_upload_bytes))
        self._on_file_closed = on_file_closed
        self._active_entries: dict[str, _WriterEntry] = {}
        self._max_part_seen: dict[tuple[str, str], int] = {}

    @staticmethod
    def _date_from_ts_iso(ts_iso: str) -> str:
        dt = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def _start_stamp_from_ts_iso(ts_iso: str) -> str:
        dt = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt.strftime("%Y%m%dT%H%M%SZ")

    @staticmethod
    def _build_filename(market_key: str, start_stamp: str, part: int) -> str:
        return f"{market_key}__start_{start_stamp}__part_{part:04d}.ndjson.zst"

    def _get_max_part_for_day(self, date_str: str, market_key: str) -> int:
        key = (date_str, market_key)
        existing = self._max_part_seen.get(key)
        if existing is not None:
            return existing
        dir_path = self.output_root / date_str
        max_part = 0
        if dir_path.exists():
            pattern = f"{market_key}__start_*__part_*.ndjson.zst"
            for file_path in dir_path.glob(pattern):
                match = ROTATED_FILE_PATTERN.match(file_path.name)
                if not match:
                    continue
                try:
                    part = int(match.group("part"))
                except Exception:
                    continue
                if part > max_part:
                    max_part = part
        self._max_part_seen[key] = max_part
        return max_part

    def _next_part(self, date_str: str, market_key: str) -> int:
        max_part = self._get_max_part_for_day(date_str, market_key) + 1
        self._max_part_seen[(date_str, market_key)] = max_part
        return max_part

    def _current_size(self, entry: _WriterEntry) -> int:
        try:
            return int(entry.path.stat().st_size)
        except Exception:
            try:
                return int(entry.file_handle.tell())
            except Exception:
                return 0

    def _open_new_entry(self, date_str: str, market_key: str, ts_iso: str) -> _WriterEntry:
        dir_path = self.output_root / date_str
        dir_path.mkdir(parents=True, exist_ok=True)
        start_stamp = self._start_stamp_from_ts_iso(ts_iso)
        while True:
            part = self._next_part(date_str, market_key)
            filename = self._build_filename(market_key, start_stamp, part)
            path = dir_path / filename
            try:
                fh = path.open("xb")
                break
            except FileExistsError:
                continue
        cctx = zstd.ZstdCompressor(level=self.zstd_level)
        stream = cctx.stream_writer(fh, closefd=False)
        return _WriterEntry(
            path=path,
            file_handle=fh,
            stream=stream,
            date_str=date_str,
            market_key=market_key,
            start_ts_iso=ts_iso,
            part=part,
            opened_monotonic=time.monotonic(),
            bytes_at_open=0,
        )

    def _close_entry(self, market_key: str, reason: str) -> Optional[UploadJob]:
        entry = self._active_entries.pop(market_key, None)
        if entry is None:
            return None
        try:
            entry.stream.flush(zstd.FLUSH_FRAME)
        except Exception:
            pass
        try:
            entry.stream.close()
        except Exception:
            pass
        try:
            entry.file_handle.close()
        except Exception:
            pass
        size_bytes = self._current_size(entry)
        log_event("writer_close", event_type_detail=reason, market=entry.market_key, queue_depth=None, upload_status="queued", lag=None, reconnect_count=None, file=entry.path.name, size_bytes=size_bytes, date=entry.date_str, part=entry.part)
        if size_bytes <= 0:
            return None
        job = UploadJob(
            local_path=entry.path,
            date_str=entry.date_str,
            market_key=entry.market_key,
            filename=entry.path.name,
            size_bytes=size_bytes,
        )
        if self._on_file_closed is not None:
            self._on_file_closed(job)
        return job

    def write_row(self, row: dict[str, Any]) -> None:
        ts_iso = str(row.get("ts_utc") or "")
        if not ts_iso:
            raise ValueError("row_missing_ts_utc")
        market_key = str(row.get("market_key") or "").strip()
        if not market_key:
            raise ValueError("row_missing_market_key")

        date_str = self._date_from_ts_iso(ts_iso)
        entry = self._active_entries.get(market_key)
        if entry is None:
            entry = self._open_new_entry(date_str, market_key, ts_iso)
            self._active_entries[market_key] = entry
        elif entry.date_str != date_str:
            self._close_entry(market_key, reason="utc_day_rollover")
            entry = self._open_new_entry(date_str, market_key, ts_iso)
            self._active_entries[market_key] = entry

        line = json.dumps(row, separators=(",", ":"), default=str) + "\n"
        entry.stream.write(line.encode("utf-8"))
        entry.stream.flush(zstd.FLUSH_BLOCK)
        size_bytes = self._current_size(entry)
        if size_bytes >= self.rotate_upload_threshold_bytes:
            self._close_entry(market_key, reason="rotate_threshold_reached")
            # Open the next part immediately so writes continue seamlessly.
            self._active_entries[market_key] = self._open_new_entry(date_str, market_key, ts_iso)

    def market_day_size(self, market_key: str, date_str: str) -> int:
        day_dir = self.output_root / date_str
        if not day_dir.exists():
            return 0
        total = 0
        patterns = [
            f"{market_key}__start_*__part_*.ndjson.zst",
            f"{market_key}.ndjson.zst",  # legacy files from pre-rotation runs
        ]
        for pattern in patterns:
            for path in day_dir.glob(pattern):
                if not path.is_file():
                    continue
                try:
                    total += int(path.stat().st_size)
                except Exception:
                    continue
        return int(total)

    def active_file_stats(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        now = time.monotonic()
        for market_key in sorted(self._active_entries.keys()):
            entry = self._active_entries[market_key]
            size_bytes = self._current_size(entry)
            age_s = max(0.001, now - float(entry.opened_monotonic))
            out.append(
                {
                    "market_key": market_key,
                    "file": entry.path.name,
                    "size_bytes": int(size_bytes),
                    "age_seconds": float(age_s),
                    "growth_bps": float(size_bytes - entry.bytes_at_open) / age_s,
                }
            )
        return out

    def close(self) -> None:
        for market_key in list(self._active_entries.keys()):
            self._close_entry(market_key, reason="shutdown")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone WS snapshot collector for Polymarket + Coinbase")
    parser.add_argument("--output-root", default=str(APP_ROOT / "outputs" / "ws_snapshots"), help="Output root directory")
    parser.add_argument("--duration-seconds", type=int, default=0, help="Run duration in seconds (0 = until Ctrl+C)")
    parser.add_argument("--snapshot-interval-seconds", type=int, default=1, help="Snapshot interval in seconds")
    parser.add_argument("--log-every-seconds", type=int, default=300, help="Progress/size log interval in seconds")
    return parser.parse_args(argv)


def build_collector_config(args: argparse.Namespace, runtime_cfg: RuntimeConfig) -> CollectorConfig:
    stale_timeout_raw = (os.getenv("WS_STALE_TIMEOUT_SECONDS") or "").strip()
    try:
        stale_timeout = float(stale_timeout_raw) if stale_timeout_raw else float(runtime_cfg.ws_stale_timeout_seconds)
    except ValueError:
        stale_timeout = float(runtime_cfg.ws_stale_timeout_seconds)
    stale_timeout = max(2.0, stale_timeout)

    backoff_raw = (os.getenv("WS_RECONNECT_BACKOFF_SECONDS") or "").strip()
    backoff = parse_backoff_seconds(backoff_raw, tuple(float(x) for x in runtime_cfg.ws_reconnect_backoff_seconds))

    target_mb_raw = (os.getenv("SIZE_TARGET_MB_PER_DAY") or "").strip()
    try:
        size_target_mb = float(target_mb_raw) if target_mb_raw else 5.0
    except ValueError:
        size_target_mb = 5.0
    size_target_mb = max(0.1, size_target_mb)

    zstd_level_raw = (os.getenv("ZSTD_LEVEL") or "").strip()
    try:
        zstd_level = int(zstd_level_raw) if zstd_level_raw else 3
    except ValueError:
        zstd_level = 3
    zstd_level = min(22, max(1, zstd_level))

    queue_size_raw = (os.getenv("WS_QUEUE_MAXSIZE") or "").strip()
    try:
        queue_maxsize = int(queue_size_raw) if queue_size_raw else 50_000
    except ValueError:
        queue_maxsize = 50_000
    queue_maxsize = max(1_000, queue_maxsize)

    graph_upload_enabled = parse_env_bool("GRAPH_UPLOAD_ENABLED", True)
    graph_client_id = normalize_env_scalar(os.getenv("GRAPH_CLIENT_ID"))
    graph_authority = normalize_env_scalar(os.getenv("GRAPH_AUTHORITY"))
    graph_scopes_raw = parse_graph_scopes(os.getenv("GRAPH_SCOPES"))
    graph_scopes, removed_reserved_scopes = sanitize_graph_scopes(graph_scopes_raw)
    onedrive_folder = normalize_onedrive_folder(os.getenv("ONEDRIVE_FOLDER"))

    rotate_threshold_raw = normalize_env_scalar(os.getenv("ROTATE_UPLOAD_THRESHOLD_BYTES"))
    try:
        rotate_threshold_bytes = int(rotate_threshold_raw) if rotate_threshold_raw else 3_800_000
    except ValueError:
        rotate_threshold_bytes = 3_800_000
    rotate_threshold_bytes = max(1_000_000, rotate_threshold_bytes)

    graph_max_raw = normalize_env_scalar(os.getenv("GRAPH_MAX_SINGLE_UPLOAD_BYTES"))
    try:
        graph_max_single_upload_bytes = int(graph_max_raw) if graph_max_raw else 4_000_000
    except ValueError:
        graph_max_single_upload_bytes = 4_000_000
    graph_max_single_upload_bytes = max(1_000_000, graph_max_single_upload_bytes)

    graph_timeout_raw = normalize_env_scalar(os.getenv("GRAPH_UPLOAD_TIMEOUT_SECONDS"))
    try:
        graph_upload_timeout_seconds = float(graph_timeout_raw) if graph_timeout_raw else 30.0
    except ValueError:
        graph_upload_timeout_seconds = 30.0
    graph_upload_timeout_seconds = max(3.0, graph_upload_timeout_seconds)

    graph_backoff_raw = normalize_env_scalar(os.getenv("GRAPH_UPLOAD_BACKOFF_SECONDS"))
    graph_upload_backoff_seconds = parse_backoff_seconds(
        graph_backoff_raw,
        (2.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0),
    )

    graph_token_cache_path = APP_ROOT / ".graph_token_cache.bin"
    health_host = normalize_env_scalar(os.getenv("HEALTH_HOST") or "0.0.0.0") or "0.0.0.0"
    try:
        health_port = int(normalize_env_scalar(os.getenv("HEALTH_PORT") or "8080"))
    except ValueError:
        health_port = 8080

    azure_workspace = normalize_env_scalar(os.getenv("AZURE_LOG_ANALYTICS_WORKSPACE_ID"))
    azure_shared_key = normalize_env_scalar(os.getenv("AZURE_LOG_ANALYTICS_SHARED_KEY"))
    azure_log_type = normalize_env_scalar(os.getenv("AZURE_LOG_ANALYTICS_LOG_TYPE") or "WsScraperEvents")

    alert_no_data_minutes = int(normalize_env_scalar(os.getenv("ALERT_NO_DATA_MINUTES") or "5") or "5")
    alert_reconnect_storm_count = int(normalize_env_scalar(os.getenv("ALERT_RECONNECT_STORM_COUNT") or "8") or "8")
    alert_queue_growth_minutes = int(normalize_env_scalar(os.getenv("ALERT_QUEUE_GROWTH_MINUTES") or "5") or "5")
    alert_export_failure_threshold = float(normalize_env_scalar(os.getenv("ALERT_EXPORT_FAILURE_THRESHOLD") or "0.1") or "0.1")

    if graph_upload_enabled:
        missing: list[str] = []
        if not graph_client_id:
            missing.append("GRAPH_CLIENT_ID")
        if not graph_authority:
            missing.append("GRAPH_AUTHORITY")
        if not graph_scopes_raw:
            missing.append("GRAPH_SCOPES")
        if not onedrive_folder:
            missing.append("ONEDRIVE_FOLDER")
        if missing:
            joined = ",".join(missing)
            raise RuntimeError(f"graph_upload_enabled_but_missing_env:{joined}")
        if not graph_scopes:
            raise RuntimeError(
                "graph_scopes_invalid_after_sanitization:"
                f"input={graph_scopes_raw},removed_reserved={removed_reserved_scopes}"
            )
        if removed_reserved_scopes:
            log_event("config_warning", warning="reserved_graph_scopes_removed", removed=removed_reserved_scopes)

    return CollectorConfig(
        output_root=Path(args.output_root),
        duration_seconds=max(0, int(args.duration_seconds)),
        snapshot_interval_seconds=max(1, int(args.snapshot_interval_seconds)),
        log_every_seconds=max(10, int(args.log_every_seconds)),
        ws_stale_timeout_seconds=stale_timeout,
        ws_reconnect_backoff_seconds=backoff,
        size_target_mb_per_day=size_target_mb,
        zstd_level=zstd_level,
        queue_maxsize=queue_maxsize,
        gamma_markets_url=str(runtime_cfg.gamma_markets_url),
        polymarket_ws_url=POLY_WS_URL,
        http_timeout_seconds=int(runtime_cfg.http_timeout_seconds),
        coinbase_products=COINBASE_PRODUCTS,
        graph_upload_enabled=graph_upload_enabled,
        graph_client_id=graph_client_id,
        graph_authority=graph_authority,
        graph_scopes=graph_scopes,
        onedrive_folder=onedrive_folder,
        rotate_upload_threshold_bytes=rotate_threshold_bytes,
        graph_max_single_upload_bytes=graph_max_single_upload_bytes,
        graph_upload_backoff_seconds=graph_upload_backoff_seconds,
        graph_upload_timeout_seconds=graph_upload_timeout_seconds,
        graph_token_cache_path=graph_token_cache_path,
        health_host=health_host,
        health_port=max(0, health_port),
        azure_log_analytics_workspace_id=azure_workspace,
        azure_log_analytics_shared_key=azure_shared_key,
        azure_log_analytics_log_type=azure_log_type,
        alert_no_data_minutes=max(1, alert_no_data_minutes),
        alert_reconnect_storm_count=max(1, alert_reconnect_storm_count),
        alert_queue_growth_minutes=max(1, alert_queue_growth_minutes),
        alert_export_failure_threshold=max(0.0, min(1.0, alert_export_failure_threshold)),
    )

def run(argv: Optional[list[str]] = None) -> None:
    load_dotenv(APP_ROOT / ".env", override=True)
    runtime_cfg = RuntimeConfig.from_env()
    args = parse_args(argv)
    cfg = build_collector_config(args, runtime_cfg)

    log_event(
        "collector_start",
        market="all",
        lag=0,
        reconnect_count=0,
        queue_depth=0,
        upload_status="starting",
        output_root=str(cfg.output_root),
        duration_seconds=cfg.duration_seconds,
    )

    health = HealthServer(cfg.health_host, cfg.health_port)
    health.start()
    azure = AzureLogAnalyticsClient(
        cfg.azure_log_analytics_workspace_id,
        cfg.azure_log_analytics_shared_key,
        cfg.azure_log_analytics_log_type,
    )

    event_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=cfg.queue_maxsize)
    aggregator = WsSnapshotAggregator()

    uploader: Optional[GraphUploader] = None
    if cfg.graph_upload_enabled:
        auth_manager = GraphAuthManager(
            client_id=cfg.graph_client_id,
            authority=cfg.graph_authority,
            scopes=cfg.graph_scopes,
            cache_path=cfg.graph_token_cache_path,
        )
        _ = auth_manager.acquire_access_token(allow_device_flow=True)
        uploader = GraphUploader(
            auth_manager=auth_manager,
            onedrive_folder=cfg.onedrive_folder,
            timeout_seconds=cfg.graph_upload_timeout_seconds,
            backoff_seconds=cfg.graph_upload_backoff_seconds,
            max_single_upload_bytes=cfg.graph_max_single_upload_bytes,
        )
        uploader.start()
        recovered_jobs = discover_pending_upload_jobs(cfg.output_root)
        for job in recovered_jobs:
            uploader.enqueue(job)
        if recovered_jobs:
            log_event("graph_upload_recovered", market="all", lag=None, reconnect_count=0, queue_depth=uploader.pending_count(), upload_status="recovered", files=len(recovered_jobs))

    writer = NdjsonZstdWriter(
        output_root=cfg.output_root,
        zstd_level=cfg.zstd_level,
        rotate_upload_threshold_bytes=cfg.rotate_upload_threshold_bytes,
        max_single_upload_bytes=cfg.graph_max_single_upload_bytes,
        on_file_closed=uploader.enqueue if uploader else None,
    )

    session = build_retrying_session()
    resolver = PolymarketMarketResolver(cfg, session)

    cb_driver = CoinbaseWsDriver(cfg, event_queue)
    pm_driver = PolymarketWsDriver(cfg, event_queue)

    cb_driver.connect()
    pm_driver.start()

    changed, metas, bindings, errors = resolver.refresh(int(time.time()))
    aggregator.update_polymarket_meta(metas)
    pm_driver.set_asset_bindings(bindings)
    if changed:
        log_event("polymarket_subscription", market="all", lag=0, reconnect_count=pm_driver.reconnect_count, queue_depth=event_queue.qsize(), upload_status="n/a", asset_ids=len(bindings))
    for err in errors:
        log_event("resolver_warning", market="all", lag=None, reconnect_count=0, queue_depth=event_queue.qsize(), upload_status="n/a", message=err)

    started = time.monotonic()
    next_resolve_ts = int(time.time())
    next_snapshot_ts = ts_floor(int(time.time()), cfg.snapshot_interval_seconds) + cfg.snapshot_interval_seconds
    next_log_ts = int(time.time()) + cfg.log_every_seconds
    queue_history: list[int] = []

    try:
        while True:
            if cfg.duration_seconds > 0 and (time.monotonic() - started) >= cfg.duration_seconds:
                log_event("collector_stop", market="all", lag=0, reconnect_count=0, queue_depth=event_queue.qsize(), upload_status="stopping", reason="duration_reached")
                break

            now_ts = int(time.time())
            queue_depth = int(event_queue.qsize())
            queue_history.append(queue_depth)
            max_points = max(1, cfg.alert_queue_growth_minutes * 20)
            queue_history = queue_history[-max_points:]

            if now_ts >= next_resolve_ts:
                changed, metas, bindings, errors = resolver.refresh(now_ts)
                aggregator.update_polymarket_meta(metas)
                if changed:
                    pm_driver.set_asset_bindings(bindings)
                    log_event("polymarket_subscription", market="all", lag=0, reconnect_count=pm_driver.reconnect_count, queue_depth=queue_depth, upload_status="n/a", asset_ids=len(bindings), markets=len(metas))
                for err in errors:
                    log_event("resolver_warning", market="all", lag=None, reconnect_count=0, queue_depth=queue_depth, upload_status="n/a", message=err)
                next_resolve_ts = now_ts + 1

            if cb_driver.seconds_since_message() > cfg.ws_stale_timeout_seconds:
                ok = cb_driver.reconnect()
                log_event("ws_reconnect", market="cb", lag=cb_driver.seconds_since_message(), reconnect_count=cb_driver.reconnect_count, queue_depth=queue_depth, upload_status="n/a", ok=ok)

            if pm_driver.seconds_since_message() > cfg.ws_stale_timeout_seconds:
                pm_driver.request_reconnect()
                log_event("ws_stale", market="pm", lag=pm_driver.seconds_since_message(), reconnect_count=pm_driver.reconnect_count, queue_depth=queue_depth, upload_status="n/a")

            drained = 0
            while drained < 20_000:
                try:
                    event = event_queue.get_nowait()
                except queue.Empty:
                    break
                aggregator.apply_event(event)
                drained += 1

            while next_snapshot_ts <= now_ts:
                dropped = cb_driver.collect_and_reset_dropped_counts()
                pm_dropped = pm_driver.collect_and_reset_dropped_counts()
                for market_key, count in pm_dropped.items():
                    dropped[market_key] = dropped.get(market_key, 0) + int(count)
                aggregator.apply_dropped_counts(dropped)

                rows = aggregator.build_snapshots(float(next_snapshot_ts), cfg.snapshot_interval_seconds)
                for row in rows:
                    writer.write_row(row)
                next_snapshot_ts += cfg.snapshot_interval_seconds

            if now_ts >= next_log_ts:
                day_start_ts = ts_floor(now_ts, 86400)
                elapsed = max(1.0, float(now_ts - day_start_ts))
                current_date = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d")
                target_bytes = cfg.size_target_mb_per_day * 1024.0 * 1024.0
                for market_key in sorted(aggregator.states.keys()):
                    size_bytes = writer.market_day_size(market_key, current_date)
                    projected = projected_daily_bytes(size_bytes, elapsed)
                    log_event("size_projection", market=market_key, lag=0, reconnect_count=cb_driver.reconnect_count + pm_driver.reconnect_count, queue_depth=queue_depth, upload_status="n/a", size_bytes=size_bytes, projected_day_bytes=int(projected), target_bytes=int(target_bytes), over_target=projected > target_bytes)

                upload_stats = uploader.stats_snapshot() if uploader is not None else {"pending_queue": 0, "uploaded_ok": 0, "failed_attempts": 0, "retry_attempts": 0, "success_rate": 1.0, "export_lag_seconds": 0}

                health.update(WorkerHeartbeat(worker="coinbase", ws_freshness_seconds=cb_driver.seconds_since_message(), queue_backlog=queue_depth, blob_write_success_rate=1.0, export_lag_seconds=0.0, reconnect_count=cb_driver.reconnect_count, last_update_unix=time.time()))
                health.update(WorkerHeartbeat(worker="polymarket", ws_freshness_seconds=pm_driver.seconds_since_message(), queue_backlog=queue_depth, blob_write_success_rate=1.0, export_lag_seconds=0.0, reconnect_count=pm_driver.reconnect_count, last_update_unix=time.time()))
                health.update(WorkerHeartbeat(worker="uploader", ws_freshness_seconds=0.0, queue_backlog=int(upload_stats["pending_queue"]), blob_write_success_rate=float(upload_stats["success_rate"]), export_lag_seconds=float(upload_stats["export_lag_seconds"]), reconnect_count=0, last_update_unix=time.time()))

                metric_event = {
                    "event_type": "worker_metrics",
                    "queue_depth": queue_depth,
                    "reconnect_count": cb_driver.reconnect_count + pm_driver.reconnect_count,
                    "upload_status": "ok",
                    "upload_success_rate": upload_stats["success_rate"],
                    "export_lag_seconds": upload_stats["export_lag_seconds"],
                    "ws_freshness_coinbase": cb_driver.seconds_since_message(),
                    "ws_freshness_polymarket": pm_driver.seconds_since_message(),
                }
                if azure.enabled:
                    ok, detail = azure.post_event(metric_event)
                    log_event("azure_log_analytics", market="all", lag=metric_event["export_lag_seconds"], reconnect_count=metric_event["reconnect_count"], queue_depth=queue_depth, upload_status="sent" if ok else "failed", detail=detail)

                oldest_event = min((s.last_event_ts or now_ts) for s in aggregator.states.values())
                no_data = (now_ts - oldest_event) >= (cfg.alert_no_data_minutes * 60)
                reconnect_storm = (cb_driver.reconnect_count + pm_driver.reconnect_count) >= cfg.alert_reconnect_storm_count
                queue_growth = len(queue_history) > 10 and queue_history[-1] > queue_history[0]
                export_fail = float(upload_stats["success_rate"]) < (1.0 - cfg.alert_export_failure_threshold)
                if no_data:
                    log_event("alert", market="all", lag=now_ts - oldest_event, reconnect_count=cb_driver.reconnect_count + pm_driver.reconnect_count, queue_depth=queue_depth, upload_status="n/a", alert_name="no_data_for_n_minutes")
                if reconnect_storm:
                    log_event("alert", market="all", lag=0, reconnect_count=cb_driver.reconnect_count + pm_driver.reconnect_count, queue_depth=queue_depth, upload_status="n/a", alert_name="reconnect_storm")
                if queue_growth:
                    log_event("alert", market="all", lag=0, reconnect_count=cb_driver.reconnect_count + pm_driver.reconnect_count, queue_depth=queue_depth, upload_status="n/a", alert_name="sustained_queue_growth")
                if export_fail:
                    log_event("alert", market="all", lag=upload_stats["export_lag_seconds"], reconnect_count=cb_driver.reconnect_count + pm_driver.reconnect_count, queue_depth=queue_depth, upload_status="failed", alert_name="onedrive_export_failure_threshold")

                next_log_ts = now_ts + cfg.log_every_seconds

            time.sleep(0.05)
    finally:
        try:
            cb_driver.close()
        finally:
            pm_driver.close()
            writer.close()
            if uploader is not None:
                uploader.close()
            health.close()



def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        log_event("stopped_by_user")


if __name__ == "__main__":
    main()
