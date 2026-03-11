from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class RuntimeConfig:
    gamma_markets_url: str
    http_timeout_seconds: int
    ws_stale_timeout_seconds: int
    ws_reconnect_backoff_seconds: tuple[int, int, int]

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        gamma_markets_url = (os.getenv("GAMMA_MARKETS_URL") or "https://gamma-api.polymarket.com/markets").strip()

        try:
            http_timeout_seconds = int((os.getenv("HTTP_TIMEOUT_SECONDS") or "8").strip())
        except ValueError:
            http_timeout_seconds = 8

        try:
            ws_stale_timeout_seconds = int((os.getenv("WS_STALE_TIMEOUT_SECONDS") or "10").strip())
        except ValueError:
            ws_stale_timeout_seconds = 10

        raw_backoff = (os.getenv("WS_RECONNECT_BACKOFF_SECONDS") or "1,2,5").strip()
        values: list[int] = []
        for token in raw_backoff.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                values.append(int(token))
            except ValueError:
                continue
        if not values:
            values = [1, 2, 5]
        return cls(
            gamma_markets_url=gamma_markets_url,
            http_timeout_seconds=max(1, http_timeout_seconds),
            ws_stale_timeout_seconds=max(2, ws_stale_timeout_seconds),
            ws_reconnect_backoff_seconds=tuple(values[:3]) if len(values) >= 3 else (values + [5])[:3],
        )
