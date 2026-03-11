from __future__ import annotations

import json
import re
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


BTC_5M_SLUG_RE = re.compile(r"^btc-updown-5m-(\d+)$")
HB_TIME_RE = re.compile(
    r"^(?P<d>\d{4}-\d{2}-\d{2}) (?P<t>\d{2}:\d{2}:\d{2})(?:\.(?P<f>\d+))? (?P<z>[+-]\d{4})"
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: Optional[datetime]) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).isoformat()


def ts_floor_5m(ts: int, window_seconds: int = 300) -> int:
    return ts - (ts % window_seconds)


def ts_floor_1m(ts: int, window_seconds: int = 60) -> int:
    return ts - (ts % window_seconds)


def slug_from_start_ts(start_ts: int) -> str:
    return f"btc-updown-5m-{start_ts}"


def as_int_sign(value: Optional[float]) -> Optional[int]:
    if value is None:
        return None
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def sign_to_label(value: Optional[int]) -> str:
    if value == 1:
        return "Up"
    if value == -1:
        return "Down"
    if value == 0:
        return "Flat"
    return ""


def label_to_sign(label: str) -> Optional[int]:
    val = str(label or "").strip().lower()
    if val == "up":
        return 1
    if val == "down":
        return -1
    if val == "flat":
        return 0
    return None


def parse_iso8601(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def parse_coinbase_heartbeat_time(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    m = HB_TIME_RE.match(text)
    if not m:
        return None
    frac = (m.group("f") or "")[:6]
    frac = frac.ljust(6, "0") if frac else ""
    z = m.group("z")
    z = f"{z[:3]}:{z[3:]}"
    if frac:
        stamp = f"{m.group('d')}T{m.group('t')}.{frac}{z}"
    else:
        stamp = f"{m.group('d')}T{m.group('t')}{z}"
    try:
        return datetime.fromisoformat(stamp).astimezone(timezone.utc)
    except ValueError:
        return None


def as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except json.JSONDecodeError:
            return [text]
    return []


def coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.10f}".rstrip("0").rstrip(".")
    return str(value)


def compact_json(value: Any, max_len: int = 1200) -> str:
    try:
        out = json.dumps(value, separators=(",", ":"), default=str)
    except Exception:
        out = str(value)
    if len(out) <= max_len:
        return out
    return out[: max_len - 3] + "..."


def mask_addr(value: str) -> str:
    text = str(value or "")
    if len(text) <= 10:
        return text
    return f"{text[:6]}...{text[-4:]}"


def parse_boolish(value: Any) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "y", "on")


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        return default
    return parse_boolish(value)


def normalize_private_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not text.startswith("0x"):
        text = "0x" + text
    return text


def is_valid_private_key(value: str) -> bool:
    text = normalize_private_key(value)
    if not text:
        return False
    payload = text[2:]
    if len(payload) != 64:
        return False
    try:
        int(payload, 16)
        return True
    except ValueError:
        return False


def normalize_condition_id(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if text.startswith("0x"):
        text = text[2:]
    if len(text) != 64:
        return ""
    try:
        int(text, 16)
    except ValueError:
        return ""
    return "0x" + text


def ensure_module_from_venv(module_name: str, module_file: str, venv_path: Path) -> None:
    """
    Fail fast when imports are shadowed by project-root package directories.
    """
    file_path = Path(module_file).resolve()
    if venv_path.resolve() not in file_path.parents:
        raise RuntimeError(
            f"{module_name} import is not from active .venv: {file_path}. "
            "Remove/quarantine root-level shadowing package directories."
        )
