#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

try:
    import msal
except Exception as exc:  # pragma: no cover
    raise SystemExit("missing_dependency:msal (pip install msal)") from exc


def _normalize(value: str | None) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1].strip()
    return text


def _parse_scopes(raw: str | None) -> list[str]:
    text = _normalize(raw)
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [_normalize(str(x)) for x in parsed if _normalize(str(x))]
        except Exception:
            pass
    return [token.strip().strip("[]").strip(",") for token in text.replace(",", " ").split() if token.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Local-only Microsoft Graph device-code bootstrap utility")
    parser.add_argument("--env-file", default=str(Path(__file__).resolve().parents[1] / ".env"))
    parser.add_argument("--client-id", default="")
    parser.add_argument("--authority", default="")
    parser.add_argument("--scopes", default="")
    args = parser.parse_args()

    load_dotenv(args.env_file, override=False)

    client_id = _normalize(args.client_id) or _normalize(os.getenv("GRAPH_CLIENT_ID"))
    authority = _normalize(args.authority) or _normalize(os.getenv("GRAPH_AUTHORITY"))
    scopes = _parse_scopes(args.scopes or os.getenv("GRAPH_SCOPES"))

    if not client_id or not authority or not scopes:
        raise SystemExit("missing_required_input:client_id,authority,scopes")

    app = msal.PublicClientApplication(client_id=client_id, authority=authority)
    flow = app.initiate_device_flow(scopes=scopes)
    if not isinstance(flow, dict) or not flow.get("user_code"):
        raise SystemExit(f"device_flow_init_failed:{flow}")

    print(str(flow.get("message") or "").strip())
    result = app.acquire_token_by_device_flow(flow)
    token = str((result or {}).get("access_token") or "").strip()
    if not token:
        detail = str((result or {}).get("error_description") or (result or {}).get("error") or "unknown_error")
        raise SystemExit(f"device_flow_failed:{detail}")

    print("device_flow_success")


if __name__ == "__main__":
    main()
