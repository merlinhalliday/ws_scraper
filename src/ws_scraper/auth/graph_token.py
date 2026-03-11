from __future__ import annotations

from pathlib import Path

from ws_scraper.auth.identity import LocalGraphAuthManager


class GraphAuthManager(LocalGraphAuthManager):
    """
    Backward-compatible wrapper for legacy imports.
    """

    def __init__(self, client_id: str, authority: str, scopes: tuple[str, ...], cache_path: Path, **_: object):
        super().__init__(
            client_id=client_id,
            authority=authority,
            scopes=scopes,
            cache_path=Path(cache_path),
        )

    def acquire_access_token(self, allow_device_flow: bool = True) -> str:
        del allow_device_flow
        return super().acquire_access_token()


__all__ = ["GraphAuthManager", "LocalGraphAuthManager"]
