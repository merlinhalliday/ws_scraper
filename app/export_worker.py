from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from dotenv import load_dotenv

from polygale.config import APP_ROOT
from ws_scraper import GraphAuthManager, GraphUploader, discover_pending_upload_jobs, normalize_onedrive_folder, parse_backoff_seconds

from app.worker_common import JsonCheckpoint, Shutdown, env_float


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scheduled sync of selected outputs to OneDrive")
    parser.add_argument("--source-root", default=str(APP_ROOT / "outputs" / "ws_snapshots"))
    parser.add_argument("--checkpoint-path", default=str(APP_ROOT / "outputs" / "worker_state" / "export_checkpoint.json"))
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> None:
    load_dotenv(APP_ROOT / ".env", override=True)
    args = parse_args(argv)

    client_id = (os.getenv("GRAPH_CLIENT_ID") or "").strip()
    authority = (os.getenv("GRAPH_AUTHORITY") or "").strip()
    scopes = tuple((os.getenv("GRAPH_SCOPES") or "Files.ReadWrite.All").replace(",", " ").split())
    onedrive_folder = normalize_onedrive_folder(os.getenv("ONEDRIVE_FOLDER") or "/ws-snapshots")
    if not (client_id and authority and onedrive_folder):
        raise RuntimeError("GRAPH_CLIENT_ID, GRAPH_AUTHORITY, and ONEDRIVE_FOLDER are required for export worker")

    backoff = parse_backoff_seconds(os.getenv("GRAPH_UPLOAD_BACKOFF_SECONDS") or "", (2.0, 5.0, 15.0, 30.0))
    timeout_seconds = env_float("GRAPH_UPLOAD_TIMEOUT_SECONDS", 30.0, minimum=3.0)
    token_cache = APP_ROOT / ".graph_token_cache.bin"

    auth = GraphAuthManager(
        client_id=client_id,
        authority=authority,
        scopes=scopes,
        token_cache_path=token_cache,
        persist_cache=True,
    )
    uploader = GraphUploader(
        auth_manager=auth,
        onedrive_folder=onedrive_folder,
        timeout_seconds=timeout_seconds,
        backoff_seconds=backoff,
        max_single_upload_bytes=int(os.getenv("GRAPH_MAX_SINGLE_UPLOAD_BYTES") or "4000000"),
    )

    interval_s = env_float("EXPORT_SYNC_INTERVAL_SECONDS", 300.0, minimum=30.0)
    source_root = Path(args.source_root)
    checkpoint = JsonCheckpoint(Path(args.checkpoint_path))
    shutdown = Shutdown()
    shutdown.install()

    uploader.start()
    try:
        while not shutdown.requested():
            jobs = discover_pending_upload_jobs(source_root)
            for job in jobs:
                uploader.enqueue(job)
            checkpoint.save({"last_scan_at": int(time.time()), "jobs_enqueued": len(jobs)})
            shutdown.wait(interval_s)
    finally:
        uploader.close()


if __name__ == "__main__":
    run()
