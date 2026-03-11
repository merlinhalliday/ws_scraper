# ws_scraper

Simple Polymarket + Coinbase websocket data siphon.

## Quick checks

```bash
ls -lh
```

## Run with tmux

```bash
sudo apt install tmux
tmux new -s scraper
cd ~/Documents/Projects/Data
source .venv/bin/activate
python ws_scraper.py
```

To leave it running and disconnect, press `Ctrl+b` then `d`.

Later, reconnect and reattach with:

```bash
tmux attach -t scraper
```

To inspect or stop tmux sessions:

```bash
tmux ls
tmux kill-session -t scraper
```


## Azure-first canonical persistence + decoupled OneDrive export

The collector now persists rotated `.ndjson.zst` files to Azure Blob Storage first (system of record), then enqueues export messages for a separate Graph export worker.

### Required env vars

- `AZURE_BLOB_ENABLED=1`
- `AZURE_STORAGE_CONNECTION_STRING=...`
- `AZURE_BLOB_CONTAINER=ws-canonical`
- `AZURE_BLOB_PREFIX=ws-snapshots`
- `AZURE_EXPORT_QUEUE_NAME=ws-export-jobs`

### Optional export-worker env vars

- `EXPORT_JOB_ENABLED=1` (defaults to `GRAPH_UPLOAD_ENABLED`)
- `EXPORT_POISON_ATTEMPTS=5`
- `AZURE_BLOB_DEAD_LETTER_CONTAINER=ws-export-dead-letter`
- Existing Graph auth envs (`GRAPH_CLIENT_ID`, `GRAPH_AUTHORITY`, `GRAPH_SCOPES`, `ONEDRIVE_FOLDER`)

### Behavior

- Ingestion writes local rotated files, uploads them to Azure Blob, and enqueues an export message.
- OneDrive export runs independently off Azure Queue messages.
- Export state tracks `blob_name -> etag/hash/upload status` for idempotency.
- Failed export messages are retried with backoff and moved to dead-letter blob storage after poison threshold.
- Graph outages do not block websocket scraping or canonical persistence.
