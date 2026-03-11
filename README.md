# ws_scraper

Simple Polymarket + Coinbase websocket data siphon.

## Worker architecture

The scraper is split into container-friendly workers:

- `app/ingest_worker.py`: receives websocket events, builds snapshot batches, and publishes batches to Azure Queue Storage by default (or Azure Service Bus when `WORKER_QUEUE_BACKEND=servicebus`).
- `app/persist_worker.py`: consumes queued batches and writes immutable `ndjson.zst` blobs to Azure Blob Storage under `raw/ndjson-zstd/date=.../market=...`.
- `app/export_worker.py`: optional scheduled job that syncs selected outputs to OneDrive.

Graceful shutdown is implemented with `SIGINT`/`SIGTERM` handlers and JSON checkpoint files in `outputs/worker_state`. The persist worker only acknowledges queue messages after successful blob upload.

## Environment variables (workers)

- Queue backend
  - `WORKER_QUEUE_BACKEND=azure-storage|servicebus|memory`
  - `WORKER_QUEUE_NAME` (default: `ws-snapshot-batches`)
  - `AZURE_STORAGE_QUEUE_CONNECTION_STRING` (for Azure Queue Storage)
  - `AZURE_SERVICEBUS_CONNECTION_STRING` (for Service Bus)
- Blob persistence
  - `AZURE_BLOB_CONNECTION_STRING`
  - `AZURE_BLOB_CONTAINER` (default: `ws-snapshots`)
- Export (OneDrive)
  - `GRAPH_CLIENT_ID`, `GRAPH_AUTHORITY`, `GRAPH_SCOPES`, `ONEDRIVE_FOLDER`
  - `EXPORT_SYNC_INTERVAL_SECONDS` (default: `300`)

## Run workers

```bash
python -m app.ingest_worker
python -m app.persist_worker
python -m app.export_worker
```

## Run legacy single-process collector

```bash
python ws_scraper.py
```
