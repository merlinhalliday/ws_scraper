# ws_scraper

Simple Polymarket + Coinbase websocket data siphon.

## Worker architecture

The scraper is split into container-friendly workers:

- `app/ingest_worker.py`: receives websocket events, builds per-market snapshot batches, and publishes them to the ingest queue.
- `app/persist_worker.py`: consumes ingest batches and writes immutable Bronze `ndjson.zst` blobs to Azure Blob Storage, then emits `BronzeWritten`.
- `app/transform_worker.py`: consumes `BronzeWritten`, normalizes rows into a stable typed schema, and writes Silver Parquet.
- `app/export_worker.py`: stages Silver files for export and emits relay intents.
- `app/local_relay_worker.py`: local-only worker that mirrors staged exports from Azure Blob into a personal OneDrive target.

Every worker emits structured JSON logs, serves `/healthz`, `/readyz`, and `/metrics`, and checkpoints progress in `outputs/worker_state`.

## Environment variables (workers)

- Runtime
  - `APP_ENV=cloud|local`
  - `IDENTITY_MODE=managed_identity|client_secret|delegated_local`
  - `QUEUE_BACKEND=servicebus|azure-storage|memory`
- Queues
  - `INGEST_QUEUE_NAME` (default: `ws-snapshot-batches`)
  - `TRANSFORM_QUEUE_NAME` (default: `ws-bronze-written`)
  - `EXPORT_QUEUE_NAME` (default: `ws-export-intents`)
  - `RELAY_QUEUE_NAME` (default: `ws-relay-intents`)
  - `AZURE_SERVICEBUS_CONNECTION_STRING` or `AZURE_SERVICEBUS_FQDN`
  - `AZURE_STORAGE_QUEUE_CONNECTION_STRING` when `QUEUE_BACKEND=azure-storage`
- Blob storage
  - `AZURE_BLOB_CONNECTION_STRING` or `AZURE_BLOB_ACCOUNT_URL`
  - `AZURE_BLOB_RAW_CONTAINER`
  - `AZURE_BLOB_SILVER_CONTAINER`
  - `AZURE_BLOB_EXPORT_CONTAINER`
  - `AZURE_BLOB_LEDGER_CONTAINER`
- Local relay only
  - `EXPORT_MODE=personal_onedrive_local`
  - `GRAPH_CLIENT_ID`, `GRAPH_AUTHORITY`, `GRAPH_SCOPES`, `ONEDRIVE_FOLDER`
- Observability
  - `OBS_HOST`, `OBS_PORT`, `WORKER_HEARTBEAT_TIMEOUT_SECONDS`, `LOG_LEVEL`

## Run workers

```bash
python -m app.ingest_worker
python -m app.persist_worker
python -m app.transform_worker
python -m app.export_worker
python -m app.local_relay_worker
```

## Run legacy single-process collector

```bash
python ws_scraper.py
```

## Deployment

See `docs/deployment_instructions.md` for Azure Container Apps deployment, test steps, and failure recovery guidance.
