# Change Request: Cloud-Grade Graph Export, Analytics Layout, and Observability Hardening

## Context (post-merge review)
The worker split and queue/blob foundations are present, but several reliability and cloud-readiness gaps remain against objectives 3–6:

- `GraphAuthManager` still supports device-code flow (`allow_device_flow=True`) and prints interactive prompts, which is unsuitable for unattended runtime.
- OneDrive export still depends on an in-process uploader queue (`GraphUploader`) and periodic filesystem scans; this is not a durable, replay-safe export pipeline.
- Data persistence is currently raw `ndjson.zst` only (`raw/ndjson-zstd/...`) with no Bronze/Silver promotion to Parquet.
- Observability is mostly `print(...)` statements with no structured logging, metrics, or health endpoints for worker operations.

## Single consolidated task

### Implement a production-grade **Cloud Export & Observability Platform** across workers

Deliver one integrated change that removes interactive Graph auth, makes OneDrive delivery durable and decoupled, adds Bronze→Silver conversion, and introduces operational telemetry.

#### Scope and acceptance criteria

1. **Non-interactive Graph identity (Objective 3)**
   - Replace runtime device-code auth with cloud-safe identity:
     - Primary path: Azure Managed Identity / Workload Identity or confidential-client credentials.
     - Local-dev fallback may remain, but only behind explicit local mode and never in cloud default paths.
   - Remove/disable startup behavior that blocks on `allow_device_flow=True` in unattended workers.
   - Add clear config contract for identity mode selection and required secrets/permissions.

2. **Durable, decoupled OneDrive export (Objective 4)**
   - Eliminate in-memory upload queue dependency for reliability-critical delivery.
   - Persist export intents in durable infrastructure (queue/topic + checkpoint state) and process with at-least-once semantics.
   - Make export idempotent (content hash or object key ledger) to prevent duplicates on retries/restarts.
   - Ensure ingest/persist workers never block on Graph availability.

3. **Bronze/Silver data architecture with Parquet (Objective 5)**
   - Keep existing compressed raw output as Bronze.
   - Add Silver conversion job that reads Bronze and writes partitioned Parquet (`date/source/market[/hour]`).
   - Define and enforce a stable schema contract for Parquet outputs, including numeric typing and timestamp normalization.

4. **Operational observability baseline (Objective 6)**
   - Introduce structured JSON logs across ingest/persist/export/transform workers.
   - Emit minimal metrics (e.g., WS lag, queue depth, persisted batches, export failures, transform lag).
   - Add health/readiness probes (or equivalent heartbeat artifacts) for each long-running worker.
   - Document alert thresholds for “no data”, backlog growth, and repeated export/auth failures.

5. **Done definition**
   - A full unattended run in cloud mode performs ingest→persist→transform→export without interactive auth.
   - Crash/restart simulations show no data loss and bounded duplicate exports.
   - At least one documented runbook explains failure modes and recovery steps.

## Suggested implementation slices (single change request, staged delivery)
1. Identity abstraction + cloud auth mode
2. Durable export queue + idempotent exporter
3. Bronze→Silver Parquet transform worker
4. Structured logs/metrics/health + docs
