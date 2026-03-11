# ws_scraper

Simple Polymarket + Coinbase websocket data siphon.

## Observability and health

The collector now emits structured JSON logs to stdout with standard fields:

- `event_type`
- `market`
- `lag`
- `reconnect_count`
- `queue_depth`
- `upload_status`

### Health/readiness endpoints

Set `HEALTH_HOST` and `HEALTH_PORT` (default `0.0.0.0:8080`) to expose:

- `GET /health` (`/healthz`, `/metrics` aliases): worker heartbeat payload.
- `GET /ready` (`/readyz` alias): returns `200` when websocket freshness is healthy, else `503`.

Per-worker heartbeat metrics include:

- websocket connectivity freshness (`ws_freshness_seconds`)
- queue backlog (`queue_backlog`)
- blob write success rate (`blob_write_success_rate`)
- export lag (`export_lag_seconds`)

### Azure Monitor / Log Analytics integration

Set:

- `AZURE_LOG_ANALYTICS_WORKSPACE_ID`
- `AZURE_LOG_ANALYTICS_SHARED_KEY`
- optional `AZURE_LOG_ANALYTICS_LOG_TYPE` (default `WsScraperEvents`)

When configured, periodic worker metric events are posted to Log Analytics.

### Alert thresholds (env-configurable)

- `ALERT_NO_DATA_MINUTES` (default `5`)
- `ALERT_RECONNECT_STORM_COUNT` (default `8`)
- `ALERT_QUEUE_GROWTH_MINUTES` (default `5`)
- `ALERT_EXPORT_FAILURE_THRESHOLD` (default `0.1`)

The collector emits `event_type="alert"` with these alert names:

- `no_data_for_n_minutes`
- `reconnect_storm`
- `sustained_queue_growth`
- `onedrive_export_failure_threshold`

### Example KQL alert queries

```kusto
WsScraperEvents_CL
| where event_type_s == "alert"
| where alert_name_s == "no_data_for_n_minutes"
```

```kusto
WsScraperEvents_CL
| where event_type_s == "alert"
| where alert_name_s == "reconnect_storm"
```

```kusto
WsScraperEvents_CL
| where event_type_s == "alert"
| where alert_name_s == "sustained_queue_growth"
```

```kusto
WsScraperEvents_CL
| where event_type_s == "alert"
| where alert_name_s == "onedrive_export_failure_threshold"
```
