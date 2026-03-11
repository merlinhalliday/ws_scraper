# ws_scraper

Simple Polymarket + Coinbase websocket data siphon.

## Data lake layout (Bronze ➜ Silver)

This repo now supports a two-layer storage model for Azure Blob/Data Lake:

- **Bronze (raw immutable capture):** keep original `*.ndjson.zst` snapshots exactly as written by `ws_scraper.py`.
  - Recommended path prefix: `bronze/ws_snapshots/...`
  - Do not mutate/delete recent Bronze files; use them for replay/debug/backfill.
- **Silver (analytics optimized):** scheduled transform writes typed Parquet partitions.
  - Path prefix: `silver/parquet/date=.../source=.../market_key=...[/hour=...]`
  - Designed for low-cost scans from Azure Data Explorer / Synapse / Fabric.

## Collector

Run the collector to produce Bronze NDJSON+Zstd:

```bash
python ws_scraper.py --output-root outputs/ws_snapshots
```

The output is your immutable Bronze layer. Keep it as-is.

## Scheduled Silver transform job

Install dependencies:

```bash
pip install -r requirements.txt
```

Run once (batch mode):

```bash
python silver_transform.py \
  --bronze-root outputs/ws_snapshots \
  --silver-root outputs/silver \
  --run-once
```

Run continuously on a schedule (every 5 minutes):

```bash
python silver_transform.py \
  --bronze-root outputs/ws_snapshots \
  --silver-root outputs/silver \
  --poll-interval-seconds 300
```

Optional hourly partitioning:

```bash
python silver_transform.py --partition-by-hour
```

### Silver partition keys

- Required: `date`, `source`, `market_key`
- Optional: `hour` (`--partition-by-hour`)

### Schema normalization

`silver_transform.py` enforces a stable schema and numeric typing:

- Timestamps: `ts_utc` as UTC Parquet timestamp
- Count columns: integer types
- Price/spread/size columns: float64
- String dimensions and IDs remain string typed

This removes mixed `""`/number types present in raw Bronze snapshots.

## Lifecycle guidance (Azure)

Keep Bronze for replay/debug and recovery, but tier older objects automatically:

- Hot tier for newest Bronze and active Silver partitions
- Cool tier after your operational window (for example 7–30 days)
- Archive tier for long retention / compliance history

Apply lifecycle rules by prefix (for example `bronze/` vs `silver/`) so raw capture remains available while storage cost stays controlled.

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
