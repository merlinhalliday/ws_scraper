# ws_scraper

Simple Polymarket + Coinbase websocket data siphon.

## Fresh setup

```bash
cd ~/Documents/Projects
git clone <your-repo-url> Data
cd Data
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Create `.env` with the live credentials and runtime settings. The scheduled reboot feature is controlled by:

```bash
RESTART_SCHEDULE_S=21600
PM_USER_TRADES_ENABLED=true
```

Set `RESTART_SCHEDULE_S=0` to disable scheduled reboots.
Set `PM_USER_TRADES_ENABLED=false` to disable the parallel Polymarket user-trade dataset.

## Install the live services

The scraper runs from the repo virtualenv directly:

```bash
/home/axolotl/Documents/Projects/Data/.venv/bin/python /home/axolotl/Documents/Projects/Data/ws_scraper.py
```

Install or refresh the collector service:

```bash
sudo install -m 644 .ws_scraper.service.staged /etc/systemd/system/ws_scraper.service
sudo systemctl daemon-reload
sudo systemctl enable --now ws_scraper.service
```

Install or refresh the scheduled reboot setup:

```bash
./.venv/bin/python scripts/install_scheduled_reboot.py
```

Repo-managed sources for that setup are:

```bash
scripts/install_scheduled_reboot.py
scripts/ws_scraper_safe_reboot.sh
systemd/ws_scraper-scheduled-reboot.service
systemd/ws_scraper-scheduled-reboot.timer.template
```

That installer creates and manages:

```bash
/usr/local/sbin/ws_scraper_safe_reboot.sh
/etc/systemd/system/ws_scraper-scheduled-reboot.service
/etc/systemd/system/ws_scraper-scheduled-reboot.timer
```

The reboot timer is boot-relative and uses:

1. `network-online.target`
2. `ws_scraper.service`
3. `ws_scraper-scheduled-reboot.timer`

Its timer configuration is rendered from `.env` as:

```ini
OnBootSec=RESTART_SCHEDULE_S
OnUnitActiveSec=RESTART_SCHEDULE_S
```

The PIA WireGuard service is no longer part of the automatic boot sequence.

## Verify the setup

```bash
sudo systemctl status ws_scraper
sudo systemctl status ws_scraper-scheduled-reboot.timer
sudo systemctl status ws_scraper-scheduled-reboot.service
sudo systemctl status watchdog
systemctl list-timers --all ws_scraper-scheduled-reboot.timer
```

Follow logs:

```bash
journalctl -u ws_scraper -f
journalctl -t ws_scraper_safe_reboot -n 50 --no-pager
```

## Disable or change the schedule

Edit `.env`, change `RESTART_SCHEDULE_S`, then rerun:

```bash
./.venv/bin/python scripts/install_scheduled_reboot.py
```

Use `RESTART_SCHEDULE_S=0` to disable the reboot timer entirely.

## Manual controlled reboot test

This reboots the Pi immediately after safely stopping `ws_scraper.service`:

```bash
sudo systemctl start ws_scraper-scheduled-reboot.service
```

## Data handling during scheduled reboot

- `ws_scraper.service` still stops via `SIGINT`.
- The collector still exits through its existing shutdown path and closes active files with `Writer close reason=shutdown`.
- File naming stays unchanged: `market__start_YYYYMMDDTHHMMSSZ__part_NNNN.ndjson.zst`.
- Existing file size rotation and upload limits stay unchanged.
- Closed files that were not uploaded before reboot remain on disk.
- On the next startup, pending `.ndjson.zst` files are rediscovered and uploaded.
- Local files are still deleted only after a successful upload.

## Parallel Polymarket user-trade dataset

When `PM_USER_TRADES_ENABLED=true`, the scraper also writes a second dataset in parallel to the existing Coinbase + Polymarket snapshot files.

This v1 dataset captures public user-attributed **Polymarket trades** only. It does **not** capture public user-attributed order placements or cancellations, because those are not available for all users from the public endpoints.

The new files use the same:

- NDJSON + Zstandard format
- date-directory layout
- rotation thresholds
- Graph upload process
- local-file cleanup rules

New market keys are per asset and per outcome:

```bash
pm-usertrade-btc-up
pm-usertrade-btc-down
pm-usertrade-eth-up
pm-usertrade-eth-down
pm-usertrade-sol-up
pm-usertrade-sol-down
pm-usertrade-xrp-up
pm-usertrade-xrp-down
pm-usertrade-doge-up
pm-usertrade-doge-down
pm-usertrade-hype-up
pm-usertrade-hype-down
pm-usertrade-bnb-up
pm-usertrade-bnb-down
```

Each file keeps the same naming convention:

```bash
pm-usertrade-btc-up__start_YYYYMMDDTHHMMSSZ__part_NNNN.ndjson.zst
```

### Row types

Trade rows include:

- `row_type=trade`
- public user fields such as `proxy_wallet`, `name`, `pseudonym`
- trade fields such as `price`, `size`, `transaction_hash`, `side`
- market-window fields such as `window_t0_utc`, `window_open_utc`, `window_close_utc`, `relative_t_ms`, `window_phase`
- `collection_mode` showing whether the row came from `live_poll`, `startup_backfill`, or `overlap_backfill`
- `capture_ok` and `capture_reason`

Audit rows include:

- `row_type=audit`
- the same market/window identity fields
- `capture_ok`, `capture_reason`, `suspected_gap`
- `api_error_count`, `pages_fetched`, `rows_written`, `rows_deduped`, `queue_overflow_count`, `poll_lag_ms_max`, `backfill_applied`

`capture_ok=false` means the bounded collector could not be fully confident that the window was complete. In practice this is where you should trust the data less during training or analysis.

### Restart behavior

- The scheduled reboot units are unchanged.
- The user-trade dataset runs under the same `ws_scraper.service` entrypoint, but with its own internal poller thread, queue, dedupe state, and writer.
- On startup after a reboot, the collector backfills the previous/current/next 5-minute windows before continuing live polling.
- If the bounded backfill cannot fully reach the requested window coverage within API/hardware limits, the final audit row marks that window with `capture_ok=false` rather than silently pretending it is complete.

### Verify both datasets are flowing

Use the normal service checks:

```bash
sudo systemctl status ws_scraper
sudo systemctl status ws_scraper-scheduled-reboot.timer
```

Then look for the new collector in the journal:

```bash
journalctl -u ws_scraper -n 200 --no-pager | grep -E "pm_user_trades|Polymarket user-trade"
```

Expected lines include:

- `Polymarket user-trade collector started.`
- `Polymarket user-trade backfill mode=...`
- `pm_user_trades active_conditions=... tracked_windows=...`

You can also confirm files on disk:

```bash
find outputs/ws_snapshots -type f -name 'pm-usertrade-*.ndjson.zst' | sort | tail
```

## Notes

- `wg-quick@pia.service` has been removed from automatic startup, so a reboot will not bring the VPN back up by itself.
- The scraper runs as user `axolotl` with working directory `/home/axolotl/Documents/Projects/Data`.
- `watchdog` is installed and configured to use `/dev/watchdog`.
- If you first install the reboot timer on a machine that has already been up longer than `RESTART_SCHEDULE_S`, the installer enables the timer but does not start it immediately, to avoid an accidental instant reboot. After the next boot, it runs normally.
