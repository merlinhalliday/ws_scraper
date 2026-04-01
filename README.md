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
```

Set `RESTART_SCHEDULE_S=0` to disable scheduled reboots.

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

## Notes

- `wg-quick@pia.service` has been removed from automatic startup, so a reboot will not bring the VPN back up by itself.
- The scraper runs as user `axolotl` with working directory `/home/axolotl/Documents/Projects/Data`.
- `watchdog` is installed and configured to use `/dev/watchdog`.
- If you first install the reboot timer on a machine that has already been up longer than `RESTART_SCHEDULE_S`, the installer enables the timer but does not start it immediately, to avoid an accidental instant reboot. After the next boot, it runs normally.
