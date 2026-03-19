# ws_scraper

Simple Polymarket + Coinbase websocket data siphon.

## Quick checks

```bash
ls -lh
```

## Service setup

The scraper now runs as a `systemd` service and starts automatically on boot.

It uses the repo virtualenv directly:

```bash
/home/axolotl/Documents/Projects/Data/.venv/bin/python /home/axolotl/Documents/Projects/Data/ws_scraper.py
```

Boot order is:

1. `network-online.target`
2. `wg-quick@pia.service`
3. `ws_scraper.service`

That ordering ensures the PIA WireGuard tunnel is brought up before the scraper starts.

## Manage the services

```bash
sudo systemctl status wg-quick@pia
sudo systemctl status ws_scraper
sudo systemctl status watchdog
```

Restart them manually if needed:

```bash
sudo systemctl restart wg-quick@pia
sudo systemctl restart ws_scraper
sudo systemctl restart watchdog
```

Enable them at boot:

```bash
sudo systemctl enable wg-quick@pia
sudo systemctl enable ws_scraper
sudo systemctl enable watchdog
```

Follow scraper logs live:

```bash
journalctl -u ws_scraper -f
```

## Notes

- The PIA WireGuard config is expected at `/etc/wireguard/pia.conf`.
- The scraper runs as user `axolotl` with working directory `/home/axolotl/Documents/Projects/Data`.
- `watchdog` is installed and configured to use `/dev/watchdog`.
