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
