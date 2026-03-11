from __future__ import annotations

from ws_scraper.app.worker import run
from ws_scraper.auth.graph_token import GraphAuthManager
from ws_scraper.ingest.coinbase_ws import CoinbaseWsDriver
from ws_scraper.ingest.polymarket_ws import PolymarketMarketResolver, PolymarketWsDriver
from ws_scraper.pipeline.snapshot_aggregator import WsSnapshotAggregator
from ws_scraper.sink.blob_writer import NdjsonZstdWriter
from ws_scraper.sink.onedrive_exporter import GraphUploader


def _dependency_graph() -> dict[str, object]:
    return {
        "aggregator": WsSnapshotAggregator,
        "coinbase_driver": CoinbaseWsDriver,
        "polymarket_driver": PolymarketWsDriver,
        "market_resolver": PolymarketMarketResolver,
        "blob_writer": NdjsonZstdWriter,
        "graph_auth": GraphAuthManager,
        "graph_uploader": GraphUploader,
    }


def main() -> None:
    _dependency_graph()
    run()


if __name__ == "__main__":
    main()
