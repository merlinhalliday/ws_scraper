from __future__ import annotations

import queue
import tempfile
import unittest
from collections import OrderedDict
from pathlib import Path

import ws_scraper


def _hex_id(width: int, suffix: str) -> str:
    return "0x" + str(suffix).lower().rjust(width, "0")[-width:]


def make_meta(symbol: str = "btc", bucket_start_ts: int = 2_000_000_000, suffix: str = "1") -> ws_scraper.PolymarketMarketMeta:
    slug = ws_scraper.market_slug(symbol, bucket_start_ts)
    return ws_scraper.PolymarketMarketMeta(
        symbol=symbol,
        market_key=ws_scraper.POLY_MARKET_KEY[symbol],
        slug=slug,
        resolver_returned_slug=slug,
        bucket_start_ts=bucket_start_ts,
        condition_id=_hex_id(64, suffix),
        yes_asset_id=f"asset-up-{suffix}",
        no_asset_id=f"asset-down-{suffix}",
    )


def make_trade(
    condition_id: str,
    asset_id: str,
    ts: float,
    wallet_suffix: str,
    tx_suffix: str,
    outcome: str = "up",
    outcome_index: int = 0,
) -> dict[str, object]:
    return {
        "conditionId": condition_id,
        "asset": asset_id,
        "timestamp": float(ts),
        "proxyWallet": _hex_id(40, wallet_suffix),
        "price": 0.55,
        "size": 10.0,
        "side": "BUY",
        "transactionHash": _hex_id(64, tx_suffix),
        "outcome": outcome,
        "outcomeIndex": outcome_index,
        "name": "",
        "pseudonym": "",
    }


class FakeResponse:
    def __init__(self, payload, ok: bool = True, status_code: int = 200, text: str = ""):
        self._payload = payload
        self.ok = ok
        self.status_code = status_code
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, responses: dict[tuple[str, str | None, int], list[FakeResponse]]):
        self._responses = {key: list(value) for key, value in responses.items()}
        self.calls: list[tuple[str, str | None, int]] = []

    def get(self, url, params, timeout):
        key = (
            str(params.get("market") or ""),
            params.get("user"),
            int(params.get("offset") or 0),
        )
        self.calls.append(key)
        queue_for_key = self._responses.get(key)
        if not queue_for_key:
            raise AssertionError(f"unexpected request: {key}")
        return queue_for_key.pop(0)


class PolymarketUserTradeCollectorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._original_zstd = ws_scraper.zstd
        if ws_scraper.zstd is None:
            ws_scraper.zstd = object()

    @classmethod
    def tearDownClass(cls) -> None:
        ws_scraper.zstd = cls._original_zstd

    def make_config(self, output_root: Path, **overrides) -> ws_scraper.CollectorConfig:
        values = {
            "output_root": output_root,
            "duration_seconds": 0,
            "snapshot_interval_seconds": 1,
            "log_every_seconds": 60,
            "ws_stale_timeout_seconds": 10.0,
            "ws_reconnect_backoff_seconds": (1.0, 2.0, 5.0),
            "size_target_mb_per_day": 5.0,
            "zstd_level": 3,
            "queue_maxsize": 1000,
            "gamma_markets_url": "https://gamma-api.polymarket.com/markets",
            "polymarket_ws_url": ws_scraper.POLY_WS_URL,
            "http_timeout_seconds": 5,
            "coinbase_products": ws_scraper.COINBASE_PRODUCTS,
            "graph_upload_enabled": False,
            "graph_client_id": "",
            "graph_authority": "",
            "graph_scopes": (),
            "onedrive_folder": "",
            "rotate_upload_threshold_bytes": 1_000_000,
            "graph_max_single_upload_bytes": 1_000_000,
            "graph_upload_backoff_seconds": (1.0,),
            "graph_upload_timeout_seconds": 30.0,
            "graph_token_cache_path": output_root / ".graph_token_cache.bin",
            "restart_schedule_s": 0,
            "pm_user_trades_enabled": True,
            "pm_user_trades_api_max_limit": 2,
            "pm_user_trades_api_max_offset": 10,
            "pm_user_trades_live_limit": 2,
            "pm_user_trades_backfill_limit": 2,
            "pm_user_trades_live_page_cap": 3,
            "pm_user_trades_backfill_page_cap": 3,
            "pm_user_trades_repair_enabled": True,
            "pm_user_trades_repair_wallet_cap": 10,
            "pm_user_trades_repair_page_cap": 3,
            "pm_user_trades_final_catchup_enabled": True,
        }
        values.update(overrides)
        return ws_scraper.CollectorConfig(**values)

    def make_collector(self, **config_overrides) -> tuple[ws_scraper.PolymarketUserTradeCollector, ws_scraper.PolymarketUserTradeWindowState]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        output_root = Path(tempdir.name)
        config = self.make_config(output_root, **config_overrides)
        collector = ws_scraper.PolymarketUserTradeCollector(
            config=config,
            output_root=output_root,
            zstd_level=config.zstd_level,
            rotate_upload_threshold_bytes=config.rotate_upload_threshold_bytes,
            max_single_upload_bytes=config.graph_max_single_upload_bytes,
            on_file_closed=None,
        )
        meta = make_meta()
        window = ws_scraper.build_polymarket_user_trade_window(meta)
        collector._windows[window.condition_id] = window
        collector._dedupe_by_condition[window.condition_id] = OrderedDict()
        return collector, window

    def drain_rows(self, collector: ws_scraper.PolymarketUserTradeCollector) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        while True:
            try:
                row = collector._row_queue.get_nowait()
            except queue.Empty:
                return rows
            rows.append(row)
            collector._row_queue.task_done()

    def test_live_catchup_stops_when_prior_newest_trade_reappears(self) -> None:
        collector, window = self.make_collector(pm_user_trades_live_limit=2, pm_user_trades_live_page_cap=3)
        trade_old = make_trade(window.condition_id, window.outcomes["up"].asset_id, 2_000_000_120, "11", "aa")
        trade_new = make_trade(window.condition_id, window.outcomes["up"].asset_id, 2_000_000_130, "12", "bb")
        prior_key = collector._trade_dedupe_key(window.condition_id, trade_old)
        collector._dedupe_by_condition[window.condition_id][prior_key] = None
        window.newest_trade_ts_seen = float(trade_old["timestamp"])
        window.newest_trade_dedupe_key = prior_key
        collector._session = FakeSession(
            {
                (window.condition_id, None, 0): [FakeResponse([trade_new, trade_old])],
            }
        )

        ok = collector._catch_up_condition_live(window.condition_id, "live_poll")

        self.assertTrue(ok)
        self.assertEqual(collector._session.calls, [(window.condition_id, None, 0)])
        self.assertEqual(window.live_pages_fetched, 1)
        self.assertEqual(window.outcomes["up"].rows_written, 1)
        self.assertEqual(window.outcomes["up"].rows_deduped, 1)
        self.assertEqual(window.outcomes["up"].capture_reason, "")

    def test_live_catchup_marks_live_page_cap_hit(self) -> None:
        collector, window = self.make_collector(
            pm_user_trades_live_limit=1,
            pm_user_trades_live_page_cap=2,
            pm_user_trades_api_max_offset=10,
        )
        trade_a = make_trade(window.condition_id, window.outcomes["up"].asset_id, 2_000_000_120, "21", "ca")
        trade_b = make_trade(window.condition_id, window.outcomes["up"].asset_id, 2_000_000_121, "22", "cb")
        collector._session = FakeSession(
            {
                (window.condition_id, None, 0): [FakeResponse([trade_b])],
                (window.condition_id, None, 1): [FakeResponse([trade_a])],
            }
        )

        collector._catch_up_condition_live(window.condition_id, "live_poll")

        self.assertEqual(window.live_pages_fetched, 2)
        self.assertIn("live_page_cap_hit", window.outcomes["up"].capture_reason)
        self.assertTrue(window.outcomes["up"].suspected_gap)

    def test_backfill_marks_page_cap_or_offset_cap(self) -> None:
        page_cap_collector, page_cap_window = self.make_collector(
            pm_user_trades_backfill_limit=1,
            pm_user_trades_backfill_page_cap=2,
            pm_user_trades_api_max_offset=10,
        )
        trade_a = make_trade(page_cap_window.condition_id, page_cap_window.outcomes["up"].asset_id, 2_000_000_120, "31", "da")
        trade_b = make_trade(page_cap_window.condition_id, page_cap_window.outcomes["up"].asset_id, 2_000_000_121, "32", "db")
        page_cap_collector._session = FakeSession(
            {
                (page_cap_window.condition_id, None, 0): [FakeResponse([trade_a])],
                (page_cap_window.condition_id, None, 1): [FakeResponse([trade_b])],
            }
        )

        page_cap_collector._backfill_condition(page_cap_window.condition_id, "startup_backfill")

        self.assertIn("backfill_page_cap_hit", page_cap_window.outcomes["up"].capture_reason)

        offset_cap_collector, offset_cap_window = self.make_collector(
            pm_user_trades_backfill_limit=1,
            pm_user_trades_backfill_page_cap=5,
            pm_user_trades_api_max_offset=0,
        )
        trade_c = make_trade(offset_cap_window.condition_id, offset_cap_window.outcomes["up"].asset_id, 2_000_000_120, "33", "dc")
        offset_cap_collector._session = FakeSession(
            {
                (offset_cap_window.condition_id, None, 0): [FakeResponse([trade_c])],
            }
        )

        offset_cap_collector._backfill_condition(offset_cap_window.condition_id, "startup_backfill")

        self.assertIn("backfill_offset_cap_hit", offset_cap_window.outcomes["up"].capture_reason)

    def test_finalize_defers_until_final_catchup_and_repair_are_done(self) -> None:
        collector, window = self.make_collector()
        window.outcomes["up"].suspected_gap = True
        window.outcomes["up"].capture_ok = False
        window.observed_wallets[_hex_id(40, "44")] = None

        catchup_calls: list[tuple[str, str]] = []

        def fake_catchup(condition_id: str, collection_mode: str) -> bool:
            catchup_calls.append((condition_id, collection_mode))
            return True

        def fake_enqueue_repair(condition_id: str) -> bool:
            with collector._state_lock:
                queued_window = collector._windows[condition_id]
                queued_window.repair_requested = True
                queued_window.repair_pending = True
            return True

        collector._catch_up_condition_live = fake_catchup  # type: ignore[assignment]
        collector._enqueue_repair = fake_enqueue_repair  # type: ignore[assignment]

        collector._finalize_closed_windows(window.window_close_ts + 100)
        self.assertEqual(catchup_calls, [(window.condition_id, "final_catchup")])
        self.assertIn(window.condition_id, collector._windows)
        self.assertTrue(window.final_catchup_applied)
        self.assertEqual(self.drain_rows(collector), [])

        collector._finalize_closed_windows(window.window_close_ts + 100)
        self.assertIn(window.condition_id, collector._windows)
        self.assertTrue(window.repair_pending)
        self.assertEqual(self.drain_rows(collector), [])

        window.repair_pending = False
        window.repair_completed = True
        collector._finalize_closed_windows(window.window_close_ts + 100)

        self.assertNotIn(window.condition_id, collector._windows)
        final_rows = self.drain_rows(collector)
        self.assertEqual(len(final_rows), 2)
        self.assertTrue(all(row["row_type"] == "audit" for row in final_rows))

    def test_repair_worker_updates_counters_and_reuses_trade_pipeline(self) -> None:
        collector, window = self.make_collector(
            pm_user_trades_api_max_limit=1,
            pm_user_trades_api_max_offset=2,
            pm_user_trades_repair_page_cap=2,
            pm_user_trades_repair_wallet_cap=2,
        )
        wallet_one = _hex_id(40, "51")
        wallet_two = _hex_id(40, "52")
        window.repair_requested = True
        window.repair_pending = True
        window.outcomes["up"].suspected_gap = True
        window.outcomes["up"].capture_ok = False
        window.observed_wallets[wallet_one] = None
        window.observed_wallets[wallet_two] = None

        trade = make_trade(window.condition_id, window.outcomes["up"].asset_id, 2_000_000_120, "51", "ea")
        collector._session = FakeSession(
            {
                (window.condition_id, wallet_one, 0): [FakeResponse([trade])],
                (window.condition_id, wallet_one, 1): [FakeResponse([])],
                (window.condition_id, wallet_two, 0): [FakeResponse([])],
            }
        )

        collector._repair_condition(window.condition_id)

        self.assertTrue(window.repair_completed)
        self.assertFalse(window.repair_pending)
        self.assertTrue(window.repair_applied)
        self.assertEqual(window.repair_wallets_attempted, 2)
        self.assertEqual(window.repair_wallets_with_new_rows, 1)
        self.assertEqual(window.repair_pages_fetched, 3)
        self.assertEqual(window.outcomes["up"].rows_written, 1)

    def test_resolver_rejects_gamma_slug_mismatch_and_keeps_verified_slug(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        config = self.make_config(Path(tempdir.name))

        mismatch_session = FakeSession(
            {
                ("btc-updown-5m-1700000000", None, 0): [],
            }
        )
        mismatch_session.get = lambda url, params, timeout: FakeResponse(  # type: ignore[assignment]
            [
                {
                    "slug": "wrong-slug",
                    "conditionId": _hex_id(64, "61"),
                    "clobTokenIds": ["yes", "no"],
                    "outcomes": ["Yes", "No"],
                }
            ]
        )
        resolver = ws_scraper.PolymarketMarketResolver(config, mismatch_session)
        meta, err = resolver._fetch_market_meta("btc", 1_700_000_000)
        self.assertIsNone(meta)
        self.assertIn("gamma_slug_mismatch", err)

        match_session = FakeSession(
            {
                ("btc-updown-5m-1700000000", None, 0): [],
            }
        )
        match_slug = ws_scraper.market_slug("btc", 1_700_000_000)
        match_session.get = lambda url, params, timeout: FakeResponse(  # type: ignore[assignment]
            [
                {
                    "slug": match_slug,
                    "conditionId": _hex_id(64, "62"),
                    "clobTokenIds": ["yes", "no"],
                    "outcomes": ["Yes", "No"],
                }
            ]
        )
        resolver = ws_scraper.PolymarketMarketResolver(config, match_session)
        meta, err = resolver._fetch_market_meta("btc", 1_700_000_000)
        self.assertEqual(err, "")
        self.assertIsNotNone(meta)
        self.assertEqual(meta.slug, match_slug)
        self.assertEqual(meta.resolver_returned_slug, match_slug)


if __name__ == "__main__":
    unittest.main()
