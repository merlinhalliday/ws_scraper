from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = Path(__file__).resolve().parents[1]

# Endpoints
HOST = "https://clob.polymarket.com"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
RELAYER_HOST_DEFAULT = "https://relayer-v2.polymarket.com/"

# Strategy defaults
TRAIN_FRACTION = 0.70
MAX_STREAK = 7
SEQ_WINDOW = 2016
SEQ_MIN_LOCAL_SAMPLES = 100
RSI_LENGTH = 14
RSI_THRESHOLD = 0.51

# Runtime defaults
WINDOW_SECONDS = 300
HTTP_TIMEOUT_SECONDS = 8

COINBASE_PRODUCT_ID = "BTC-USD"
TRAINING_CSV_PATH = APP_ROOT / "BTC_USD_5min_clean.csv"
ROLLING_HISTORY_BARS = 2016
ROLLING_HISTORY_MARGIN_BARS = 96
COINBASE_BOOTSTRAP_DAYS = 8
COINBASE_MAX_CANDLES_PER_REQUEST = 300

LOG_DELAY_SECONDS = 150
CANDLE_WAIT_TIMEOUT_SECONDS = 20
WS_STALE_TIMEOUT_SECONDS = 10
WS_RECONNECT_BACKOFF_SECONDS = (1, 2, 5)
STARTUP_ARM_WINDOW_SECONDS = 5

LIVE_BET_USD = 1.0
FAK_WINDOW_SECONDS = 30
FAK_RETRY_INTERVAL_SECONDS = 0.4
FAK_MAX_PRICE_CAP = 0.99
FAK_MIN_REMAINING_USD = 0.01
MARKET_LOOKUP_RETRY_SECONDS = 3.0
MARKET_LOOKUP_RETRY_SLEEP = 0.25

# Martingale defaults
MARTINGALE_ENABLED = True
MARTINGALE_BASE_BET_USD = LIVE_BET_USD
MARTINGALE_LOSS_STREAK_START_USD = 0.0
MARTINGALE_FALLBACK_MULTIPLIER = 1.9
MARTINGALE_MIN_MULTIPLIER = 1.2
MARTINGALE_MAX_MULTIPLIER = 2.5
MARTINGALE_QUOTE_UPLIFT_PCT = 0.06
MAX_LOSS_STREAK = 200 # value in dollars, when exceeded switches Maringale off

# Risk-pause defaults
LOSS_PAUSE_TRIGGER_STREAK = 6
LOSS_PAUSE_MARKETS = 144

# Redeem defaults
REDEEM_ENABLED_DEFAULT = True
REDEEM_SCOPE = "data_api_redeemable_positions"
REDEEM_POLICY = "one_attempt_only"
REDEEM_TX_TYPE = "PROXY"
REDEEM_INDEX_SETS = [1, 2]
REDEEM_MAX_PER_LOG_TICK = 1
REDEEM_STARTUP_CATCHUP_ENABLED = False
REDEEM_STARTUP_CATCHUP_TIMEOUT_SECONDS = 45
REDEEM_STARTUP_CATCHUP_POLL_SECONDS = 2
REDEEM_POSITIONS_URL = "https://data-api.polymarket.com/positions"
REDEEM_POSITIONS_LIMIT = 500
REDEEM_POSITIONS_REFRESH_SECONDS = 300
REDEEM_STATUS_POLL_SECONDS = 120
REDEEM_RATE_LIMIT_BUFFER_SECONDS = 15
REDEEM_STATE_FILE = PROJECT_ROOT / "outputs" / "redeem_runtime_state.json"
REDEEM_PENDING_REQUEST_MAX_AGE_SECONDS = 3600
REDEEM_DAILY_SUBMIT_CAP = 90
REDEEM_WINDOW_SECONDS = 300
REDEEM_WINDOW_MAX = 1


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def pick_first_existing_path(candidates: list[Path], fallback: Path) -> Path:
    for p in candidates:
        if p.exists():
            return p
    return fallback


@dataclass(frozen=True)
class RuntimeConfig:
    project_root: Path
    host: str
    gamma_markets_url: str
    relayer_host_default: str

    train_fraction: float
    max_streak: int
    seq_window: int
    seq_min_local_samples: int
    rsi_length: int
    rsi_threshold: float

    window_seconds: int
    http_timeout_seconds: int

    coinbase_product_id: str
    training_csv_path: Path
    rolling_history_bars: int
    rolling_history_margin_bars: int
    coinbase_bootstrap_days: int
    coinbase_max_candles_per_request: int

    log_delay_seconds: int
    candle_wait_timeout_seconds: int
    ws_stale_timeout_seconds: int
    ws_reconnect_backoff_seconds: tuple[int, int, int]
    startup_arm_window_seconds: int

    live_bet_usd: float
    fak_window_seconds: float
    fak_retry_interval_seconds: float
    fak_max_price_cap: float
    fak_min_remaining_usd: float
    market_lookup_retry_seconds: float
    market_lookup_retry_sleep: float

    martingale_enabled: bool
    martingale_base_bet_usd: float
    martingale_loss_streak_start_usd: float
    martingale_fallback_multiplier: float
    martingale_min_multiplier: float
    martingale_max_multiplier: float
    martingale_quote_uplift_pct: float
    max_loss_streak: float | None
    loss_pause_trigger_streak: int
    loss_pause_markets: int

    redeem_enabled_default: bool
    redeem_scope: str
    redeem_policy: str
    redeem_tx_type: str
    redeem_index_sets: tuple[int, int]
    redeem_max_per_log_tick: int
    redeem_startup_catchup_enabled: bool
    redeem_startup_catchup_timeout_seconds: float
    redeem_startup_catchup_poll_seconds: float
    redeem_positions_url: str
    redeem_positions_limit: int
    redeem_positions_refresh_seconds: float
    redeem_status_poll_seconds: float
    redeem_rate_limit_buffer_seconds: float
    redeem_state_file: Path
    redeem_pending_request_max_age_seconds: float
    redeem_daily_submit_cap: int
    redeem_window_seconds: float
    redeem_window_max: int

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        raw_fak_retry = (os.getenv("FAK_RETRY_INTERVAL_SECONDS") or "").strip()
        try:
            fak_retry = float(raw_fak_retry) if raw_fak_retry else FAK_RETRY_INTERVAL_SECONDS
        except ValueError:
            fak_retry = FAK_RETRY_INTERVAL_SECONDS

        raw_log_delay = (os.getenv("LOG_DELAY_SECONDS") or "").strip()
        try:
            log_delay = int(raw_log_delay) if raw_log_delay else LOG_DELAY_SECONDS
        except ValueError:
            log_delay = LOG_DELAY_SECONDS

        raw_martingale_base = (os.getenv("MARTINGALE_BASE_BET_USD") or "").strip()
        try:
            martingale_base_bet = float(raw_martingale_base) if raw_martingale_base else MARTINGALE_BASE_BET_USD
        except ValueError:
            martingale_base_bet = MARTINGALE_BASE_BET_USD
        if martingale_base_bet <= 0:
            martingale_base_bet = MARTINGALE_BASE_BET_USD

        raw_martingale_start = (os.getenv("MARTINGALE_LOSS_STREAK_START_USD") or "").strip()
        try:
            martingale_loss_start = (
                float(raw_martingale_start) if raw_martingale_start else MARTINGALE_LOSS_STREAK_START_USD
            )
        except ValueError:
            martingale_loss_start = MARTINGALE_LOSS_STREAK_START_USD
        martingale_loss_start = max(0.0, martingale_loss_start)

        raw_fallback_mult = (os.getenv("MARTINGALE_FALLBACK_MULTIPLIER") or "").strip()
        try:
            martingale_fallback_mult = (
                float(raw_fallback_mult) if raw_fallback_mult else MARTINGALE_FALLBACK_MULTIPLIER
            )
        except ValueError:
            martingale_fallback_mult = MARTINGALE_FALLBACK_MULTIPLIER
        if martingale_fallback_mult <= 1.0:
            martingale_fallback_mult = MARTINGALE_FALLBACK_MULTIPLIER

        raw_min_mult = (os.getenv("MARTINGALE_MIN_MULTIPLIER") or "").strip()
        try:
            martingale_min_mult = float(raw_min_mult) if raw_min_mult else MARTINGALE_MIN_MULTIPLIER
        except ValueError:
            martingale_min_mult = MARTINGALE_MIN_MULTIPLIER
        if martingale_min_mult <= 1.0:
            martingale_min_mult = MARTINGALE_MIN_MULTIPLIER

        raw_max_mult = (os.getenv("MARTINGALE_MAX_MULTIPLIER") or "").strip()
        try:
            martingale_max_mult = float(raw_max_mult) if raw_max_mult else MARTINGALE_MAX_MULTIPLIER
        except ValueError:
            martingale_max_mult = MARTINGALE_MAX_MULTIPLIER
        if martingale_max_mult <= martingale_min_mult:
            martingale_max_mult = MARTINGALE_MAX_MULTIPLIER
        if martingale_max_mult <= martingale_min_mult:
            martingale_max_mult = martingale_min_mult

        raw_quote_uplift = (os.getenv("MARTINGALE_QUOTE_UPLIFT_PCT") or "").strip()
        try:
            martingale_quote_uplift_pct = (
                float(raw_quote_uplift) if raw_quote_uplift else MARTINGALE_QUOTE_UPLIFT_PCT
            )
        except ValueError:
            martingale_quote_uplift_pct = MARTINGALE_QUOTE_UPLIFT_PCT
        martingale_quote_uplift_pct = max(0.0, martingale_quote_uplift_pct)

        raw_max_loss_streak = (os.getenv("MAX_LOSS_STREAK") or "").strip()
        max_loss_streak: float | None
        try:
            max_loss_streak = float(raw_max_loss_streak) if raw_max_loss_streak else MAX_LOSS_STREAK
        except ValueError:
            max_loss_streak = MAX_LOSS_STREAK
        if max_loss_streak is not None and max_loss_streak <= 0:
            max_loss_streak = None

        raw_loss_pause_trigger = (os.getenv("LOSS_PAUSE_TRIGGER_STREAK") or "").strip()
        try:
            loss_pause_trigger_streak = (
                int(raw_loss_pause_trigger) if raw_loss_pause_trigger else int(LOSS_PAUSE_TRIGGER_STREAK)
            )
        except ValueError:
            loss_pause_trigger_streak = int(LOSS_PAUSE_TRIGGER_STREAK)
        loss_pause_trigger_streak = max(1, loss_pause_trigger_streak)

        raw_loss_pause_markets = (os.getenv("LOSS_PAUSE_MARKETS") or "").strip()
        try:
            loss_pause_markets = int(raw_loss_pause_markets) if raw_loss_pause_markets else int(LOSS_PAUSE_MARKETS)
        except ValueError:
            loss_pause_markets = int(LOSS_PAUSE_MARKETS)
        loss_pause_markets = max(1, loss_pause_markets)

        raw_catchup_timeout = (os.getenv("REDEEM_STARTUP_CATCHUP_TIMEOUT_SECONDS") or "").strip()
        try:
            catchup_timeout = (
                float(raw_catchup_timeout) if raw_catchup_timeout else float(REDEEM_STARTUP_CATCHUP_TIMEOUT_SECONDS)
            )
        except ValueError:
            catchup_timeout = float(REDEEM_STARTUP_CATCHUP_TIMEOUT_SECONDS)

        raw_catchup_poll = (os.getenv("REDEEM_STARTUP_CATCHUP_POLL_SECONDS") or "").strip()
        try:
            catchup_poll = float(raw_catchup_poll) if raw_catchup_poll else float(REDEEM_STARTUP_CATCHUP_POLL_SECONDS)
        except ValueError:
            catchup_poll = float(REDEEM_STARTUP_CATCHUP_POLL_SECONDS)

        default_training_csv = pick_first_existing_path(
            candidates=[
                APP_ROOT / "BTC_USD_5min_clean.csv",
                PROJECT_ROOT / "BTC_USD_5min_clean.csv",
                APP_ROOT / "BTC_USDC_5min_clean.csv",
                PROJECT_ROOT / "BTC_USDC_5min_clean.csv",
            ],
            fallback=TRAINING_CSV_PATH,
        )

        raw_positions_limit = (os.getenv("REDEEM_POSITIONS_LIMIT") or "").strip()
        try:
            positions_limit = int(raw_positions_limit) if raw_positions_limit else int(REDEEM_POSITIONS_LIMIT)
        except ValueError:
            positions_limit = int(REDEEM_POSITIONS_LIMIT)
        positions_limit = max(1, positions_limit)

        raw_positions_refresh = (os.getenv("REDEEM_POSITIONS_REFRESH_SECONDS") or "").strip()
        try:
            positions_refresh_seconds = (
                float(raw_positions_refresh) if raw_positions_refresh else float(REDEEM_POSITIONS_REFRESH_SECONDS)
            )
        except ValueError:
            positions_refresh_seconds = float(REDEEM_POSITIONS_REFRESH_SECONDS)
        positions_refresh_seconds = max(5.0, positions_refresh_seconds)

        raw_status_poll = (os.getenv("REDEEM_STATUS_POLL_SECONDS") or "").strip()
        try:
            status_poll_seconds = float(raw_status_poll) if raw_status_poll else float(REDEEM_STATUS_POLL_SECONDS)
        except ValueError:
            status_poll_seconds = float(REDEEM_STATUS_POLL_SECONDS)
        status_poll_seconds = max(5.0, status_poll_seconds)

        raw_rl_buffer = (os.getenv("REDEEM_RATE_LIMIT_BUFFER_SECONDS") or "").strip()
        try:
            rate_limit_buffer_seconds = (
                float(raw_rl_buffer) if raw_rl_buffer else float(REDEEM_RATE_LIMIT_BUFFER_SECONDS)
            )
        except ValueError:
            rate_limit_buffer_seconds = float(REDEEM_RATE_LIMIT_BUFFER_SECONDS)
        rate_limit_buffer_seconds = max(0.0, rate_limit_buffer_seconds)

        raw_redeem_state_file = (os.getenv("REDEEM_STATE_FILE") or "").strip()
        redeem_state_file = Path(raw_redeem_state_file) if raw_redeem_state_file else REDEEM_STATE_FILE
        if not redeem_state_file.is_absolute():
            redeem_state_file = PROJECT_ROOT / redeem_state_file

        raw_pending_max_age = (os.getenv("REDEEM_PENDING_REQUEST_MAX_AGE_SECONDS") or "").strip()
        try:
            redeem_pending_request_max_age_seconds = (
                float(raw_pending_max_age)
                if raw_pending_max_age
                else float(REDEEM_PENDING_REQUEST_MAX_AGE_SECONDS)
            )
        except ValueError:
            redeem_pending_request_max_age_seconds = float(REDEEM_PENDING_REQUEST_MAX_AGE_SECONDS)
        redeem_pending_request_max_age_seconds = max(5.0, redeem_pending_request_max_age_seconds)

        raw_redeem_daily_submit_cap = (os.getenv("REDEEM_DAILY_SUBMIT_CAP") or "").strip()
        try:
            redeem_daily_submit_cap = (
                int(raw_redeem_daily_submit_cap) if raw_redeem_daily_submit_cap else int(REDEEM_DAILY_SUBMIT_CAP)
            )
        except ValueError:
            redeem_daily_submit_cap = int(REDEEM_DAILY_SUBMIT_CAP)
        redeem_daily_submit_cap = max(1, redeem_daily_submit_cap)

        raw_redeem_window_seconds = (os.getenv("REDEEM_WINDOW_SECONDS") or "").strip()
        try:
            redeem_window_seconds = (
                float(raw_redeem_window_seconds) if raw_redeem_window_seconds else float(REDEEM_WINDOW_SECONDS)
            )
        except ValueError:
            redeem_window_seconds = float(REDEEM_WINDOW_SECONDS)
        redeem_window_seconds = max(1.0, redeem_window_seconds)

        raw_redeem_window_max = (os.getenv("REDEEM_WINDOW_MAX") or "").strip()
        try:
            redeem_window_max = int(raw_redeem_window_max) if raw_redeem_window_max else int(REDEEM_WINDOW_MAX)
        except ValueError:
            redeem_window_max = int(REDEEM_WINDOW_MAX)
        redeem_window_max = max(1, redeem_window_max)

        return cls(
            project_root=PROJECT_ROOT,
            host=HOST,
            gamma_markets_url=GAMMA_MARKETS_URL,
            relayer_host_default=(os.getenv("POLY_RELAYER_HOST") or "").strip() or RELAYER_HOST_DEFAULT,
            train_fraction=TRAIN_FRACTION,
            max_streak=MAX_STREAK,
            seq_window=SEQ_WINDOW,
            seq_min_local_samples=SEQ_MIN_LOCAL_SAMPLES,
            rsi_length=RSI_LENGTH,
            rsi_threshold=RSI_THRESHOLD,
            window_seconds=WINDOW_SECONDS,
            http_timeout_seconds=HTTP_TIMEOUT_SECONDS,
            coinbase_product_id=COINBASE_PRODUCT_ID,
            training_csv_path=Path((os.getenv("POLYGALE_TRAINING_CSV") or "").strip() or default_training_csv),
            rolling_history_bars=ROLLING_HISTORY_BARS,
            rolling_history_margin_bars=ROLLING_HISTORY_MARGIN_BARS,
            coinbase_bootstrap_days=COINBASE_BOOTSTRAP_DAYS,
            coinbase_max_candles_per_request=COINBASE_MAX_CANDLES_PER_REQUEST,
            log_delay_seconds=log_delay,
            candle_wait_timeout_seconds=CANDLE_WAIT_TIMEOUT_SECONDS,
            ws_stale_timeout_seconds=WS_STALE_TIMEOUT_SECONDS,
            ws_reconnect_backoff_seconds=WS_RECONNECT_BACKOFF_SECONDS,
            startup_arm_window_seconds=STARTUP_ARM_WINDOW_SECONDS,
            live_bet_usd=LIVE_BET_USD,
            fak_window_seconds=FAK_WINDOW_SECONDS,
            fak_retry_interval_seconds=fak_retry,
            fak_max_price_cap=FAK_MAX_PRICE_CAP,
            fak_min_remaining_usd=FAK_MIN_REMAINING_USD,
            market_lookup_retry_seconds=MARKET_LOOKUP_RETRY_SECONDS,
            market_lookup_retry_sleep=MARKET_LOOKUP_RETRY_SLEEP,
            martingale_enabled=env_bool("MARTINGALE_ENABLED", MARTINGALE_ENABLED),
            martingale_base_bet_usd=martingale_base_bet,
            martingale_loss_streak_start_usd=martingale_loss_start,
            martingale_fallback_multiplier=martingale_fallback_mult,
            martingale_min_multiplier=martingale_min_mult,
            martingale_max_multiplier=martingale_max_mult,
            martingale_quote_uplift_pct=martingale_quote_uplift_pct,
            max_loss_streak=max_loss_streak,
            loss_pause_trigger_streak=loss_pause_trigger_streak,
            loss_pause_markets=loss_pause_markets,
            redeem_enabled_default=env_bool("POLY_REDEEM_ENABLED", REDEEM_ENABLED_DEFAULT),
            redeem_scope=REDEEM_SCOPE,
            redeem_policy=REDEEM_POLICY,
            redeem_tx_type=REDEEM_TX_TYPE,
            redeem_index_sets=(REDEEM_INDEX_SETS[0], REDEEM_INDEX_SETS[1]),
            redeem_max_per_log_tick=REDEEM_MAX_PER_LOG_TICK,
            redeem_startup_catchup_enabled=env_bool(
                "REDEEM_STARTUP_CATCHUP_ENABLED", REDEEM_STARTUP_CATCHUP_ENABLED
            ),
            redeem_startup_catchup_timeout_seconds=catchup_timeout,
            redeem_startup_catchup_poll_seconds=catchup_poll,
            redeem_positions_url=(os.getenv("REDEEM_POSITIONS_URL") or "").strip() or REDEEM_POSITIONS_URL,
            redeem_positions_limit=positions_limit,
            redeem_positions_refresh_seconds=positions_refresh_seconds,
            redeem_status_poll_seconds=status_poll_seconds,
            redeem_rate_limit_buffer_seconds=rate_limit_buffer_seconds,
            redeem_state_file=redeem_state_file,
            redeem_pending_request_max_age_seconds=redeem_pending_request_max_age_seconds,
            redeem_daily_submit_cap=redeem_daily_submit_cap,
            redeem_window_seconds=redeem_window_seconds,
            redeem_window_max=redeem_window_max,
        )
