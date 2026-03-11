from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class SignalDecision:
    seq_sig: Optional[int]
    seq_dir: Optional[int]
    seq_streak: Optional[int]
    seq_n_local: int
    seq_p_rev: Optional[float]
    seq_used_local: bool
    rsi_value: Optional[float]
    rsi_prob_up: Optional[float]
    rsi_sig: Optional[int]
    agreement_sig: Optional[int]
    note: str = ""


@dataclass
class ExecutionResult:
    status: str
    attempts: int
    orders_posted: int
    orders_failed: int
    order_ids: list[str]
    filled_usd: float
    filled_shares: float
    avg_fill_price: float
    potential_payout_usd: float
    first_order_time_utc: str
    last_order_time_utc: str
    last_status: str
    errors: list[str]
    last_response: Any


@dataclass
class MarketState:
    market_start_ts: int
    candle_deadline_ts: int
    log_due_ts: int
    decision_done: bool
    written: bool
    row: dict[str, Any]


@dataclass
class RedeemSummary:
    attempted: list[str]
    submitted: list[str]
    request_ids: list[str]
    errors: list[str]
    status_updates: list[str]
    pending_count: int
    confirmed_count: int


@dataclass
class CatchupSummary:
    enabled: bool
    attempted: int
    submitted: int
    request_ids: list[str]
    status_updates: list[str]
    errors: list[str]
    loops: int
    timed_out: bool
    open_candidates_final: int
    pending_final: int
    confirmed_final: int


@dataclass
class Exposure:
    market_start_ts: int
    bet_side: int
    filled_usd: float
    potential_payout_usd: float
    resolved: bool = False


@dataclass
class LossRecoveryState:
    loss_streak_usd: float
    open_exposures: dict[int, Exposure]
    martingale_runtime_enabled: bool = True
    consecutive_losses: int = 0
    loss_pause_markets_remaining: int = 0
    max_loss_halt_active: bool = False
    max_loss_halt_trigger_market_start_ts: int | None = None
    max_loss_halt_trigger_time_utc: str = ""
    max_loss_halt_trigger_loss_streak_usd: float | None = None
