"""Wire schemas of the terminal API (REST bodies and the payloads of stream messages).

These models are the published contract — they generate the OpenAPI document, and the web
terminal's TypeScript types mirror them (``frontend/src/lib/api/types.ts``). Decimal amounts
are strings so no precision is lost on the way to the browser; times are epoch milliseconds.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

DecimalStr = Annotated[str, Field(description="Exact decimal, as a string", examples=["0.25"])]
EpochMs = Annotated[int, Field(description="Epoch milliseconds, UTC")]


class _Out(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# ------------------------------------------------------------------------------- system


class HealthOut(_Out):
    status: Literal["ok", "degraded"]
    database: bool
    redis: bool
    version: str


class TicketOut(_Out):
    ticket: str = Field(description="Single-use credential for one stream connection")
    expires_in: float = Field(description="Seconds until the ticket expires")


# ------------------------------------------------------------------------------ markets


class MarketOut(_Out):
    venue: str
    symbol: str


class MarketsOut(_Out):
    markets: list[MarketOut]
    timeframes: list[str]


class CandleOut(_Out):
    """One candle; ``t`` is its open time. The one-letter names keep candle streams small."""

    t: EpochMs
    o: float
    h: float
    l: float  # noqa: E741 — the OHLC wire name
    c: float
    v: float


class CandlesOut(_Out):
    venue: str
    symbol: str
    timeframe: str
    candles: list[CandleOut]


# ----------------------------------------------------------------------------- sessions


class SessionSummaryOut(_Out):
    id: str
    mode: str
    venue: str
    strategy: str
    symbols: list[str]
    timeframe: str
    status: str
    last_equity: DecimalStr | None
    halts: list[str]
    created: EpochMs
    updated: EpochMs


class SessionInfoOut(_Out):
    id: str
    mode: Literal["paper", "sandbox", "live"]
    venue: str
    strategy: str
    timeframe: str
    symbols: list[str]
    quote: str
    initial_capital: DecimalStr
    status: str
    created: EpochMs
    updated: EpochMs
    config: dict[str, Any] = Field(description="The session's stored configuration")


class EngineOut(_Out):
    state: Literal["running", "stopping", "stopped"]
    instance: str | None


class HaltOut(_Out):
    reason: str
    since: EpochMs
    until: EpochMs | None = Field(description="When the halt lifts by itself, if it does")
    detail: str
    resumable: bool = Field(description="Whether `resume` lifts it")


class StateOut(_Out):
    halts: list[HaltOut]
    flatten: str | None = Field(description="Why positions are being closed, while they are")
    high_water_mark: DecimalStr | None
    day_start_equity: DecimalStr | None


class FlattenOut(_Out):
    state: Literal["started", "completed", "incomplete", "ended"]
    reason: str
    left_open: str | None = None
    note: str | None = None


class AccountOut(_Out):
    quote: str
    initial_capital: DecimalStr
    cash: DecimalStr
    realized_pnl: DecimalStr = Field(description="Closed-trade PnL before fees")
    fees: DecimalStr


class PositionOut(_Out):
    symbol: str
    quantity: DecimalStr = Field(description="Signed: positive long, negative short")
    average_price: DecimalStr
    realized_pnl: DecimalStr
    fees: DecimalStr


class OrderOut(_Out):
    client_order_id: str
    venue_order_id: str | None
    symbol: str
    side: Literal["buy", "sell"]
    type: Literal["market", "limit"]
    tif: Literal["gtc", "ioc", "fok"]
    post_only: bool
    reduce_only: bool
    status: Literal[
        "pending",
        "unknown",
        "open",
        "partially_filled",
        "cancel_pending",
        "filled",
        "cancelled",
        "rejected",
        "expired",
    ]
    quantity: DecimalStr
    filled: DecimalStr
    average_price: DecimalStr | None
    limit_price: DecimalStr | None
    reason: str | None
    intent: str
    created: EpochMs
    ts: EpochMs
    version: int
    note: str | None = None


class FillOut(_Out):
    client_order_id: str
    trade_id: str
    symbol: str
    side: Literal["buy", "sell"]
    quantity: DecimalStr
    price: DecimalStr
    fee: DecimalStr
    fee_currency: str
    liquidity: Literal["maker", "taker"]
    ts: EpochMs


class EquityPointOut(_Out):
    ts: EpochMs
    equity: DecimalStr
    cash: DecimalStr
    gross_exposure: DecimalStr
    drawdown: DecimalStr = Field(description="Fraction below the high-water mark")
    day_pnl: DecimalStr


class RiskEventOut(_Out):
    kind: Literal["rejected", "adjusted", "breach", "halt", "resume", "anomaly", "market_data"]
    code: str
    message: str
    symbol: str | None
    ts: EpochMs


class SessionSnapshotOut(_Out):
    """Everything about one session; the ``session:`` stream sends this first, then deltas
    whose ``kind`` names the part they update (order, fill, position, account, equity, risk,
    state, flatten, engine)."""

    session: SessionInfoOut
    engine: EngineOut
    state: StateOut
    flatten: FlattenOut | None
    account: AccountOut
    positions: list[PositionOut]
    orders: list[OrderOut]
    fills: list[FillOut]
    equity: list[EquityPointOut]
    risk: list[RiskEventOut]


# ----------------------------------------------------------------------------- commands


class CommandIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["halt", "flatten", "resume", "stop"]
    reason: str = Field(min_length=1, max_length=500)


class CommandOut(_Out):
    offset: str = Field(description="Position of the command in the session's command stream")
    engine: str | None = Field(
        description="The engine that will apply it now; null: applied when the session starts"
    )


class ErrorOut(_Out):
    detail: str


__all__ = [
    "AccountOut",
    "CandleOut",
    "CandlesOut",
    "CommandIn",
    "CommandOut",
    "EngineOut",
    "EquityPointOut",
    "ErrorOut",
    "FillOut",
    "FlattenOut",
    "HaltOut",
    "HealthOut",
    "MarketOut",
    "MarketsOut",
    "OrderOut",
    "PositionOut",
    "RiskEventOut",
    "SessionInfoOut",
    "SessionSnapshotOut",
    "SessionSummaryOut",
    "StateOut",
    "TicketOut",
]
