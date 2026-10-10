"""asyncpg implementation of :class:`~crypto_quant.application.ports.trading.TradingStore`.

Every ``write`` is one transaction that starts by checking — and locking — the session row
with the caller's fence::

    UPDATE trading.sessions SET … WHERE session_id = $1 AND fence = $2

No row means another engine claimed the session since (``claim_session`` bumps the fence
under the same row lock), and the whole batch is refused with ``FencedOutError``. Inside
the transaction, orders are upserted only when the incoming version is higher, fills are
inserted at most once per ``(session, venue, symbol, trade_id)``, equity points are upserted per
timestamp and risk events are appended.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Final

import asyncpg
import orjson
import structlog

from crypto_quant.application.errors import FencedOutError, SessionConfigError, StorageError
from crypto_quant.application.execution.events import (
    EquityPoint,
    RiskEventKind,
    RiskEventRecord,
    SessionState,
)
from crypto_quant.application.ports.trading import SessionRecord, SessionSummary, WriteBatch
from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.orders import (
    OrderFill,
    OrderRecord,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from crypto_quant.domain.risk import Halt, HaltReason
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.numbers import decimal_str
from crypto_quant.domain.shared.time import datetime_to_ns, ns_to_datetime
from crypto_quant.infrastructure.persistence.errors import translate

_log = structlog.get_logger(__name__)

STATE_VERSION: Final = 1

_CREATE_SESSION = """
INSERT INTO trading.sessions
    (session_id, mode, venue, strategy, timeframe, symbols, quote_currency, initial_capital,
     config, status, fence, state, last_equity, halts)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12::jsonb, $13, $14)
"""

_LOAD_SESSION = """
SELECT session_id, mode, venue, strategy, timeframe, symbols, quote_currency, initial_capital,
config::text AS config, status, fence, state::text AS state, created_at, updated_at
FROM trading.sessions
WHERE session_id = $1
"""

_CLAIM = """
UPDATE trading.sessions
SET fence = fence + 1, status = 'running', updated_at = now()
WHERE session_id = $1
RETURNING fence
"""

_RELEASE = """
UPDATE trading.sessions SET status = $3, updated_at = now()
WHERE session_id = $1 AND fence = $2
"""

# The fence check, the row lock that serialises it against claim_session, and the session
# state update, in one statement.
_FENCED_STATE = """
UPDATE trading.sessions
SET updated_at = now(),
    state = COALESCE($3::jsonb, state),
    last_equity = CASE WHEN $3::jsonb IS NULL THEN last_equity ELSE $4 END,
    halts = COALESCE($5::text[], halts)
WHERE session_id = $1 AND fence = $2
RETURNING fence
"""

_UPSERT_ORDER = """
INSERT INTO trading.orders
    (client_order_id, session_id, venue, symbol, side, order_type, quantity, limit_price,
     time_in_force, post_only, reduce_only, intent, status, venue_order_id, filled_quantity,
     filled_notional, venue_filled_quantity, fees, reject_reason, created_ns, updated_ns,
     version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
        $18::jsonb, $19, $20, $21, $22)
ON CONFLICT (client_order_id) DO UPDATE SET
    status = EXCLUDED.status,
    venue_order_id = EXCLUDED.venue_order_id,
    filled_quantity = EXCLUDED.filled_quantity,
    filled_notional = EXCLUDED.filled_notional,
    venue_filled_quantity = EXCLUDED.venue_filled_quantity,
    fees = EXCLUDED.fees,
    reject_reason = EXCLUDED.reject_reason,
    updated_ns = EXCLUDED.updated_ns,
    version = EXCLUDED.version
WHERE trading.orders.version < EXCLUDED.version
"""

_INSERT_FILL = """
INSERT INTO trading.fills
    (venue, symbol, trade_id, client_order_id, session_id, side, quantity, price, fee,
     fee_currency, liquidity, ts_ns)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (session_id, venue, symbol, trade_id) DO NOTHING
"""

_INSERT_RISK_EVENT = """
INSERT INTO trading.risk_events (session_id, ts_ns, kind, code, message, symbol, detail)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
"""

_UPSERT_EQUITY = """
INSERT INTO trading.equity (session_id, ts, equity, cash, gross_exposure, drawdown, day_pnl)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (session_id, ts) DO UPDATE SET
    equity = EXCLUDED.equity,
    cash = EXCLUDED.cash,
    gross_exposure = EXCLUDED.gross_exposure,
    drawdown = EXCLUDED.drawdown,
    day_pnl = EXCLUDED.day_pnl
"""

_LOAD_ORDERS = """
SELECT client_order_id, session_id, venue, symbol, side, order_type, quantity, limit_price,
time_in_force, post_only, reduce_only, intent, status, venue_order_id, filled_quantity,
filled_notional, venue_filled_quantity, fees::text AS fees, reject_reason, created_ns,
updated_ns, version
FROM trading.orders
WHERE session_id = $1
ORDER BY created_ns, client_order_id
"""

_LOAD_FILLS = """
SELECT venue, symbol, trade_id, client_order_id, side, quantity, price, fee, fee_currency,
       liquidity, ts_ns
FROM trading.fills
WHERE session_id = $1
ORDER BY ts_ns, venue, symbol, trade_id
"""

_LIST_SESSIONS = """
SELECT session_id, mode, venue, strategy, symbols, timeframe, status, last_equity, halts,
       created_at, updated_at
FROM trading.sessions
ORDER BY updated_at DESC
LIMIT $1
"""

_RECENT_RISK_EVENTS = """
SELECT e.ts_ns, e.kind, e.code, e.message, e.symbol, e.detail::text AS detail, s.venue
FROM trading.risk_events e
JOIN trading.sessions s USING (session_id)
WHERE e.session_id = $1
ORDER BY e.id DESC
LIMIT $2
"""

_RECENT_EQUITY = """
SELECT ts, equity, cash, gross_exposure, drawdown, day_pnl
FROM trading.equity
WHERE session_id = $1
ORDER BY ts DESC
LIMIT $2
"""


class PostgresTradingStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # --------------------------------------------------------------------- sessions

    async def create_session(self, record: SessionRecord) -> None:
        state = record.state
        try:
            await self._pool.execute(
                _CREATE_SESSION,
                record.session_id,
                record.mode,
                record.venue,
                record.strategy,
                record.timeframe.value,
                [i.symbol for i in record.instruments],
                record.quote_currency,
                record.initial_capital,
                _json(record.config),
                record.status,
                record.fence,
                None if state is None else encode_state(state),
                None if state is None else state.last_equity,
                [] if state is None else [h.reason.value for h in state.halts],
            )
        except asyncpg.UniqueViolationError as exc:
            raise SessionConfigError(f"session {record.session_id!r} already exists") from exc
        except Exception as exc:
            raise translate(exc, f"create session {record.session_id}") from exc

    async def load_session(self, session_id: str) -> SessionRecord | None:
        try:
            row = await self._pool.fetchrow(_LOAD_SESSION, session_id)
        except Exception as exc:
            raise translate(exc, f"load session {session_id}") from exc
        return None if row is None else _session_record(row)

    async def claim_session(self, session_id: str) -> int:
        try:
            fence = await self._pool.fetchval(_CLAIM, session_id)
        except Exception as exc:
            raise translate(exc, f"claim session {session_id}") from exc
        if fence is None:
            raise SessionConfigError(f"session {session_id!r} does not exist")
        return int(fence)

    async def release_session(self, session_id: str, *, fence: int, status: str) -> None:
        try:
            result = await self._pool.execute(_RELEASE, session_id, fence, status)
        except Exception as exc:
            raise translate(exc, f"release session {session_id}") from exc
        if result.split()[-1] == "0":
            raise FencedOutError(
                f"session {session_id!r} is owned by a newer engine (fence {fence} is stale)"
            )

    async def list_sessions(self, *, limit: int) -> list[SessionSummary]:
        try:
            rows = await self._pool.fetch(_LIST_SESSIONS, limit)
        except Exception as exc:
            raise translate(exc, "list sessions") from exc
        return [
            SessionSummary(
                session_id=row["session_id"],
                mode=row["mode"],
                venue=row["venue"],
                strategy=row["strategy"],
                symbols=tuple(row["symbols"]),
                timeframe=row["timeframe"],
                status=row["status"],
                last_equity=row["last_equity"],
                halts=tuple(row["halts"]),
                created_ns=datetime_to_ns(row["created_at"]),
                updated_ns=datetime_to_ns(row["updated_at"]),
            )
            for row in rows
        ]

    # ------------------------------------------------------------------ orders/fills

    async def load_orders(self, session_id: str) -> list[OrderRecord]:
        try:
            rows = await self._pool.fetch(_LOAD_ORDERS, session_id)
        except Exception as exc:
            raise translate(exc, f"load orders of {session_id}") from exc
        return [_order_record(row) for row in rows]

    async def load_fills(self, session_id: str) -> list[OrderFill]:
        try:
            rows = await self._pool.fetch(_LOAD_FILLS, session_id)
        except Exception as exc:
            raise translate(exc, f"load fills of {session_id}") from exc
        return [_fill(row) for row in rows]

    async def recent_risk_events(self, session_id: str, *, limit: int) -> list[RiskEventRecord]:
        try:
            rows = await self._pool.fetch(_RECENT_RISK_EVENTS, session_id, limit)
        except Exception as exc:
            raise translate(exc, f"load risk events of {session_id}") from exc
        events = [
            RiskEventRecord(
                session_id=session_id,
                ts_ns=row["ts_ns"],
                kind=RiskEventKind(row["kind"]),
                code=row["code"],
                message=row["message"],
                instrument=None
                if row["symbol"] is None
                else InstrumentId(row["venue"], row["symbol"]),
                detail=orjson.loads(row["detail"]),
            )
            for row in rows
        ]
        events.reverse()  # oldest first
        return events

    async def recent_equity(self, session_id: str, *, limit: int) -> list[EquityPoint]:
        """The latest ``limit`` equity points, oldest first."""
        try:
            rows = await self._pool.fetch(_RECENT_EQUITY, session_id, limit)
        except Exception as exc:
            raise translate(exc, f"load the equity of {session_id}") from exc
        points = [
            EquityPoint(
                session_id=session_id,
                ts_ns=datetime_to_ns(row["ts"]),
                equity=row["equity"],
                cash=row["cash"],
                gross_exposure=row["gross_exposure"],
                drawdown=row["drawdown"],
                day_pnl=row["day_pnl"],
            )
            for row in rows
        ]
        points.reverse()
        return points

    # ------------------------------------------------------------------------ writes

    async def write(self, batch: WriteBatch, *, fence: int) -> None:
        if batch.empty:
            return
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                await _write_batch(conn, batch, fence)
        except (FencedOutError, StorageError):
            raise
        except Exception as exc:
            raise translate(exc, f"trading write for {batch.session_id}") from exc


async def _write_batch(conn: Any, batch: WriteBatch, fence: int) -> None:
    """The statements of one write, inside the caller's transaction."""
    session_id, state = batch.session_id, batch.state
    claimed = await conn.fetchval(
        _FENCED_STATE,
        session_id,
        fence,
        None if state is None else encode_state(state),
        None if state is None else state.last_equity,
        None if state is None else [h.reason.value for h in state.halts],
    )
    if claimed is None:
        current = await conn.fetchval(
            "SELECT fence FROM trading.sessions WHERE session_id = $1", session_id
        )
        raise _refusal(session_id, fence, current)
    if batch.orders:
        await conn.executemany(_UPSERT_ORDER, [_order_args(o) for o in batch.orders])
    if batch.fills:
        await conn.executemany(_INSERT_FILL, [_fill_args(f, session_id) for f in batch.fills])
    if batch.risk_events:
        await conn.executemany(_INSERT_RISK_EVENT, [_risk_event_args(e) for e in batch.risk_events])
    if batch.equity:
        await conn.executemany(_UPSERT_EQUITY, [_equity_args(p) for p in batch.equity])


# ============================================================================ mapping


def _refusal(session_id: str, fence: int, current: int | None) -> Exception:
    if current is None:
        return StorageError(f"session {session_id!r} does not exist")
    return FencedOutError(
        f"session {session_id!r} was taken over (fence {current}, this engine holds {fence})"
    )


def _json(value: Any) -> str:
    return orjson.dumps(value).decode()


def encode_state(state: SessionState) -> str:
    return _json(
        {
            "v": STATE_VERSION,
            "high_water_mark": _opt_str(state.high_water_mark),
            "risk_day": None if state.risk_day is None else state.risk_day.isoformat(),
            "day_start_equity": _opt_str(state.day_start_equity),
            "halts": [
                {
                    "reason": halt.reason.value,
                    "since_ns": halt.since_ns,
                    "detail": halt.detail,
                    "until_ns": halt.until_ns,
                }
                for halt in state.halts
            ],
            "last_equity": _opt_str(state.last_equity),
            "command_offset": state.command_offset,
            "flatten_reason": state.flatten_reason,
            "acted_signals": [[symbol, signal] for symbol, signal in state.acted_signals],
        }
    )


def decode_state(text: str) -> SessionState:
    try:
        data = orjson.loads(text)
    except orjson.JSONDecodeError as exc:
        raise StorageError(f"stored session state is not JSON: {exc}") from exc
    if not isinstance(data, dict) or data.get("v") != STATE_VERSION:
        version = data.get("v") if isinstance(data, dict) else None
        raise StorageError(f"stored session state version {version!r} is not supported")
    try:
        return SessionState(
            high_water_mark=_opt_decimal(data["high_water_mark"]),
            risk_day=None if data["risk_day"] is None else date.fromisoformat(data["risk_day"]),
            day_start_equity=_opt_decimal(data["day_start_equity"]),
            halts=tuple(
                Halt(
                    reason=HaltReason(h["reason"]),
                    since_ns=int(h["since_ns"]),
                    detail=str(h["detail"]),
                    until_ns=None if h["until_ns"] is None else int(h["until_ns"]),
                )
                for h in data["halts"]
            ),
            last_equity=_opt_decimal(data["last_equity"]),
            command_offset=data["command_offset"],
            flatten_reason=data["flatten_reason"],
            acted_signals=tuple((str(s), float(v)) for s, v in data["acted_signals"]),
        )
    except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
        raise StorageError(f"stored session state is unreadable: {exc!r}") from exc


def _opt_str(value: Decimal | None) -> str | None:
    return None if value is None else decimal_str(value)


def _opt_decimal(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def _session_record(row: asyncpg.Record) -> SessionRecord:
    try:
        venue = row["venue"]
        state_text = row["state"]
        return SessionRecord(
            session_id=row["session_id"],
            mode=row["mode"],
            venue=venue,
            strategy=row["strategy"],
            timeframe=Timeframe.parse(row["timeframe"]),
            instruments=tuple(InstrumentId(venue, symbol) for symbol in row["symbols"]),
            quote_currency=row["quote_currency"],
            initial_capital=row["initial_capital"],
            config=orjson.loads(row["config"]),
            status=row["status"],
            fence=int(row["fence"]),
            state=None if state_text is None else decode_state(state_text),
            created_ns=datetime_to_ns(row["created_at"]),
            updated_ns=datetime_to_ns(row["updated_at"]),
        )
    except (DomainError, ValueError) as exc:
        raise StorageError(f"stored session {row['session_id']!r} is unreadable: {exc}") from exc


def _order_args(order: OrderRecord) -> tuple[object, ...]:
    return (
        order.client_order_id,
        order.session_id,
        order.instrument.venue,
        order.instrument.symbol,
        order.side.value,
        order.order_type.value,
        order.quantity,
        order.limit_price,
        order.time_in_force.value,
        order.post_only,
        order.reduce_only,
        order.intent,
        order.status.value,
        order.venue_order_id,
        order.filled_quantity,
        order.filled_notional,
        order.venue_filled_quantity,
        _json({currency: decimal_str(amount) for currency, amount in order.fees}),
        order.reject_reason,
        order.created_ns,
        order.updated_ns,
        order.version,
    )


def _order_record(row: asyncpg.Record) -> OrderRecord:
    fees: dict[str, str] = orjson.loads(row["fees"])
    return OrderRecord(
        client_order_id=row["client_order_id"],
        session_id=row["session_id"],
        instrument=InstrumentId(row["venue"], row["symbol"]),
        side=OrderSide(row["side"]),
        order_type=OrderType(row["order_type"]),
        quantity=row["quantity"],
        limit_price=row["limit_price"],
        time_in_force=TimeInForce(row["time_in_force"]),
        post_only=row["post_only"],
        reduce_only=row["reduce_only"],
        intent=row["intent"],
        status=OrderStatus(row["status"]),
        venue_order_id=row["venue_order_id"],
        filled_quantity=row["filled_quantity"],
        filled_notional=row["filled_notional"],
        venue_filled_quantity=row["venue_filled_quantity"],
        fees=tuple(sorted((currency, Decimal(amount)) for currency, amount in fees.items())),
        reject_reason=row["reject_reason"],
        created_ns=row["created_ns"],
        updated_ns=row["updated_ns"],
        version=row["version"],
    )


def _fill_args(fill: OrderFill, session_id: str) -> tuple[object, ...]:
    return (
        fill.venue,
        fill.instrument.symbol,
        fill.trade_id,
        fill.client_order_id,
        session_id,
        fill.side.value,
        fill.quantity,
        fill.price,
        fill.fee,
        fill.fee_currency,
        fill.liquidity.value,
        fill.ts_ns,
    )


def _fill(row: asyncpg.Record) -> OrderFill:
    return OrderFill(
        venue=row["venue"],
        trade_id=row["trade_id"],
        client_order_id=row["client_order_id"],
        instrument=InstrumentId(row["venue"], row["symbol"]),
        side=OrderSide(row["side"]),
        quantity=row["quantity"],
        price=row["price"],
        fee=row["fee"],
        fee_currency=row["fee_currency"],
        liquidity=Liquidity(row["liquidity"]),
        ts_ns=row["ts_ns"],
    )


def _risk_event_args(event: RiskEventRecord) -> tuple[object, ...]:
    return (
        event.session_id,
        event.ts_ns,
        event.kind.value,
        event.code,
        event.message,
        None if event.instrument is None else event.instrument.symbol,
        _json(dict(event.detail)),
    )


def _equity_args(point: EquityPoint) -> tuple[object, ...]:
    return (
        point.session_id,
        ns_to_datetime(point.ts_ns),
        point.equity,
        point.cash,
        point.gross_exposure,
        point.drawdown,
        point.day_pnl,
    )


__all__ = ["PostgresTradingStore", "decode_state", "encode_state"]
