"""``cq trade`` argument handling and output, with the composition root replaced."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest
from typer.testing import CliRunner

from crypto_quant.application.errors import (
    BusUnavailableError,
    LiveTradingNotArmedError,
    SessionConfigError,
    SessionLockedError,
    VenueUnavailableError,
)
from crypto_quant.application.execution.config import SizingMethod, TradingMode
from crypto_quant.application.execution.events import (
    CommandKind,
    RiskEventKind,
    RiskEventRecord,
    SessionState,
)
from crypto_quant.application.execution.runner import RunResult
from crypto_quant.application.ports.trading import SessionSummary
from crypto_quant.bootstrap import trading
from crypto_quant.domain.market import Timeframe
from crypto_quant.domain.orders import Order, OrderRecord, OrderRequest, OrderSide, OrderType
from crypto_quant.domain.risk import BreachAction, Halt, HaltReason
from crypto_quant.entrypoints.cli.main import app
from tests.support.trading import BTC, T0, session_config, session_record

D = Decimal
runner = CliRunner()


def working_order(*, reason: str | None = None) -> OrderRecord:
    order = Order.new(
        client_order_id="cqabc123",
        session_id="s1",
        request=OrderRequest(
            instrument=BTC,
            side=OrderSide.BUY,
            quantity=D("0.1"),
            order_type=OrderType.LIMIT,
            limit_price=D("49000"),
        ),
        intent="rebalance",
        now_ns=T0,
    )
    order.reject_reason = reason
    return order.record()


@dataclass
class FakeBootstrap:
    result: RunResult = field(
        default_factory=lambda: RunResult(reason="operator stop", failed=False)
    )
    runs: list[tuple[str, trading.NewSession | None, int | None]] = field(default_factory=list)
    run_error: BaseException | None = None
    commands: list[tuple[str, CommandKind, str, str]] = field(default_factory=list)
    summaries: list[SessionSummary] = field(default_factory=list)
    orders: list[OrderRecord] = field(default_factory=list)
    status: trading.SessionStatus | None = None
    engine: str | None = None
    working_only: list[bool] = field(default_factory=list)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(trading, "prepare", lambda _env_file: object())
        monkeypatch.setattr(trading, "run_session", self.run_session)
        monkeypatch.setattr(trading, "list_sessions", self.list_sessions)
        monkeypatch.setattr(trading, "session_status", self.session_status)
        monkeypatch.setattr(trading, "session_orders", self.session_orders)
        monkeypatch.setattr(trading, "send_command", self.send_command)

    async def run_session(
        self, settings: Any, session_id: str, request: Any, *, metrics_port: int | None
    ) -> RunResult:
        self.runs.append((session_id, request, metrics_port))
        if self.run_error is not None:
            raise self.run_error
        return self.result

    async def list_sessions(self, settings: Any, *, limit: int) -> list[SessionSummary]:
        return self.summaries

    async def session_status(
        self, settings: Any, session_id: str, *, events: int
    ) -> trading.SessionStatus:
        if self.status is None:
            raise SessionConfigError(f"session {session_id!r} does not exist")
        return self.status

    async def session_orders(
        self, settings: Any, session_id: str, *, working_only: bool
    ) -> list[OrderRecord]:
        self.working_only.append(working_only)
        return self.orders

    async def send_command(
        self, settings: Any, session_id: str, kind: CommandKind, *, reason: str, issued_by: str
    ) -> trading.CommandReceipt:
        self.commands.append((session_id, kind, reason, issued_by))
        return trading.CommandReceipt(offset="7-0", engine=self.engine)


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeBootstrap:
    bootstrap = FakeBootstrap()
    bootstrap.install(monkeypatch)
    return bootstrap


def test_a_new_session_is_described_by_its_options(fake: FakeBootstrap) -> None:
    result = runner.invoke(
        app,
        [
            "trade", "run", "mom-1", "--strategy", "momentum", "-s", "BTC/USDT", "-s", "ETH/USDT",
            "-t", "15m", "-p", "lookback=12", "--mode", "paper", "--capital", "2500.5",
            "--sizing", "vol_parity", "--target-vol", "0.3", "--order-type", "limit",
            "--limit-offset-bps", "2", "--post-only", "--max-drawdown", "none",
            "--daily-loss", "0.03", "--daily-loss-action", "halt", "--max-order", "500",
            "--max-gross", "5000", "--max-position", "2000", "--metrics-port", "9200",
        ],
    )  # fmt: skip
    assert result.exit_code == 0, result.output
    assert "session mom-1 stopped: operator stop" in result.output
    ((session_id, request, port),) = fake.runs
    assert session_id == "mom-1" and port == 9200 and request is not None
    assert request.strategy == "momentum" and request.params["lookback"] == 12
    assert request.symbols == ("BTC/USDT", "ETH/USDT") and request.timeframe is Timeframe.M15
    assert request.mode is TradingMode.PAPER and request.capital == D("2500.5")
    assert request.sizing.method is SizingMethod.VOL_PARITY
    assert request.sizing.target_volatility == 0.3
    assert request.execution.post_only and request.execution.order_type is OrderType.LIMIT
    risk = request.risk
    assert risk.max_drawdown is None and risk.daily_loss_limit == D("0.03")
    assert risk.daily_loss_action is BreachAction.HALT
    assert risk.max_order_notional == D("500") and risk.max_gross_exposure == D("5000")


def test_resuming_and_failures(fake: FakeBootstrap) -> None:
    resumed = runner.invoke(app, ["trade", "run", "mom-1"])
    assert resumed.exit_code == 0 and fake.runs[-1][1] is None
    assert fake.runs[-1][2] is None, "the port defaults to CQ_TRADE_METRICS_PORT"
    on_port = runner.invoke(app, ["trade", "run", "mom-1", "--metrics-port", "0"])
    assert on_port.exit_code == 0 and fake.runs[-1][2] == 0
    fake.result = RunResult(reason="venue failure: key revoked", failed=True)
    failed = runner.invoke(app, ["trade", "run", "mom-1"])
    assert failed.exit_code == 1 and "key revoked" in failed.output


@pytest.mark.parametrize(
    ("error", "exit_code", "message"),
    [
        (BusUnavailableError("Redis: ConnectionError: refused"), 1, "Redis"),
        (SessionLockedError("session mom-1 is owned by host-2-7"), 1, "host-2-7"),
        (VenueUnavailableError("binance: 503"), 1, "503"),
        (asyncio.CancelledError(), 1, "interrupted"),
        (LiveTradingNotArmedError("CQ_LIVE_TRADING_ENABLED is false"), 2, "ENABLED"),
        (SessionConfigError("stored configuration differs: capital"), 2, "differs"),
    ],
)
def test_run_failures_map_to_exit_codes(
    fake: FakeBootstrap, error: BaseException, exit_code: int, message: str
) -> None:
    """Exit 1 = retry later (bus, lease, venue, signal); exit 2 = fix the request."""
    fake.run_error = error
    result = runner.invoke(app, ["trade", "run", "mom-1"])
    assert result.exit_code == exit_code, result.output
    assert message in result.output


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--strategy", "momentum"], "--symbol"),
        (["-s", "BTC/USDT"], "--strategy"),
        (["--mode", "live"], "new sessions only"),
        (["--capital", "5000"], "--capital: new sessions only"),
        (["--sizing", "kelly", "--post-only"], "--sizing, --post-only: new sessions only"),
        (["--max-drawdown", "0.1", "-p", "lookback=3"], "--param, --max-drawdown"),
        (["--strategy", "nope", "-s", "BTC/USDT"], "unknown strategy"),
        (["--strategy", "momentum", "-s", "BTC/USDT", "--capital", "lots"], "not a number"),
        (["--strategy", "momentum", "-s", "BTC/USDT", "--max-order", "inf"], "not finite"),
        (["--strategy", "momentum", "-s", "BTC/USDT", "--max-drawdown", "2"], "max_drawdown"),
        (["--strategy", "momentum", "-s", "BTC/USDT", "-t", "7m"], "7m"),
        (["--strategy", "momentum", "-s", "BTC/USDT", "--fraction", "0"], "fraction"),
    ],
)
def test_invalid_run_options_are_refused(
    fake: FakeBootstrap, args: list[str], message: str
) -> None:
    result = runner.invoke(app, ["trade", "run", "mom-1", *args])
    assert result.exit_code == 2, result.output
    assert message in result.output
    assert fake.runs == []


def test_sessions_are_listed(fake: FakeBootstrap) -> None:
    assert "no sessions" in runner.invoke(app, ["trade", "sessions"]).output
    fake.summaries = [
        SessionSummary("mom-1", "paper", "binance", "momentum", ("BTC/USDT",), "1h", "running",
                       D("10250.5"), ("kill_switch",), T0, T0),
        SessionSummary("mr-2", "live", "binance", "mean_reversion", ("ETH/USDT", "BTC/USDT"), "15m",
                       "stopped", None, (), T0, T0),
    ]  # fmt: skip
    output = runner.invoke(app, ["trade", "sessions", "--limit", "5"]).output
    assert "mom-1" in output and "10,250.50" in output and "kill_switch" in output
    assert "ETH/USDT,BTC/USDT" in output


def test_status_shows_the_whole_picture(fake: FakeBootstrap) -> None:
    missing = runner.invoke(app, ["trade", "status", "mom-1"])
    assert missing.exit_code == 2 and "does not exist" in missing.output
    config = session_config(session_id="mom-1")
    state = SessionState(
        high_water_mark=D("10000"),
        risk_day=None,
        day_start_equity=None,
        halts=(
            Halt(HaltReason.DAILY_LOSS, T0, "5% lost", until_ns=T0 + 3_600_000_000_000),
            Halt(HaltReason.KILL_SWITCH, T0, "operator"),
        ),
        last_equity=D("9500"),
        command_offset=None,
        flatten_reason="daily loss 5.00%",
    )
    fake.status = trading.SessionStatus(
        record=session_record(config, state),
        positions=(("BTC/USDT", D("0.25")),),
        working_orders=(working_order(),),
        recent_risk_events=(
            RiskEventRecord("mom-1", T0, RiskEventKind.BREACH, "daily_loss", "5% lost", BTC),
            RiskEventRecord("mom-1", T0, RiskEventKind.HALT, "kill_switch", "operator"),
        ),
        engine="host-1-42",
    )
    output = runner.invoke(app, ["trade", "status", "mom-1"]).output
    for expected in (
        "engine        host-1-42",
        "equity        9,500.00",
        "HALT          daily_loss until",
        "HALT          kill_switch: operator",
        "flattening    daily loss 5.00%",
        "position      BTC/USDT     0.25",
        "working orders (1)",
        "cqabc123",
        "breach      daily_loss BTC/USDT: 5% lost",
    ):
        assert expected in output, output


def test_orders_are_listed(fake: FakeBootstrap) -> None:
    assert "no orders" in runner.invoke(app, ["trade", "orders", "mom-1"]).output
    fake.orders = [working_order(reason="insufficient balance")]
    output = runner.invoke(app, ["trade", "orders", "mom-1", "--all"]).output
    assert "@49000" in output and "(insufficient balance)" in output
    assert fake.working_only == [True, False]


@pytest.mark.parametrize(
    ("command", "kind"),
    [
        ("halt", CommandKind.HALT),
        ("flatten", CommandKind.FLATTEN),
        ("resume", CommandKind.RESUME),
        ("stop", CommandKind.STOP),
    ],
)
def test_operator_commands_are_queued(fake: FakeBootstrap, command: str, kind: CommandKind) -> None:
    result = runner.invoke(app, ["trade", command, "mom-1", "-r", "because"])
    assert result.exit_code == 0, result.output
    assert f"{kind.value} queued for mom-1 (offset 7-0)" in result.output
    assert "next starts" in result.output
    ((session_id, sent_kind, reason, issued_by),) = fake.commands
    assert (session_id, sent_kind, reason) == ("mom-1", kind, "because")
    assert issued_by.startswith("cli:")
    fake.engine = "host-1-42"
    again = runner.invoke(app, ["trade", command, "mom-1", "-r", "again"])
    assert "engine host-1-42 will apply it now" in again.output
