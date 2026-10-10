"""Which sessions this deployment may run (ARCHITECTURE D10: two-key live arming).

Real money moves only when every one of these holds:

1. the deployment is set to live (``CQ_TRADING_MODE=live``) — key one;
2. live trading is explicitly enabled (``CQ_LIVE_TRADING_ENABLED=true``) — key two;
3. the exchange is not in sandbox mode, and API credentials are configured;
4. deployment-wide hard caps on order size and gross exposure are set, and the session's
   quote currency is a USD stablecoin so that those USD caps mean what they say;
5. the market is spot (derivatives trading is not part of this release);
6. at start-up, the API key cannot withdraw funds (checked against the venue where it can
   be, by the composition root).

A session can never run in a mode above the deployment's: a paper deployment refuses to
resume a live session, whatever the session's stored configuration says.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Final

from crypto_quant.application.errors import LiveTradingNotArmedError, SessionConfigError
from crypto_quant.application.execution.config import SessionConfig, TradingMode

_RANK: Final = {TradingMode.PAPER: 0, TradingMode.SANDBOX: 1, TradingMode.LIVE: 2}

USD_QUOTES: Final = frozenset({"USD", "USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD"})


@dataclass(frozen=True, slots=True)
class Deployment:
    """The deployment-wide switches, independent of any one session."""

    trading_mode: TradingMode
    live_trading_enabled: bool
    exchange_sandbox: bool
    has_credentials: bool
    market_type: str
    max_order_notional: Decimal | None
    max_gross_exposure: Decimal | None


def check_arming(config: SessionConfig, deployment: Deployment) -> None:
    """Refuse a session this deployment must not run; returns silently otherwise."""
    mode = config.mode
    if _RANK[mode] > _RANK[deployment.trading_mode]:
        raise LiveTradingNotArmedError(
            f"session {config.session_id!r} is a {mode} session but CQ_TRADING_MODE is "
            f"{deployment.trading_mode}"
        )
    if mode is TradingMode.PAPER:
        return
    if deployment.market_type != "spot":
        raise SessionConfigError(
            f"{mode} trading is spot-only in this release (CQ_EXCHANGE_MARKET_TYPE is "
            f"{deployment.market_type})"
        )
    if not deployment.has_credentials:
        raise SessionConfigError(
            f"{mode} trading needs CQ_EXCHANGE_API_KEY and CQ_EXCHANGE_API_SECRET"
        )
    if mode is TradingMode.SANDBOX:
        return
    problems: list[str] = []
    if not deployment.live_trading_enabled:
        problems.append("CQ_LIVE_TRADING_ENABLED is not true")
    if deployment.exchange_sandbox:
        problems.append("CQ_EXCHANGE_SANDBOX is true (live orders would go to the test network)")
    if deployment.max_order_notional is None or deployment.max_gross_exposure is None:
        problems.append("CQ_MAX_ORDER_NOTIONAL_USD and CQ_MAX_GROSS_EXPOSURE_USD must both be set")
    if config.quote_currency not in USD_QUOTES:
        problems.append(
            f"the quote currency {config.quote_currency} is not a USD stablecoin, so the USD "
            "hard caps cannot be applied"
        )
    if problems:
        raise LiveTradingNotArmedError("live trading is not armed: " + "; ".join(problems))


def with_hard_caps(config: SessionConfig, deployment: Deployment) -> SessionConfig:
    """The session's configuration with its risk limits tightened by the deployment's hard
    caps. Applied at every start (never stored), so lowering a cap takes effect on restart."""
    risk = config.risk.capped_by(
        max_order_notional=deployment.max_order_notional,
        max_gross_exposure=deployment.max_gross_exposure,
    )
    return config if risk == config.risk else replace(config, risk=risk)
