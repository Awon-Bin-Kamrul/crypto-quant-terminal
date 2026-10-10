"""Instrument identity."""

from __future__ import annotations

import re
from dataclasses import dataclass

from crypto_quant.domain.shared.errors import InvalidInstrumentError

# CCXT unified symbols: BASE/QUOTE for spot, BASE/QUOTE:SETTLE for perpetuals,
# BASE/QUOTE:SETTLE-YYMMDD[-STRIKE-C|P] for dated futures and options.
_ASSET = r"[A-Za-z0-9._]{1,24}"
_UNIFIED_SYMBOL = re.compile(rf"^{_ASSET}/{_ASSET}(:{_ASSET}(-[0-9A-Za-z.-]{{1,32}})?)?$")
_VENUE = re.compile(r"^[a-z][a-z0-9]{1,31}$")


@dataclass(frozen=True, slots=True, order=True)
class InstrumentId:
    """A tradable market on one venue, e.g. ``binance`` + ``BTC/USDT``.

    ``venue`` is the CCXT exchange id; ``symbol`` is the CCXT unified symbol, so the same
    identifier works for REST, WebSocket and storage without translation tables.
    """

    venue: str
    symbol: str

    def __post_init__(self) -> None:
        if not _VENUE.fullmatch(self.venue):
            raise InvalidInstrumentError(f"venue {self.venue!r} must be a lower-case CCXT id")
        if not _UNIFIED_SYMBOL.fullmatch(self.symbol):
            raise InvalidInstrumentError(
                f"symbol {self.symbol!r} is not a CCXT unified symbol like 'BTC/USDT'"
            )

    @property
    def slug(self) -> str:
        """Key- and URL-safe symbol form: ``BTC/USDT`` → ``BTC-USDT``,
        ``BTC/USDT:USDT`` → ``BTC-USDT-USDT``."""
        return self.symbol.replace("/", "-").replace(":", "-")

    def __str__(self) -> str:
        return f"{self.venue}:{self.symbol}"
