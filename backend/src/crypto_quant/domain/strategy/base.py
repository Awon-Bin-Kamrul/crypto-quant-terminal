"""The strategy interface.

A strategy turns closed bars into a **target exposure**: the fraction of equity it wants to
hold, positive for long and negative for short (``1.0`` = fully long, ``-0.5`` = half short,
``0.0`` = flat). Engines own everything else — order sizing, execution, costs, accounting —
so the same strategy object runs unchanged in the event-driven backtester and, later, in paper
and live trading.

Two ways to express the logic:

* :meth:`Strategy.on_bar` — incremental, called once per closed bar. Required. This is what
  the event-driven engine (and the live engine) calls.
* :meth:`VectorizedStrategy.vector_exposure` — the same rule as one Polars expression over the
  whole history. Optional; it unlocks fast parameter sweeps. A parity test keeps the two forms
  in agreement.

Returning ``None`` (or a null in the vectorized form) means "keep the current target", which is
how hysteresis rules (enter at one threshold, exit at another) are written without state.
"""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Generic, Protocol, TypeVar

from crypto_quant.domain.market import Bar

if TYPE_CHECKING:
    import polars as pl

ParamValue = int | float | bool | str


class StrategyParams:
    """Base class for parameter sets.

    Subclasses must be frozen dataclasses with a default for every field (defaults are the
    documented starting point), and should validate ranges in ``__post_init__``.
    """

    def as_dict(self) -> dict[str, ParamValue]:
        """Field values by name. Raises ``TypeError`` if the subclass is not a dataclass."""
        return {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}  # type: ignore[arg-type]


P = TypeVar("P", bound=StrategyParams)


class StrategyContext(Protocol):
    """Read-only view of the engine's state, handed to :meth:`Strategy.on_bar`."""

    @property
    def bar_index(self) -> int:
        """Zero-based index of the bar being processed."""
        ...

    @property
    def equity(self) -> float:
        """Portfolio value marked at this bar's close."""
        ...

    @property
    def position_units(self) -> float:
        """Signed position in base-asset units (negative = short)."""
        ...

    @property
    def exposure(self) -> float:
        """Current position value as a fraction of equity (drifts with price)."""
        ...

    @property
    def target_exposure(self) -> float:
        """The target the engine is currently working towards."""
        ...


class Strategy(ABC, Generic[P]):
    """Base class of every strategy. See the module docstring for the contract.

    Subclasses declare:
        name: registry key, e.g. ``"mean_reversion"``.
        params_type: the :class:`StrategyParams` dataclass this strategy accepts.
    """

    name: ClassVar[str]
    description: ClassVar[str] = ""
    params_type: ClassVar[type[StrategyParams]]

    def __init__(self, params: P) -> None:
        if not isinstance(params, self.params_type):
            raise TypeError(
                f"{type(self).__name__} expects {self.params_type.__name__}, "
                f"got {type(params).__name__}"
            )
        self.params: P = params

    @property
    @abstractmethod
    def warmup_bars(self) -> int:
        """Bars needed before the strategy can emit a decision (engines report it; strategies
        must return ``None`` until their indicators are ready)."""

    @abstractmethod
    def on_bar(self, bar: Bar, ctx: StrategyContext) -> float | None:
        """Decide after ``bar`` closed.

        Returns:
            The new target exposure, or ``None`` to keep the current one. Engines clamp the
            value to the configured leverage and, for long-only portfolios, to ``>= 0``.
        """


class VectorizedStrategy(Strategy[P]):
    """A strategy that can also express its rule as a single Polars expression."""

    @classmethod
    @abstractmethod
    def vector_exposure(cls, params: P) -> pl.Expr:
        """Target exposure for every bar, computed from the columns ``open``, ``high``,
        ``low``, ``close`` and ``volume`` of bars sorted oldest first.

        The value at row ``t`` may only use rows ``<= t`` (the engine trades it at the open of
        ``t + 1``). Nulls mean "keep the previous target", exactly like ``None`` from
        :meth:`Strategy.on_bar`.
        """
