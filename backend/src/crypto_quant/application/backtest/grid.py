"""Parameter grids for sweeps, and parsing of ``name=value`` strings from the CLI."""

from __future__ import annotations

import dataclasses
import itertools
import typing
from collections.abc import Iterator, Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

from crypto_quant.domain.strategy import ParamValue, StrategyParams

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}
_RANGE_PARTS = 3
_MAX_RANGE_VALUES = 10_000


def _field_types(params_type: type[StrategyParams]) -> dict[str, type[Any]]:
    """Declared type of every field; ``dataclasses.fields`` raises for non-dataclasses."""
    hints = typing.get_type_hints(params_type)
    return {f.name: hints[f.name] for f in dataclasses.fields(params_type)}  # type: ignore[arg-type]


def coerce(params_type: type[StrategyParams], name: str, text: str) -> ParamValue:
    """Convert CLI text to the declared type of parameter ``name``."""
    types = _field_types(params_type)
    if name not in types:
        known = ", ".join(sorted(types))
        raise ValueError(f"{params_type.__name__} has no parameter {name!r} (known: {known})")
    try:
        return _convert(types[name], text.strip())
    except ValueError as exc:
        raise ValueError(f"parameter {name}: {exc}") from exc


def _convert(target: type[Any], raw: str) -> ParamValue:
    if target is bool:
        lowered = raw.lower()
        if lowered in _TRUE:
            return True
        if lowered in _FALSE:
            return False
        raise ValueError(f"expected a boolean, got {raw!r}")
    if target is int:
        return int(raw)
    if target is float:
        return float(raw)
    if target is str:
        return raw
    raise ValueError(f"unsupported parameter type {target!r}")


def parse_assignments(
    params_type: type[StrategyParams], assignments: Sequence[str]
) -> dict[str, list[ParamValue]]:
    """Parse ``["lookback=12,24", "entry_z=1.5:2.5:0.5"]`` into
    ``{"lookback": [12, 24], "entry_z": [1.5, 2.0, 2.5]}``.

    A numeric value may be an inclusive range ``start:stop:step``.
    """
    parsed: dict[str, list[ParamValue]] = {}
    types = _field_types(params_type)
    for assignment in assignments:
        name, sep, values = assignment.partition("=")
        if not sep or not name.strip() or not values.strip():
            raise ValueError(f"expected name=value[,value...], got {assignment!r}")
        key = name.strip()
        if key in parsed:
            raise ValueError(f"parameter {key!r} given twice")
        options: list[ParamValue] = []
        for token in (v.strip() for v in values.split(",")):
            if not token:
                continue
            if ":" in token and types.get(key) in (int, float):
                options.extend(_expand_range(key, token, types[key]))
            else:
                options.append(coerce(params_type, key, token))
        if not options:
            raise ValueError(f"no values given for {key!r}")
        parsed[key] = list(dict.fromkeys(options))  # drop duplicates, keep order
    return parsed


def _expand_range(name: str, token: str, target: type[Any]) -> list[ParamValue]:
    parts = token.split(":")
    if len(parts) != _RANGE_PARTS:
        raise ValueError(f"parameter {name}: expected start:stop:step, got {token!r}")
    try:
        start, stop, step = (Decimal(part.strip()) for part in parts)
    except InvalidOperation as exc:
        raise ValueError(f"parameter {name}: {token!r} is not a numeric range") from exc
    if step <= 0:
        raise ValueError(f"parameter {name}: range step must be positive")
    if stop < start:
        raise ValueError(f"parameter {name}: range stop is below its start")
    count = int((stop - start) / step) + 1
    if count > _MAX_RANGE_VALUES:
        raise ValueError(f"parameter {name}: range yields {count} values (max {_MAX_RANGE_VALUES})")
    # Decimal arithmetic: 0.1 steps land exactly on 0.3, not 0.30000000000000004.
    values = [start + step * i for i in range(count)]
    if target is int:
        if any(v != v.to_integral_value() for v in values):
            raise ValueError(f"parameter {name}: integer range {token!r} has fractional values")
        return [int(v) for v in values]
    return [float(v) for v in values]


def build_params(
    params_type: type[StrategyParams], values: Mapping[str, ParamValue]
) -> StrategyParams:
    """Defaults overridden by ``values``; the dataclass validates the combination."""
    _field_types(params_type)  # rejects non-dataclasses with a clear message
    return params_type(**values)


class ParameterGrid:
    """Cartesian product of parameter values, in a deterministic order.

    Invalid combinations (for example ``exit_z >= entry_z``) are skipped and counted rather
    than aborting the whole sweep.
    """

    def __init__(
        self, params_type: type[StrategyParams], values: Mapping[str, Sequence[ParamValue]]
    ) -> None:
        types = _field_types(params_type)
        unknown = sorted(set(values) - set(types))
        if unknown:
            raise ValueError(f"{params_type.__name__} has no parameters {unknown}")
        empty = sorted(name for name, options in values.items() if not options)
        if empty:
            raise ValueError(f"no values given for {empty}")
        self.params_type = params_type
        self.values = {name: tuple(options) for name, options in values.items()}
        self.skipped: list[tuple[dict[str, ParamValue], str]] = []

    @property
    def size(self) -> int:
        size = 1
        for options in self.values.values():
            size *= len(options)
        return size

    def __iter__(self) -> Iterator[StrategyParams]:
        self.skipped = []
        names = list(self.values)
        for combination in itertools.product(*(self.values[name] for name in names)):
            assignment = dict(zip(names, combination, strict=True))
            try:
                yield build_params(self.params_type, assignment)
            except ValueError as exc:
                self.skipped.append((assignment, str(exc)))
