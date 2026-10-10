"""Composition root: builds adapters and wires them to ports for a given mode.

The only place that knows which concrete adapter satisfies which port in backtest, paper and
live mode, and the only place that resolves strategies from the strategy registry.

Enforced by the ``layers`` import contract (backend/pyproject.toml).
"""
