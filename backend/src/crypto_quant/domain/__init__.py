"""Domain layer: the trading model and its rules.

Pure Python: the standard library, ``Decimal``, and Polars/NumPy for computation. No I/O, no
frameworks, no logging, no knowledge of whether it runs in a backtest or in production.

Enforced by the ``layers`` and ``domain-pure`` import contracts (backend/pyproject.toml).
"""
