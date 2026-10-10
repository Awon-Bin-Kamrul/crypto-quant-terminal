"""Strategy library: concrete strategies built on ``crypto_quant.domain.strategy``.

Strategies may import the domain and nothing else, so a strategy cannot touch the network,
the database or the clock directly. It therefore behaves identically in a backtest, a paper
session and live trading.

Enforced by the ``strategies-sandboxed`` import contract (backend/pyproject.toml).

Each strategy's vectorized form imports Polars inside the method: processes that only run
strategies bar by bar (the event-driven engine, and later paper and live trading) never pay
Polars' import time and memory.
"""
