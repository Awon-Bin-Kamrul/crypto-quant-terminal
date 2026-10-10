"""Infrastructure layer: driven adapters that implement the application ports.

TimescaleDB (asyncpg), Redis, CCXT, configuration, logging and metrics live here. Adapters
translate between the outside world and domain types and validate at the boundary.

Enforced by the ``layers`` import contract (backend/pyproject.toml).
"""
