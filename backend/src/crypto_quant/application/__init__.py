"""Application layer: use cases, the trading engine and the ports they depend on.

Orchestrates the domain through ``application.ports`` (typing.Protocol interfaces). Never
imports an adapter, a web framework, a database driver or an exchange client.

Enforced by the ``layers`` and ``application-no-io`` import contracts (backend/pyproject.toml).
"""
