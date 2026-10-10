"""Entrypoints: driving adapters (FastAPI, the ``cq`` CLI, long-running workers).

They translate external input into use-case calls on the container that ``bootstrap``
builds, and never reach into ``infrastructure`` directly.

Enforced by the ``layers`` and ``entrypoints-via-bootstrap`` import contracts.
"""
