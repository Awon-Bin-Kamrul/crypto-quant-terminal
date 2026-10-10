"""Paper and live execution: the trading core, order management, venues and the runner.

One deterministic core (``core.TradingCore``) turns events — closed bars, venue reports,
operator commands, timer ticks — into effects: orders to submit or cancel, records to persist,
events to publish. The runner (``runner.ExecutionRunner``) feeds it and carries the effects
out. Paper and live sessions differ only in the venue adapter plugged into the runner.
"""
