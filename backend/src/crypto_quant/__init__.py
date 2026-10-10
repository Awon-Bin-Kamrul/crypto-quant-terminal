"""Crypto Quant Terminal: event-driven crypto research, backtesting and execution."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str = version("crypto-quant-terminal")
except PackageNotFoundError:  # imported from a source tree that was never installed
    __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
