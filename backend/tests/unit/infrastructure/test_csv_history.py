from __future__ import annotations

import gzip
import zipfile
from pathlib import Path

import numpy as np
import pytest

from crypto_quant.application.errors import HistoryFormatError, HistoryNotFoundError
from crypto_quant.domain.market import Timeframe, TimeRange
from crypto_quant.domain.market.frame import MarketFrame, resample
from crypto_quant.infrastructure.files.csv_history import (
    CsvBarHistory,
    infer_timeframe,
    parse_time,
    read_csv_bars,
)
from tests.support.backtest import BTC, T0, synthetic_frame

M1 = Timeframe.M1
MINUTE = M1.duration_ns
BASE = synthetic_frame(24 * 60 * 2, timeframe=M1, seed=4)  # two days of 1-minute bars


def _rows(frame: MarketFrame, time_format: str = "ms") -> list[list[str]]:
    rows = []
    for k in range(len(frame)):
        ts = int(frame.open_time_ns[k])
        stamp = {
            "ms": str(ts // 1_000_000),
            "us": str(ts // 1_000),
            "s": str(ts // 1_000_000_000),
            "ns": str(ts),
            "iso": np.datetime_as_string(np.datetime64(ts, "ns"), unit="s") + "Z",
        }[time_format]
        rows.append(
            [
                stamp,
                repr(float(frame.open[k])),
                repr(float(frame.high[k])),
                repr(float(frame.low[k])),
                repr(float(frame.close[k])),
                repr(float(frame.volume[k])),
            ]
        )
    return rows


def _write(path: Path, lines: list[list[str]], header: list[str] | None = None) -> Path:
    body = "\n".join(",".join(row) for row in ([header] if header else []) + lines) + "\n"
    path.write_text(body, encoding="utf-8")
    return path


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_parse_time_infers_the_epoch_unit(unit: str) -> None:
    ts = T0 + 90 * MINUTE
    divisor = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}[unit]
    assert parse_time(str(ts // divisor)) == ts


def test_parse_time_accepts_iso_strings() -> None:
    assert parse_time("2024-01-01T00:00:00Z") == T0
    assert parse_time("2024-01-01 00:00:00") == T0  # naive means UTC
    assert parse_time("2024-01-01T02:00:00+02:00") == T0
    assert parse_time("1704067200.0") == T0
    with pytest.raises(ValueError, match="isoformat"):
        parse_time("yesterday")


async def test_binance_layout_without_header(tmp_path: Path) -> None:
    lines = [[*row, "0", "0"] for row in _rows(BASE, "us")]  # extra columns are ignored
    path = _write(tmp_path / "BTCUSDT-1m.csv", lines)
    history = await CsvBarHistory(path).load(BTC, Timeframe.H1, None)
    expected = resample(BASE, Timeframe.H1)
    assert history.frame.content_hash() == expected.content_hash()
    assert history.partial_bars == 0
    assert history.source == f"csv:{path}"


async def test_header_layout_window_and_partial_buckets(tmp_path: Path) -> None:
    # Drop 10 minutes inside hour 3: that hour becomes a partial bar.
    keep = np.ones(len(BASE), dtype=bool)
    keep[3 * 60 + 5 : 3 * 60 + 15] = False
    rows = [row for row, kept in zip(_rows(BASE, "iso"), keep, strict=True) if kept]
    path = _write(
        tmp_path / "bars.csv", rows, ["Timestamp", "Open", "High", "Low", "Close", "Volume"]
    )
    window = TimeRange(T0 + 2 * 60 * MINUTE, T0 + 6 * 60 * MINUTE)
    history = await CsvBarHistory(path).load(BTC, Timeframe.H1, window)
    assert len(history.frame) == 4
    assert history.frame.start_ns == window.start_ns
    assert history.partial_bars == 1


@pytest.mark.parametrize("compression", ["gz", "zip"])
async def test_compressed_files(tmp_path: Path, compression: str) -> None:
    text = "\n".join(",".join(row) for row in _rows(BASE.slice(0, 120))) + "\n"
    if compression == "gz":
        path = tmp_path / "bars.csv.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            handle.write(text)
    else:
        path = tmp_path / "bars.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("BTCUSDT-1m-2024-01-01.csv", text)
    history = await CsvBarHistory(path).load(BTC, Timeframe.M15, None)
    assert len(history.frame) == 8


def test_rows_are_sorted_and_the_timeframe_inferred(tmp_path: Path) -> None:
    rows = _rows(BASE.slice(0, 30))
    path = _write(tmp_path / "shuffled.csv", rows[10:] + rows[:10])
    raw = read_csv_bars(path)
    assert (np.diff(raw.open_time_ns) > 0).all()
    assert infer_timeframe(raw.open_time_ns) is M1


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("", "is empty"),
        ("time,open,high,low,close\n1,2,3,4,5\n", "missing columns volume"),
        ("when,open,high,low,close,volume\n", "no time column"),
        ("1704067200000,1,2,0.5,1.5\n", "at least 6 columns"),
        ("1704067200000,1,2,0.5,abc,1\n", "bars.csv:1"),
        (
            "time,open,high,low,close,volume\n2024-01-01,1,2,0.5,1.5,1\n2024-13-01,1,2,0.5,1.5,1\n",
            "bars.csv:3",
        ),
        ("1704067200000,1,2,0.5,1.5,1\n1704067200000,1,2,0.5,1.5,1\n", "duplicate open time"),
        ("time,open,high,low,close,volume\n\n", "holds no bars"),
    ],
)
def test_malformed_files(tmp_path: Path, content: str, message: str) -> None:
    path = tmp_path / "bars.csv"
    path.write_text(content, encoding="utf-8")
    with pytest.raises((HistoryFormatError, HistoryNotFoundError), match=message):
        read_csv_bars(path)


async def test_load_errors(tmp_path: Path) -> None:
    path = _write(tmp_path / "bars.csv", _rows(BASE.slice(0, 60)))
    with pytest.raises(HistoryFormatError, match="cannot be built from 1h"):
        await CsvBarHistory(path, base_timeframe=Timeframe.H1).load(BTC, Timeframe.M5, None)
    with pytest.raises(HistoryNotFoundError, match="no bars in"):
        await CsvBarHistory(path).load(
            BTC, Timeframe.H1, TimeRange(T0 + 24 * 60 * MINUTE, T0 + 25 * 60 * MINUTE)
        )
    bad_ohlc = [
        ["1704067200000", "10", "9", "8", "9.5", "1"],
        ["1704067260000", "1", "2", "1", "2", "1"],
    ]
    with pytest.raises(HistoryFormatError, match="inconsistent OHLCV"):
        await CsvBarHistory(_write(tmp_path / "bad.csv", bad_ohlc)).load(BTC, M1, None)
    odd = [["1704067200000", "1", "2", "1", "2", "1"], ["1704067207000", "1", "2", "1", "2", "1"]]
    with pytest.raises(HistoryFormatError, match="matches no supported timeframe"):
        await CsvBarHistory(_write(tmp_path / "odd.csv", odd)).load(BTC, M1, None)
    with pytest.raises(HistoryFormatError, match="cannot read"):
        await CsvBarHistory(tmp_path / "missing.csv").load(BTC, M1, None)
    single = _write(tmp_path / "single.csv", _rows(BASE.slice(0, 1)))
    with pytest.raises(HistoryFormatError, match="single bar"):
        await CsvBarHistory(single).load(BTC, M1, None)


async def test_byte_order_mark_and_damaged_archives(tmp_path: Path) -> None:
    rows = _rows(BASE.slice(0, 30))
    text = "open_time,open,high,low,close,volume\n" + "\n".join(",".join(r) for r in rows)
    bom = tmp_path / "excel.csv"
    bom.write_text("﻿" + text + "\n", encoding="utf-8")
    assert len((await CsvBarHistory(bom).load(BTC, M1, None)).frame) == 30

    truncated = tmp_path / "cut.csv.gz"
    truncated.write_bytes(gzip.compress((text * 50).encode())[:300])
    with pytest.raises(HistoryFormatError, match=r"cut\.csv\.gz"):
        read_csv_bars(truncated)

    absurd = tmp_path / "absurd.csv"
    absurd.write_text("inf,1,2,0.5,1.5,1\n", encoding="utf-8")
    with pytest.raises(HistoryFormatError, match=r"absurd\.csv:1"):
        read_csv_bars(absurd)
