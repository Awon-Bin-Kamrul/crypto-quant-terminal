import assert from "node:assert/strict";
import { describe, it } from "node:test";

import type { BookPayload, Candle, Fill } from "@/lib/api/types";
import { ladder } from "@/lib/market/book";
import { chartTime, lastChange, mergeCandle, mergeHistory, withSnapshot } from "@/lib/market/candles";
import { buildMarkers, labelledMarkers, type TradeMarker } from "@/lib/market/markers";
import { bucketStart, isTimeframe, timeframeMs } from "@/lib/market/timeframes";

const T0 = Date.UTC(2026, 0, 1);
const candle = (minute: number, close: number): Candle => ({
  t: T0 + minute * 60_000,
  o: close,
  h: close + 1,
  l: close - 1,
  c: close,
  v: 10,
});

describe("candles", () => {
  it("appends the next candle and replaces the newest one", () => {
    const start = [candle(0, 100), candle(1, 101)];
    const appended = mergeCandle(start, candle(2, 102));
    assert.equal(appended.length, 3);
    assert.equal(start.length, 2, "the input is not modified");
    const replaced = mergeCandle(appended, { ...candle(2, 103) });
    assert.equal(replaced.at(-1)?.c, 103);
    assert.equal(mergeCandle(replaced, candle(2, 103)), replaced, "unchanged: same list");
    const corrected = mergeCandle(replaced, candle(1, 99));
    assert.equal(corrected[1]?.c, 99);
    assert.equal(mergeCandle(corrected, candle(-5, 1)), corrected, "unknown old candle ignored");
    assert.deepEqual(mergeCandle([], candle(0, 1)), [candle(0, 1)]);
  });

  it("merges history pages in front, live values winning", () => {
    const live = [candle(2, 102), candle(3, 103)];
    const merged = mergeHistory(live, [candle(0, 100), candle(1, 101), candle(2, 999)]);
    assert.deepEqual(
      merged.map((c) => c.c),
      [100, 101, 102, 103],
    );
    assert.equal(mergeHistory(live, []), live);
  });

  it("keeps scrolled-back history across a fresh snapshot only when the two overlap", () => {
    const held = [candle(0, 100), candle(1, 101), candle(2, 102), candle(3, 103)];
    const snapshot = [candle(2, 202), candle(3, 203), candle(4, 204)];
    assert.deepEqual(
      withSnapshot(held, snapshot).map((c) => c.c),
      [100, 101, 202, 203, 204],
      "older candles kept, snapshot values win",
    );
    const later = [candle(9, 109), candle(10, 110)];
    assert.equal(withSnapshot(held, later), later, "a hole between them: history dropped");
    assert.equal(withSnapshot([], snapshot), snapshot);
    assert.deepEqual(withSnapshot(held, []), []);
  });

  it("reports the last change and converts times", () => {
    assert.deepEqual(lastChange([candle(0, 100), candle(1, 110)]), {
      price: 110,
      change: 10,
      pct: 0.1,
    });
    assert.equal(lastChange([]), null);
    assert.equal(chartTime(T0 + 1_999), T0 / 1000 + 1);
  });
});

describe("timeframes", () => {
  it("knows candle lengths and buckets", () => {
    assert.equal(timeframeMs("15m"), 900_000);
    assert.equal(bucketStart(T0 + 3_700_000, "1h"), T0 + 3_600_000);
    assert.equal(isTimeframe("4h"), true);
    assert.equal(isTimeframe("7m"), false);
    assert.throws(() => timeframeMs("7m"), /unknown timeframe/);
  });
});

describe("trade markers", () => {
  const fill = (minute: number, side: "buy" | "sell", qty: string, price: string, id: string): Fill => ({
    client_order_id: `cq-${id}`,
    trade_id: id,
    symbol: "BTC/USDT",
    side,
    quantity: qty,
    price,
    fee: "0",
    fee_currency: "USDT",
    liquidity: "taker",
    ts: T0 + minute * 60_000 + 12_000,
  });

  it("groups fills per candle and side with a volume-weighted price", () => {
    const fills = [
      fill(1, "buy", "0.1", "100", "a"),
      fill(3, "buy", "0.3", "104", "b"),
      fill(4, "sell", "0.2", "110", "c"),
      fill(70, "buy", "1", "100", "d"),
      { ...fill(2, "buy", "5", "1", "e"), symbol: "ETH/USDT" },
    ];
    const markers = buildMarkers(fills, {
      symbol: "BTC/USDT",
      timeframe: "5m",
      range: { from: T0, to: T0 + 60 * 60_000 },
    });
    assert.equal(markers.length, 2, "other symbols and candles off the chart are left out");
    const [buy, sell] = markers;
    assert.equal(buy?.time, T0 / 1000);
    assert.equal(buy?.shape, "arrowUp");
    assert.equal(buy?.position, "belowBar");
    assert.equal(buy?.text, "B ×2 0.4 @ 103.000");
    assert.equal(buy?.label, "B ×2 0.4");
    assert.equal(buy?.count, 2);
    assert.equal(sell?.shape, "arrowDown");
    assert.equal(sell?.text, "S 0.2 @ 110.000");
    assert.deepEqual(buildMarkers(fills, { symbol: "BTC/USDT", timeframe: "5m", range: null }), []);
  });
});

describe("marker labels", () => {
  const marker = (minute: number, position: TradeMarker["position"], label = "B ×2 0.4"): TradeMarker => ({
    id: `${minute}:${position}`,
    time: (T0 + minute * 60_000) / 1000,
    position,
    shape: position === "belowBar" ? "arrowUp" : "arrowDown",
    side: position === "belowBar" ? "buy" : "sell",
    label,
    text: label,
    quantity: 0.4,
    price: 100,
    count: 2,
  });
  const markers = [marker(0, "belowBar"), marker(1, "belowBar"), marker(2, "aboveBar"), marker(30, "belowBar")];

  it("drops labels that would overprint on the same side, keeping the newest", () => {
    const tight = labelledMarkers(markers, 8, 60_000);
    assert.deepEqual([...tight].sort(), ["1:belowBar", "2:aboveBar", "30:belowBar"]);
    const wide = labelledMarkers(markers, 80, 60_000);
    assert.equal(wide.size, 4, "zoomed in: every label fits");
    assert.equal(labelledMarkers(markers, 0, 60_000).size, 0, "unknown spacing: no labels yet");
  });
});

describe("order book ladder", () => {
  it("accumulates depth from the touch outwards", () => {
    const book: BookPayload = {
      ts: T0,
      bids: [
        [100, 1],
        [99, 3],
      ],
      asks: [
        [101, 2],
        [102, 2],
        [103, 4],
      ],
      bid: 100,
      ask: 101,
      mid: 100.5,
      spread: 1,
      spread_bps: 99.5,
    };
    const rows = ladder(book, 2);
    assert.deepEqual(
      rows.asks.map((row) => [row.price, row.total, row.depth]),
      [
        [101, 2, 0.5],
        [102, 4, 1],
      ],
    );
    assert.deepEqual(
      rows.bids.map((row) => [row.price, row.total, row.depth]),
      [
        [100, 1, 0.25],
        [99, 4, 1],
      ],
    );
    assert.equal(rows.mid, 100.5);
    assert.deepEqual(ladder(null, 5).asks, []);
  });
});
