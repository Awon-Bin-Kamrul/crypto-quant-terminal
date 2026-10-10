import assert from "node:assert/strict";
import { describe, it } from "node:test";

import type { Market, SessionSummary } from "@/lib/api/types";
import { defaultSession, resolveMarket } from "@/lib/selection";

const MARKETS: Market[] = [
  { venue: "binance", symbol: "BTC/USDT" },
  { venue: "binance", symbol: "ETH/USDT" },
  { venue: "kraken", symbol: "BTC/USD" },
];

function summary(id: string, status: string): SessionSummary {
  return {
    id,
    mode: "paper",
    venue: "binance",
    strategy: "momentum",
    symbols: ["BTC/USDT"],
    timeframe: "1h",
    status,
    last_equity: null,
    halts: [],
    created: 0,
    updated: 0,
  };
}

describe("resolveMarket", () => {
  it("uses the stored selection as is before the catalog has loaded", () => {
    const resolved = resolveMarket({ venue: "binance", symbol: "SOL/USDT", markets: [], session: null });
    assert.deepEqual(resolved, { venue: "binance", symbol: "SOL/USDT", symbols: ["SOL/USDT"] });
    assert.deepEqual(resolveMarket({ venue: null, symbol: null, markets: [], session: null }), {
      venue: null,
      symbol: null,
      symbols: [],
    });
  });

  it("keeps a selection the gateway offers and replaces one it does not", () => {
    assert.deepEqual(resolveMarket({ venue: "binance", symbol: "ETH/USDT", markets: MARKETS, session: null }), {
      venue: "binance",
      symbol: "ETH/USDT",
      symbols: ["BTC/USDT", "ETH/USDT"],
    });
    const stale = resolveMarket({ venue: "binance", symbol: "DOGE/USDT", markets: MARKETS, session: null });
    assert.equal(stale.symbol, "BTC/USDT");
    const gone = resolveMarket({ venue: "bybit", symbol: "BTC/USDT", markets: MARKETS, session: null });
    assert.deepEqual(gone, { venue: "binance", symbol: "BTC/USDT", symbols: ["BTC/USDT", "ETH/USDT"] });
    const first = resolveMarket({ venue: null, symbol: null, markets: MARKETS, session: null });
    assert.equal(first.venue, "binance");
    assert.equal(first.symbol, "BTC/USDT");
  });

  it("puts a session's instruments first and fixes its venue", () => {
    const resolved = resolveMarket({
      venue: "kraken",
      symbol: "BTC/USD",
      markets: MARKETS,
      session: { venue: "binance", symbols: ["SOL/USDT", "BTC/USDT"] },
    });
    assert.deepEqual(resolved, {
      venue: "binance",
      symbol: "SOL/USDT",
      symbols: ["SOL/USDT", "BTC/USDT", "ETH/USDT"],
    });
  });
});

describe("defaultSession", () => {
  it("prefers a running session, then the most recent one", () => {
    assert.equal(defaultSession([]), null);
    assert.equal(defaultSession([summary("a", "stopped"), summary("b", "running")]), "b");
    assert.equal(defaultSession([summary("a", "stopped"), summary("b", "stopped")]), "a");
  });
});
