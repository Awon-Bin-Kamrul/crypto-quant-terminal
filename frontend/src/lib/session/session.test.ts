import assert from "node:assert/strict";
import { describe, it } from "node:test";

import type { EquityPoint, Fill, Order, SessionSnapshot } from "@/lib/api/types";
import { equityLine, sessionMetrics, positionMetrics } from "@/lib/session/metrics";
import {
  LIMITS,
  applyUpdate,
  fromSnapshot,
  orderHistory,
  workingOrders,
} from "@/lib/session/reducer";

const T0 = Date.UTC(2026, 0, 1);

function order(id: string, overrides: Partial<Order> = {}): Order {
  return {
    client_order_id: id,
    venue_order_id: null,
    symbol: "BTC/USDT",
    side: "buy",
    type: "limit",
    tif: "gtc",
    post_only: false,
    reduce_only: false,
    status: "open",
    quantity: "0.2",
    filled: "0",
    average_price: null,
    limit_price: "49950",
    reason: null,
    intent: "rebalance",
    created: T0,
    ts: T0,
    version: 2,
    ...overrides,
  };
}

function fill(id: string, overrides: Partial<Fill> = {}): Fill {
  return {
    client_order_id: "cq-1",
    trade_id: id,
    symbol: "BTC/USDT",
    side: "buy",
    quantity: "0.1",
    price: "50000",
    fee: "0.5",
    fee_currency: "USDT",
    liquidity: "taker",
    ts: T0 + 1_000,
    ...overrides,
  };
}

function point(minute: number, equity: string): EquityPoint {
  return {
    ts: T0 + minute * 60_000,
    equity,
    cash: "4999.5",
    gross_exposure: "5000",
    drawdown: "0.01",
    day_pnl: "-10",
  };
}

function snapshot(overrides: Partial<SessionSnapshot> = {}): SessionSnapshot {
  return {
    session: {
      id: "mom-1",
      mode: "paper",
      venue: "binance",
      strategy: "momentum",
      timeframe: "1h",
      symbols: ["BTC/USDT", "ETH/USDT"],
      quote: "USDT",
      initial_capital: "10000",
      status: "running",
      created: T0,
      updated: T0,
      config: { risk: { max_drawdown: "0.2", daily_loss_limit: "0.05" } },
    },
    engine: { state: "running", instance: "engine-1" },
    state: { halts: [], flatten: null, high_water_mark: "10100", day_start_equity: "10050" },
    flatten: null,
    account: {
      quote: "USDT",
      initial_capital: "10000",
      cash: "4999.5",
      realized_pnl: "12",
      fees: "0.5",
    },
    positions: [
      { symbol: "BTC/USDT", quantity: "0.1", average_price: "50000", realized_pnl: "12", fees: "0.5" },
      { symbol: "ETH/USDT", quantity: "0", average_price: "0", realized_pnl: "0", fees: "0" },
    ],
    orders: [order("cq-1"), order("cq-0", { status: "filled", ts: T0 - 5 })],
    fills: [fill("t1"), fill("t1")],
    equity: [point(0, "10040")],
    risk: [],
    ...overrides,
  };
}

describe("session reducer", () => {
  it("builds a view from the snapshot", () => {
    const view = fromSnapshot(snapshot());
    assert.equal(view.info.id, "mom-1");
    assert.equal(view.fills.length, 1, "duplicate fills collapse");
    assert.deepEqual(
      workingOrders(view).map((o) => o.client_order_id),
      ["cq-1"],
    );
    assert.deepEqual(
      orderHistory(view).map((o) => o.client_order_id),
      ["cq-0"],
    );
    assert.equal(view.positions["BTC/USDT"]?.quantity, "0.1");
  });

  it("keeps the highest order version and ignores stale updates", () => {
    const view = fromSnapshot(snapshot());
    const stale = applyUpdate(view, "order", order("cq-1", { version: 1, status: "pending" }));
    assert.equal(stale, view);
    const filled = applyUpdate(view, "order", order("cq-1", { version: 3, status: "filled", ts: T0 + 9 }));
    assert.equal(filled.orders["cq-1"]?.status, "filled");
    assert.deepEqual(workingOrders(filled), []);
    const sameVersionOlder = applyUpdate(filled, "order", order("cq-1", { version: 3, ts: T0 }));
    assert.equal(sameVersionOlder, filled);
  });

  it("appends fills, equity and risk events once", () => {
    let view = fromSnapshot(snapshot());
    assert.equal(applyUpdate(view, "fill", fill("t1")), view);
    view = applyUpdate(view, "fill", fill("t2", { ts: T0 + 500 }));
    assert.deepEqual(
      view.fills.map((f) => f.trade_id),
      ["t2", "t1"],
      "kept in time order",
    );
    assert.equal(applyUpdate(view, "equity", point(0, "1")), view);
    view = applyUpdate(view, "equity", point(1, "10060"));
    assert.equal(view.equity.at(-1)?.equity, "10060");
    const event = { kind: "halt" as const, code: "kill_switch", message: "manual", symbol: null, ts: T0 };
    view = applyUpdate(view, "risk", event);
    assert.equal(applyUpdate(view, "risk", { ...event }), view);
    assert.equal(view.risk.length, 1);
    assert.equal(applyUpdate(view, "telemetry", {}), view);
  });

  it("replaces positions, account, state, flatten and engine", () => {
    let view = fromSnapshot(snapshot());
    view = applyUpdate(view, "position", {
      symbol: "BTC/USDT",
      quantity: "0.3",
      average_price: "50100",
      realized_pnl: "12",
      fees: "1",
    });
    view = applyUpdate(view, "account", { ...view.account, cash: "1" });
    view = applyUpdate(view, "state", { ...view.state, flatten: "drawdown 20%" });
    view = applyUpdate(view, "flatten", { state: "started", reason: "drawdown 20%" });
    view = applyUpdate(view, "engine", { state: "stopping", instance: "engine-1" });
    assert.equal(view.positions["BTC/USDT"]?.quantity, "0.3");
    assert.equal(view.account.cash, "1");
    assert.equal(view.state.flatten, "drawdown 20%");
    assert.equal(view.flatten?.state, "started");
    assert.equal(view.engine.state, "stopping");
  });

  it("bounds how much history it keeps", () => {
    let view = fromSnapshot(snapshot({ fills: [], orders: [] }));
    for (let i = 0; i < LIMITS.fills + 10; i += 1) {
      view = applyUpdate(view, "fill", fill(`t${i}`, { ts: T0 + i }));
    }
    assert.equal(view.fills.length, LIMITS.fills);
    assert.equal(view.fills[0]?.trade_id, "t10");
    for (let i = 0; i < LIMITS.finishedOrders + 5; i += 1) {
      view = applyUpdate(view, "order", order(`done-${i}`, { status: "cancelled", ts: T0 + i }));
    }
    view = applyUpdate(view, "order", order("working"));
    assert.equal(orderHistory(view).length, LIMITS.finishedOrders);
    assert.equal(workingOrders(view).length, 1);
  });
});

describe("session metrics", () => {
  it("marks positions to the live market", () => {
    const view = fromSnapshot(snapshot());
    const metrics = sessionMetrics(view, { "BTC/USDT": 51_000 });
    assert.equal(metrics.equitySource, "live");
    assert.equal(metrics.equity, 4999.5 + 5_100);
    assert.equal(metrics.totalPnl, 99.5);
    assert.ok(Math.abs((metrics.totalPnlPct ?? 0) - 0.00995) < 1e-12);
    assert.equal(metrics.unrealized, 100);
    assert.equal(metrics.realized, 12);
    assert.equal(metrics.dayPnl, 10099.5 - 10050);
    assert.equal(metrics.dayLoss, 0);
    assert.equal(metrics.drawdown, 0.00004950495049504951);
    assert.equal(metrics.drawdownLimit, 0.2);
    assert.equal(metrics.dailyLossLimit, 0.05);
    assert.equal(metrics.grossExposure, 5_100);
    assert.ok(Math.abs((metrics.leverage ?? 0) - 5_100 / 10_099.5) < 1e-12);
    assert.equal(metrics.openPositions, 1);
    assert.equal(metrics.workingOrders, 1);

    const [btc, eth] = positionMetrics(view, { "BTC/USDT": 49_000 });
    assert.equal(btc?.side, "long");
    assert.equal(btc?.unrealized, -100);
    assert.equal(btc?.unrealizedPct, -0.02);
    assert.equal(eth?.side, "flat");
    assert.equal(eth?.unrealized, 0);
  });

  it("falls back to the engine's reported equity without marks", () => {
    const view = fromSnapshot(snapshot());
    const metrics = sessionMetrics(view, {});
    assert.equal(metrics.equitySource, "reported");
    assert.equal(metrics.equity, 10_040);
    assert.equal(metrics.grossExposure, 5_000);
    assert.equal(metrics.reportedAt, T0);
    const empty = sessionMetrics(fromSnapshot(snapshot({ equity: [] })), {});
    assert.equal(empty.equity, null);
    assert.equal(empty.totalPnl, null);
  });
});

describe("equity line", () => {
  const point = (ts: number, equity: string): EquityPoint => ({
    ts,
    equity,
    cash: equity,
    gross_exposure: "0",
    drawdown: "0",
    day_pnl: "0",
  });

  it("keeps one point per second, the last report winning, in time order", () => {
    const line = equityLine([
      point(T0 + 60_150, "10002"),
      point(T0, "10000"),
      point(T0 + 60_155, "10003.5"), // reported after a fill in the same second
      point(T0 + 120_000, "9999"),
    ]);
    assert.deepEqual(line, [
      { time: T0 / 1000, value: 10_000 },
      { time: T0 / 1000 + 60, value: 10_003.5 },
      { time: T0 / 1000 + 120, value: 9_999 },
    ]);
    assert.deepEqual(equityLine([]), []);
  });
});
