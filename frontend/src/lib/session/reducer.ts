/**
 * The client's copy of one trading session, built from the gateway's snapshot and kept current
 * by the `session:` stream's deltas. Every change returns a new object (React state), and
 * applying a delta is idempotent: orders keep their highest version, fills are keyed by trade
 * id, equity points by time — so a resent or reordered delta never corrupts the view.
 */

import {
  WORKING_STATUSES,
  type Account,
  type Engine,
  type EquityPoint,
  type Fill,
  type Flatten,
  type Order,
  type Position,
  type RiskEvent,
  type SessionInfo,
  type SessionSnapshot,
  type SessionStateView,
} from "@/lib/api/types";

export const LIMITS = {
  fills: 500,
  finishedOrders: 200,
  equity: 2_000,
  risk: 200,
} as const;

export interface SessionView {
  info: SessionInfo;
  engine: Engine;
  state: SessionStateView;
  flatten: Flatten | null;
  account: Account;
  positions: Readonly<Record<string, Position>>;
  orders: Readonly<Record<string, Order>>;
  /** Oldest first. */
  fills: readonly Fill[];
  /** Oldest first. */
  equity: readonly EquityPoint[];
  /** Oldest first. */
  risk: readonly RiskEvent[];
}

export function fromSnapshot(snapshot: SessionSnapshot): SessionView {
  const orders: Record<string, Order> = {};
  for (const order of snapshot.orders) orders[order.client_order_id] = order;
  const positions: Record<string, Position> = {};
  for (const position of snapshot.positions) positions[position.symbol] = position;
  return {
    info: snapshot.session,
    engine: snapshot.engine,
    state: snapshot.state,
    flatten: snapshot.flatten,
    account: snapshot.account,
    positions,
    orders: pruneOrders(orders),
    fills: dedupeFills(snapshot.fills).slice(-LIMITS.fills),
    equity: snapshot.equity.slice(-LIMITS.equity),
    risk: snapshot.risk.slice(-LIMITS.risk),
  };
}

/** Apply one delta; unknown kinds and stale data leave the view unchanged (same object). */
export function applyUpdate(view: SessionView, kind: string, data: unknown): SessionView {
  switch (kind) {
    case "order":
      return applyOrder(view, data as Order);
    case "fill":
      return applyFill(view, data as Fill);
    case "position": {
      const position = data as Position;
      return { ...view, positions: { ...view.positions, [position.symbol]: position } };
    }
    case "account":
      return { ...view, account: data as Account };
    case "equity": {
      const point = data as EquityPoint;
      const last = view.equity.at(-1);
      if (last !== undefined && point.ts <= last.ts) return view;
      return { ...view, equity: [...view.equity, point].slice(-LIMITS.equity) };
    }
    case "risk": {
      const event = data as RiskEvent;
      if (view.risk.some((known) => sameRisk(known, event))) return view;
      return { ...view, risk: [...view.risk, event].slice(-LIMITS.risk) };
    }
    case "state":
      return { ...view, state: data as SessionStateView };
    case "flatten":
      return { ...view, flatten: data as Flatten };
    case "engine":
      return { ...view, engine: data as Engine };
    default:
      return view;
  }
}

function applyOrder(view: SessionView, order: Order): SessionView {
  const known = view.orders[order.client_order_id];
  if (known !== undefined && (known.version > order.version || (known.version === order.version && known.ts >= order.ts))) {
    return view;
  }
  return { ...view, orders: pruneOrders({ ...view.orders, [order.client_order_id]: order }) };
}

function applyFill(view: SessionView, fill: Fill): SessionView {
  if (view.fills.some((known) => sameFill(known, fill))) return view;
  const fills = [...view.fills, fill].sort((a, b) => a.ts - b.ts).slice(-LIMITS.fills);
  return { ...view, fills };
}

function sameFill(a: Fill, b: Fill): boolean {
  return a.trade_id === b.trade_id && a.symbol === b.symbol;
}

function sameRisk(a: RiskEvent, b: RiskEvent): boolean {
  return a.ts === b.ts && a.kind === b.kind && a.code === b.code && a.message === b.message;
}

function dedupeFills(fills: readonly Fill[]): Fill[] {
  const seen = new Set<string>();
  const unique: Fill[] = [];
  for (const fill of [...fills].sort((a, b) => a.ts - b.ts)) {
    const key = `${fill.symbol}:${fill.trade_id}`;
    if (!seen.has(key)) {
      seen.add(key);
      unique.push(fill);
    }
  }
  return unique;
}

/** Keep every working order and the most recent finished ones. */
function pruneOrders(orders: Record<string, Order>): Record<string, Order> {
  const finished = Object.values(orders)
    .filter((order) => !WORKING_STATUSES.has(order.status))
    .sort((a, b) => b.ts - a.ts);
  if (finished.length <= LIMITS.finishedOrders) return orders;
  const pruned = { ...orders };
  for (const order of finished.slice(LIMITS.finishedOrders)) delete pruned[order.client_order_id];
  return pruned;
}

// -------------------------------------------------------------------------- selectors

/** Working orders, newest first. */
export function workingOrders(view: SessionView, symbol?: string): Order[] {
  return Object.values(view.orders)
    .filter((order) => WORKING_STATUSES.has(order.status) && (!symbol || order.symbol === symbol))
    .sort((a, b) => b.created - a.created);
}

/** Finished orders, most recent first. */
export function orderHistory(view: SessionView): Order[] {
  return Object.values(view.orders)
    .filter((order) => !WORKING_STATUSES.has(order.status))
    .sort((a, b) => b.ts - a.ts);
}
