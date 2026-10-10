/**
 * PnL and risk figures of a session, marked to the latest market prices.
 *
 * The engine reports equity once per base bar (1 minute). Between reports the terminal marks
 * positions to the live market itself: equity = cash + Σ quantity × mark (spot: one contract
 * is one unit). When a position has no mark yet, the last reported equity is shown instead, and
 * `equitySource` says which one the screen is looking at.
 */

import type { EquityPoint } from "@/lib/api/types";
import { chartTime } from "@/lib/market/candles";
import type { SessionView } from "@/lib/session/reducer";
import { workingOrders } from "@/lib/session/reducer";
import { num, optNum, sign } from "@/lib/numbers";

export type Marks = Readonly<Record<string, number | undefined>>;

export interface EquityLinePoint {
  /** Chart time: epoch seconds. */
  time: number;
  value: number;
}

/**
 * The equity curve as a chart line. The engine reports equity once per base bar and again
 * after fills, so several points can share one second; a chart needs strictly increasing
 * times, so each second keeps its last report.
 */
export function equityLine(points: readonly EquityPoint[]): EquityLinePoint[] {
  const line: EquityLinePoint[] = [];
  for (const point of [...points].sort((left, right) => left.ts - right.ts)) {
    const time = chartTime(point.ts);
    const value = num(point.equity);
    const last = line.at(-1);
    if (last !== undefined && last.time === time) last.value = value;
    else line.push({ time, value });
  }
  return line;
}

export interface PositionMetrics {
  symbol: string;
  quantity: number;
  side: "long" | "short" | "flat";
  averagePrice: number;
  mark: number | null;
  marketValue: number | null;
  /** (mark − average) × quantity; null without a mark. */
  unrealized: number | null;
  /** Unrealised PnL as a fraction of the position's cost. */
  unrealizedPct: number | null;
  realized: number;
  fees: number;
}

export interface SessionMetrics {
  quote: string;
  initialCapital: number;
  cash: number;
  equity: number | null;
  equitySource: "live" | "reported" | null;
  totalPnl: number | null;
  totalPnlPct: number | null;
  dayPnl: number | null;
  unrealized: number;
  realized: number;
  fees: number;
  drawdown: number | null;
  drawdownLimit: number | null;
  dailyLossLimit: number | null;
  /** Today's loss as a fraction of the day's starting equity (0 when up). */
  dayLoss: number | null;
  grossExposure: number | null;
  leverage: number | null;
  openPositions: number;
  workingOrders: number;
  highWaterMark: number | null;
  reportedAt: number | null;
}

export function positionMetrics(view: SessionView, marks: Marks): PositionMetrics[] {
  return view.info.symbols.map((symbol) => {
    const position = view.positions[symbol];
    const quantity = num(position?.quantity);
    const averagePrice = num(position?.average_price);
    const mark = marks[symbol] ?? null;
    const direction = sign(quantity);
    const cost = Math.abs(quantity * averagePrice);
    const unrealized = direction === 0 ? 0 : mark === null ? null : (mark - averagePrice) * quantity;
    return {
      symbol,
      quantity,
      side: direction > 0 ? "long" : direction < 0 ? "short" : "flat",
      averagePrice,
      mark,
      marketValue: mark === null ? null : quantity * mark,
      unrealized,
      unrealizedPct: unrealized === null || cost === 0 ? null : unrealized / cost,
      realized: num(position?.realized_pnl),
      fees: num(position?.fees),
    };
  });
}

function riskLimit(view: SessionView, key: string): number | null {
  const risk = view.info.config.risk;
  if (typeof risk !== "object" || risk === null) return null;
  const value = (risk as Record<string, unknown>)[key];
  return typeof value === "string" || typeof value === "number" ? optNum(value) : null;
}

export function sessionMetrics(view: SessionView, marks: Marks): SessionMetrics {
  const positions = positionMetrics(view, marks);
  const open = positions.filter((position) => position.side !== "flat");
  const initialCapital = num(view.account.initial_capital);
  const cash = num(view.account.cash);
  const reported = view.equity.at(-1);
  const allMarked = open.every((position) => position.mark !== null);

  let equity: number | null = null;
  let equitySource: SessionMetrics["equitySource"] = null;
  if (allMarked) {
    equity = cash + open.reduce((total, position) => total + (position.marketValue ?? 0), 0);
    equitySource = "live";
  } else if (reported !== undefined) {
    equity = num(reported.equity);
    equitySource = "reported";
  }

  const highWaterMark = optNum(view.state.high_water_mark);
  const dayStart = optNum(view.state.day_start_equity);
  let drawdown: number | null = reported !== undefined ? num(reported.drawdown) : null;
  if (equity !== null && highWaterMark !== null) {
    const peak = Math.max(highWaterMark, equity);
    drawdown = peak > 0 ? Math.max(0, (peak - equity) / peak) : 0;
  }
  let dayPnl: number | null = reported !== undefined ? num(reported.day_pnl) : null;
  if (equity !== null && dayStart !== null) dayPnl = equity - dayStart;
  const dayLoss = dayPnl !== null && dayStart !== null && dayStart > 0 ? Math.max(0, -dayPnl / dayStart) : null;

  const grossExposure = allMarked
    ? open.reduce((total, position) => total + Math.abs(position.marketValue ?? 0), 0)
    : reported !== undefined
      ? num(reported.gross_exposure)
      : null;

  return {
    quote: view.account.quote,
    initialCapital,
    cash,
    equity,
    equitySource,
    totalPnl: equity === null ? null : equity - initialCapital,
    totalPnlPct: equity === null || initialCapital === 0 ? null : (equity - initialCapital) / initialCapital,
    dayPnl,
    unrealized: open.reduce((total, position) => total + (position.unrealized ?? 0), 0),
    realized: num(view.account.realized_pnl),
    fees: num(view.account.fees),
    drawdown,
    drawdownLimit: riskLimit(view, "max_drawdown"),
    dailyLossLimit: riskLimit(view, "daily_loss_limit"),
    dayLoss,
    grossExposure,
    leverage: grossExposure === null || equity === null || equity <= 0 ? null : grossExposure / equity,
    openPositions: open.length,
    workingOrders: workingOrders(view).length,
    highWaterMark,
    reportedAt: reported?.ts ?? null,
  };
}
