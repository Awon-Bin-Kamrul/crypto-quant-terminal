/**
 * Wire types of the terminal gateway. They mirror the backend's published schemas
 * (`backend/src/crypto_quant/entrypoints/api/schemas.py`, also served as OpenAPI at
 * `/api/openapi.json`). Decimal amounts arrive as strings so no precision is lost in transit;
 * times are epoch milliseconds.
 */

export type DecimalString = string;
export type EpochMs = number;

// ------------------------------------------------------------------------------- system

export interface Health {
  status: "ok" | "degraded";
  database: boolean;
  redis: boolean;
  version: string;
}

export interface Ticket {
  ticket: string;
  expires_in: number;
}

// ------------------------------------------------------------------------------ markets

export interface Market {
  venue: string;
  symbol: string;
}

export interface Markets {
  markets: Market[];
  timeframes: string[];
}

/** One candle; `t` is its open time. */
export interface Candle {
  t: EpochMs;
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
}

export interface CandlesPage {
  venue: string;
  symbol: string;
  timeframe: string;
  candles: Candle[];
}

export interface BookPayload {
  ts: EpochMs | null;
  /** `[price, size]`, best first. */
  bids: [number, number][];
  asks: [number, number][];
  bid: number | null;
  ask: number | null;
  mid: number | null;
  spread: number | null;
  spread_bps: number | null;
}

export interface BookSnapshot {
  venue: string;
  symbol: string;
  book: BookPayload;
}

export interface CandlesSnapshot {
  venue: string;
  symbol: string;
  timeframe: string;
  candles: Candle[];
}

export interface StreamHealth {
  name: string;
  kind: "bars" | "book";
  symbol: string;
  state: "starting" | "healthy" | "reconnecting" | "stale" | "failed" | "stopped";
  age_ms: number | null;
  reconnects: number;
  error: string | null;
}

export interface FeedHealth {
  venue: string;
  alive: boolean;
  instance: string | null;
  ts: EpochMs | null;
  age_ms: number | null;
  streams: StreamHealth[];
}

// ----------------------------------------------------------------------------- sessions

export type TradingMode = "paper" | "sandbox" | "live";

export interface SessionSummary {
  id: string;
  mode: TradingMode | string;
  venue: string;
  strategy: string;
  symbols: string[];
  timeframe: string;
  status: string;
  last_equity: DecimalString | null;
  halts: string[];
  created: EpochMs;
  updated: EpochMs;
}

export interface SessionInfo {
  id: string;
  mode: TradingMode;
  venue: string;
  strategy: string;
  timeframe: string;
  symbols: string[];
  quote: string;
  initial_capital: DecimalString;
  status: string;
  created: EpochMs;
  updated: EpochMs;
  config: Record<string, unknown>;
}

export interface Engine {
  state: "running" | "stopping" | "stopped";
  instance: string | null;
}

export interface Halt {
  reason: string;
  since: EpochMs;
  until: EpochMs | null;
  detail: string;
  resumable: boolean;
}

export interface SessionStateView {
  halts: Halt[];
  flatten: string | null;
  high_water_mark: DecimalString | null;
  day_start_equity: DecimalString | null;
}

export interface Flatten {
  state: "started" | "completed" | "incomplete" | "ended";
  reason: string;
  left_open?: string | null;
  note?: string | null;
}

export interface Account {
  quote: string;
  initial_capital: DecimalString;
  cash: DecimalString;
  realized_pnl: DecimalString;
  fees: DecimalString;
}

export interface Position {
  symbol: string;
  quantity: DecimalString;
  average_price: DecimalString;
  realized_pnl: DecimalString;
  fees: DecimalString;
}

export type OrderSide = "buy" | "sell";
export type OrderStatus =
  | "pending"
  | "unknown"
  | "open"
  | "partially_filled"
  | "cancel_pending"
  | "filled"
  | "cancelled"
  | "rejected"
  | "expired";

export interface Order {
  client_order_id: string;
  venue_order_id: string | null;
  symbol: string;
  side: OrderSide;
  type: "market" | "limit";
  tif: "gtc" | "ioc" | "fok";
  post_only: boolean;
  reduce_only: boolean;
  status: OrderStatus;
  quantity: DecimalString;
  filled: DecimalString;
  average_price: DecimalString | null;
  limit_price: DecimalString | null;
  reason: string | null;
  intent: string;
  created: EpochMs;
  ts: EpochMs;
  version: number;
  note?: string | null;
}

export interface Fill {
  client_order_id: string;
  trade_id: string;
  symbol: string;
  side: OrderSide;
  quantity: DecimalString;
  price: DecimalString;
  fee: DecimalString;
  fee_currency: string;
  liquidity: "maker" | "taker";
  ts: EpochMs;
}

export interface EquityPoint {
  ts: EpochMs;
  equity: DecimalString;
  cash: DecimalString;
  gross_exposure: DecimalString;
  drawdown: DecimalString;
  day_pnl: DecimalString;
}

export type RiskEventKind =
  | "rejected"
  | "adjusted"
  | "breach"
  | "halt"
  | "resume"
  | "anomaly"
  | "market_data";

export interface RiskEvent {
  kind: RiskEventKind;
  code: string;
  message: string;
  symbol: string | null;
  ts: EpochMs;
}

export interface SessionSnapshot {
  session: SessionInfo;
  engine: Engine;
  state: SessionStateView;
  flatten: Flatten | null;
  account: Account;
  positions: Position[];
  orders: Order[];
  fills: Fill[];
  equity: EquityPoint[];
  risk: RiskEvent[];
}

/** Deltas of the `session:` stream, keyed by their `kind`. */
export interface SessionUpdates {
  order: Order;
  fill: Fill;
  position: Position;
  account: Account;
  equity: EquityPoint;
  risk: RiskEvent;
  state: SessionStateView;
  flatten: Flatten;
  engine: Engine;
}

export type SessionUpdateKind = keyof SessionUpdates;

// ----------------------------------------------------------------------------- commands

export type CommandKind = "halt" | "flatten" | "resume" | "stop";

export interface CommandReceipt {
  offset: string;
  engine: string | null;
}

export const WORKING_STATUSES: ReadonlySet<OrderStatus> = new Set<OrderStatus>([
  "pending",
  "unknown",
  "open",
  "partially_filled",
  "cancel_pending",
]);
