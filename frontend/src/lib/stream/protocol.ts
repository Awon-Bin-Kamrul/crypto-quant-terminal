/**
 * The gateway's stream protocol (see `backend/src/crypto_quant/entrypoints/api/streaming.py`).
 *
 * A snapshot replaces whatever the client holds for its topic; updates follow with `seq`
 * 1, 2, 3 … A gap in `seq` means messages were lost, and the client asks for a new snapshot.
 */

export const PROTOCOL_VERSION = 1;

export interface HelloFrame {
  type: "hello";
  v: number;
  connection: string;
  heartbeat_s: number;
}

export interface SubscriptionFrame {
  type: "subscribed" | "unsubscribed";
  topic: string;
}

export interface SnapshotFrame {
  type: "snapshot";
  topic: string;
  seq: number;
  data: unknown;
}

export interface UpdateFrame {
  type: "update";
  topic: string;
  seq: number;
  kind: string;
  data: unknown;
}

export interface ErrorFrame {
  type: "error";
  topic: string | null;
  code: string;
  message: string;
}

export interface PongFrame {
  type: "pong";
  id: number | null;
  ts: number;
}

export interface HeartbeatFrame {
  type: "heartbeat";
  ts: number;
}

export type ServerFrame =
  | HelloFrame
  | SubscriptionFrame
  | SnapshotFrame
  | UpdateFrame
  | ErrorFrame
  | PongFrame
  | HeartbeatFrame;

export type ClientFrame =
  | { op: "subscribe"; topic: string }
  | { op: "unsubscribe"; topic: string }
  | { op: "ping"; id: number };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Parse one frame; `null` for anything that is not a well-formed server frame. */
export function parseFrame(text: string): ServerFrame | null {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return null;
  }
  if (!isRecord(value) || typeof value.type !== "string") return null;
  switch (value.type) {
    case "hello":
      return typeof value.heartbeat_s === "number" && typeof value.connection === "string"
        ? (value as unknown as HelloFrame)
        : null;
    case "subscribed":
    case "unsubscribed":
      return typeof value.topic === "string" ? (value as unknown as SubscriptionFrame) : null;
    case "snapshot":
      return typeof value.topic === "string" && typeof value.seq === "number"
        ? (value as unknown as SnapshotFrame)
        : null;
    case "update":
      return typeof value.topic === "string" &&
        typeof value.seq === "number" &&
        typeof value.kind === "string"
        ? (value as unknown as UpdateFrame)
        : null;
    case "error":
      return typeof value.code === "string"
        ? ({
            type: "error",
            topic: typeof value.topic === "string" ? value.topic : null,
            code: value.code,
            message: typeof value.message === "string" ? value.message : "",
          } satisfies ErrorFrame)
        : null;
    case "pong":
      return {
        type: "pong",
        id: typeof value.id === "number" ? value.id : null,
        ts: typeof value.ts === "number" ? value.ts : 0,
      };
    case "heartbeat":
      return { type: "heartbeat", ts: typeof value.ts === "number" ? value.ts : 0 };
    default:
      return null;
  }
}

/** Canonical topic names (the gateway parses the venue first and the timeframe last). */
export const topics = {
  candles: (venue: string, symbol: string, timeframe: string): string =>
    `candles:${venue}:${symbol}:${timeframe}`,
  book: (venue: string, symbol: string): string => `book:${venue}:${symbol}`,
  session: (sessionId: string): string => `session:${sessionId}`,
  health: (venue: string): string => `health:${venue}`,
} as const;
