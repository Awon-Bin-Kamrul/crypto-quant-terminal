/**
 * Runtime configuration. `NEXT_PUBLIC_*` variables are inlined at build time; everything else
 * the terminal needs comes from the gateway.
 */

export type TransportPreference = "auto" | "ws" | "sse";

const DEFAULT_API_URL = "http://127.0.0.1:8000";

export function apiBaseUrl(): string {
  const configured = process.env.NEXT_PUBLIC_CQ_API_URL?.trim();
  return (configured || DEFAULT_API_URL).replace(/\/+$/, "");
}

export function transportPreference(): TransportPreference {
  const configured = process.env.NEXT_PUBLIC_CQ_STREAM_TRANSPORT?.trim().toLowerCase();
  return configured === "ws" || configured === "sse" ? configured : "auto";
}

/** `http(s)://host` → `ws(s)://host`. */
export function toWebSocketUrl(httpUrl: string): string {
  if (httpUrl.startsWith("https://")) return `wss://${httpUrl.slice("https://".length)}`;
  if (httpUrl.startsWith("http://")) return `ws://${httpUrl.slice("http://".length)}`;
  throw new Error(`not an http(s) URL: ${httpUrl}`);
}
