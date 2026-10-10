/**
 * What the terminal actually shows, given what the operator selected and what the gateway
 * offers. A stored or linked selection can be out of date (a market no longer configured, a
 * session on another venue), so the selection is resolved against the catalog rather than
 * trusted as is.
 */

import type { Market, SessionSummary } from "@/lib/api/types";

export interface SessionScope {
  venue: string;
  symbols: readonly string[];
}

export interface ResolvedMarket {
  venue: string | null;
  symbol: string | null;
  /** Markets offered in the picker: the session's instruments first, then the venue's. */
  symbols: string[];
}

/**
 * The venue and symbol to chart. A selected session fixes the venue; without one, the stored
 * venue is kept while the gateway still serves it. The symbol is kept when it is on offer and
 * replaced by the first one on offer otherwise. Before the catalog has loaded (no markets),
 * the stored selection is used as is, so the chart can start streaming straight away.
 */
export function resolveMarket(input: {
  venue: string | null;
  symbol: string | null;
  markets: readonly Market[];
  session: SessionScope | null;
}): ResolvedMarket {
  const venues = new Set(input.markets.map((market) => market.venue));
  const venue =
    input.session?.venue ??
    (input.venue !== null && (venues.size === 0 || venues.has(input.venue))
      ? input.venue
      : (input.markets[0]?.venue ?? input.venue));

  const symbols: string[] = [];
  const offer = (symbol: string): void => {
    if (!symbols.includes(symbol)) symbols.push(symbol);
  };
  for (const symbol of input.session?.symbols ?? []) offer(symbol);
  for (const market of input.markets) {
    if (market.venue === venue) offer(market.symbol);
  }

  if (symbols.length === 0) {
    if (input.symbol !== null) symbols.push(input.symbol);
    return { venue, symbol: input.symbol, symbols };
  }
  const symbol = input.symbol !== null && symbols.includes(input.symbol) ? input.symbol : (symbols[0] ?? null);
  return { venue, symbol, symbols };
}

/** The session to open for an operator who has never chosen one: a running one if any. */
export function defaultSession(sessions: readonly SessionSummary[]): string | null {
  const running = sessions.find((session) => session.status === "running");
  return (running ?? sessions[0])?.id ?? null;
}
