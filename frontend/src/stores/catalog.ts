/** Markets and sessions the gateway knows, refreshed periodically over REST. */

import { create } from "zustand";

import type { Market, SessionSummary } from "@/lib/api/types";

interface CatalogState {
  markets: Market[];
  timeframes: string[];
  sessions: SessionSummary[];
  marketsLoaded: boolean;
  sessionsLoaded: boolean;
  /** Why the last refresh failed (cleared by the next successful one). */
  error: string | null;
  setMarkets(markets: Market[], timeframes: string[]): void;
  setSessions(sessions: SessionSummary[]): void;
  setError(error: string | null): void;
}

export const useCatalogStore = create<CatalogState>()((set) => ({
  markets: [],
  timeframes: [],
  sessions: [],
  marketsLoaded: false,
  sessionsLoaded: false,
  error: null,
  setMarkets(markets, timeframes) {
    set({ markets, timeframes, marketsLoaded: true, error: null });
  },
  setSessions(sessions) {
    set({ sessions, sessionsLoaded: true, error: null });
  },
  setError(error) {
    set({ error });
  },
}));
