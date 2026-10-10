/**
 * What the terminal is looking at: session, market, timeframe and blotter tab. Remembered in
 * localStorage and mirrored in the URL (`?session=…&symbol=…&tf=…`), so a view can be
 * bookmarked or shared with a colleague who has the token.
 */

import { create } from "zustand";

import { isTimeframe, type Timeframe } from "@/lib/market/timeframes";
import { readJson, writeItem } from "@/lib/storage";

export type BlotterTab = "positions" | "orders" | "fills" | "history" | "risk";

const KEY = "cq.selection";

export interface Selection {
  sessionId: string | null;
  venue: string | null;
  symbol: string | null;
  timeframe: Timeframe;
  tab: BlotterTab;
  /** The operator picked a session (or "market data only") at some point on this device. */
  sessionChosen: boolean;
}

interface SelectionState extends Selection {
  hydrated: boolean;
  /** No session was ever chosen or linked: the terminal may pick one for the operator. */
  fresh: boolean;
  hydrate(): void;
  selectSession(sessionId: string | null): void;
  selectMarket(venue: string, symbol: string): void;
  selectTimeframe(timeframe: Timeframe): void;
  selectTab(tab: BlotterTab): void;
}

const DEFAULTS: Selection = {
  sessionId: null,
  venue: null,
  symbol: null,
  timeframe: "1h",
  tab: "positions",
  sessionChosen: false,
};

function fromUrl(): Partial<Selection> {
  if (typeof window === "undefined") return {};
  const params = new URLSearchParams(window.location.search);
  const selection: Partial<Selection> = {};
  const session = params.get("session");
  if (session) selection.sessionId = session;
  const venue = params.get("venue");
  if (venue) selection.venue = venue;
  const symbol = params.get("symbol");
  if (symbol) selection.symbol = symbol;
  const tf = params.get("tf");
  if (tf && isTimeframe(tf)) selection.timeframe = tf;
  return selection;
}

const TABS: readonly BlotterTab[] = ["positions", "orders", "fills", "history", "risk"];

/** What survives of a stored selection (another version, or edited by hand, may be malformed). */
function sanitize(stored: unknown): Partial<Selection> {
  if (typeof stored !== "object" || stored === null) return {};
  const record = stored as Record<string, unknown>;
  const text = (key: string): string | null | undefined => {
    const value = record[key];
    if (value === null) return null;
    return typeof value === "string" && value.length > 0 ? value : undefined;
  };
  const selection: Partial<Selection> = {};
  const sessionId = text("sessionId");
  if (sessionId !== undefined) selection.sessionId = sessionId;
  const venue = text("venue");
  if (venue !== undefined) selection.venue = venue;
  const symbol = text("symbol");
  if (symbol !== undefined) selection.symbol = symbol;
  const timeframe = text("timeframe");
  if (timeframe && isTimeframe(timeframe)) selection.timeframe = timeframe;
  const tab = text("tab");
  if (tab && (TABS as readonly string[]).includes(tab)) selection.tab = tab as BlotterTab;
  if (typeof record.sessionChosen === "boolean") selection.sessionChosen = record.sessionChosen;
  return selection;
}

function persist(selection: Selection): void {
  writeItem("local", KEY, JSON.stringify(selection));
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  const set = (name: string, value: string | null): void => {
    if (value) url.searchParams.set(name, value);
    else url.searchParams.delete(name);
  };
  set("session", selection.sessionId);
  set("venue", selection.venue);
  set("symbol", selection.symbol);
  set("tf", selection.timeframe);
  if (url.href !== window.location.href) window.history.replaceState(window.history.state, "", url);
}

function current(state: SelectionState): Selection {
  return {
    sessionId: state.sessionId,
    venue: state.venue,
    symbol: state.symbol,
    timeframe: state.timeframe,
    tab: state.tab,
    sessionChosen: state.sessionChosen,
  };
}

export const useSelectionStore = create<SelectionState>()((set, get) => {
  const update = (changes: Partial<Selection>): void => {
    set(changes);
    persist(current(get()));
  };
  return {
    ...DEFAULTS,
    hydrated: false,
    fresh: false,
    hydrate() {
      const selection: Selection = { ...DEFAULTS, ...sanitize(readJson<unknown>("local", KEY)), ...fromUrl() };
      const fresh = !selection.sessionChosen && selection.sessionId === null;
      set({ ...selection, hydrated: true, fresh });
      persist(current(get()));
    },
    selectSession(sessionId) {
      update({ sessionId, sessionChosen: true });
      set({ fresh: false });
    },
    selectMarket(venue, symbol) {
      update({ venue, symbol });
    },
    selectTimeframe(timeframe) {
      update({ timeframe });
    },
    selectTab(tab) {
      update({ tab });
    },
  };
});
