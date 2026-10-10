/**
 * Live market data per stream topic. Candle series carry a `version` that changes whenever the
 * whole series is replaced (a snapshot, an older page merged in): the chart re-sets its data
 * then, and otherwise applies only the newest candle — high-frequency updates never re-render
 * React components.
 */

import { create } from "zustand";
import { subscribeWithSelector } from "zustand/middleware";

import type { BookPayload, Candle, FeedHealth } from "@/lib/api/types";
import { mergeCandle, mergeHistory, withSnapshot } from "@/lib/market/candles";

export interface CandleSeriesState {
  candles: readonly Candle[];
  version: number;
  status: "loading" | "ready" | "error";
  error: string | null;
  /** No older history exists (a history page came back empty). */
  complete: boolean;
}

interface MarketState {
  series: Record<string, CandleSeriesState>;
  books: Record<string, BookPayload>;
  health: Record<string, FeedHealth>;
  topicErrors: Record<string, string>;
  setCandles(topic: string, candles: readonly Candle[]): void;
  applyCandle(topic: string, candle: Candle): void;
  prependHistory(topic: string, page: readonly Candle[]): void;
  setBook(topic: string, book: BookPayload): void;
  setHealth(venue: string, health: FeedHealth): void;
  setTopicError(topic: string, message: string): void;
}

const EMPTY: CandleSeriesState = {
  candles: [],
  version: 0,
  status: "loading",
  error: null,
  complete: false,
};

export function seriesOf(state: Pick<MarketState, "series">, topic: string | null): CandleSeriesState {
  return (topic !== null ? state.series[topic] : undefined) ?? EMPTY;
}

export const useMarketStore = create<MarketState>()(
  subscribeWithSelector((set) => ({
    series: {},
    books: {},
    health: {},
    topicErrors: {},
    setCandles(topic, candles) {
      set((state) => {
        const previous = state.series[topic] ?? EMPTY;
        const { [topic]: _cleared, ...topicErrors } = state.topicErrors;
        // after a reconnect, a sequence gap or a corrected candle: keep scrolled-back history
        const merged = withSnapshot(previous.candles, candles);
        return {
          topicErrors,
          series: {
            ...state.series,
            [topic]: {
              candles: merged,
              version: previous.version + 1,
              status: "ready",
              error: null,
              complete: merged.length > candles.length && previous.complete,
            },
          },
        };
      });
    },
    applyCandle(topic, candle) {
      set((state) => {
        const previous = state.series[topic];
        if (previous === undefined || previous.status !== "ready") return state;
        const candles = mergeCandle(previous.candles, candle);
        if (candles === previous.candles) return state;
        const older = candle.t < (previous.candles.at(-1)?.t ?? Number.NEGATIVE_INFINITY);
        return {
          series: {
            ...state.series,
            // a corrected older candle cannot be applied incrementally: re-set the series
            [topic]: { ...previous, candles, version: older ? previous.version + 1 : previous.version },
          },
        };
      });
    },
    prependHistory(topic, page) {
      set((state) => {
        const previous = state.series[topic];
        if (previous === undefined) return state;
        const candles = mergeHistory(previous.candles, page);
        return {
          series: {
            ...state.series,
            [topic]: {
              ...previous,
              candles,
              version: candles === previous.candles ? previous.version : previous.version + 1,
              complete: page.length === 0 || candles.length === previous.candles.length,
            },
          },
        };
      });
    },
    setBook(topic, book) {
      set((state) => ({ books: { ...state.books, [topic]: book } }));
    },
    setHealth(venue, health) {
      set((state) => ({ health: { ...state.health, [venue]: health } }));
    },
    setTopicError(topic, message) {
      set((state) => {
        const series = state.series[topic];
        // a candle topic fails before any snapshot too: the chart shows the error rather than
        // loading forever
        const failed = series ?? (topic.startsWith("candles:") ? EMPTY : undefined);
        return {
          topicErrors: { ...state.topicErrors, [topic]: message },
          series:
            failed === undefined
              ? state.series
              : { ...state.series, [topic]: { ...failed, status: "error", error: message } },
        };
      });
    },
  })),
);
