/**
 * Candle lists as the chart needs them: sorted by open time, one candle per time. Live
 * updates replace the newest candle or append the next one; history pages are merged in front.
 */

import type { Candle } from "@/lib/api/types";

/** Candle times for the chart library, which counts in seconds. */
export function chartTime(ms: number): number {
  return Math.floor(ms / 1000);
}

/** Apply one live candle. Returns a new list (the input is not modified), or the same list
 *  when nothing changed. A candle older than the newest one replaces its match, if any. */
export function mergeCandle(candles: readonly Candle[], candle: Candle): readonly Candle[] {
  const last = candles.at(-1);
  if (last === undefined || candle.t > last.t) return [...candles, candle];
  if (candle.t === last.t) {
    if (sameCandle(last, candle)) return candles;
    return [...candles.slice(0, -1), candle];
  }
  const index = findIndex(candles, candle.t);
  if (index < 0 || sameCandle(candles[index]!, candle)) return candles;
  const next = candles.slice();
  next[index] = candle;
  return next;
}

/** Merge an older page (from REST) in front of the live list; live values win on overlap. */
export function mergeHistory(candles: readonly Candle[], page: readonly Candle[]): readonly Candle[] {
  if (page.length === 0) return candles;
  const byTime = new Map<number, Candle>();
  for (const candle of page) byTime.set(candle.t, candle);
  for (const candle of candles) byTime.set(candle.t, candle);
  return [...byTime.values()].sort((a, b) => a.t - b.t);
}

/** Change of the newest close against the previous candle's close. */
/**
 * A fresh snapshot of a series the client already holds. The snapshot covers the recent
 * window only, so history the operator scrolled back to is kept in front of it — provided the
 * two overlap: a series last seen long ago would leave a hole between them, and is dropped.
 */
export function withSnapshot(previous: readonly Candle[], snapshot: readonly Candle[]): readonly Candle[] {
  const first = snapshot[0]?.t;
  const lastSeen = previous.at(-1)?.t;
  if (first === undefined || lastSeen === undefined || lastSeen < first) return snapshot;
  const older = previous.filter((candle) => candle.t < first);
  return older.length > 0 ? [...older, ...snapshot] : snapshot;
}

export function lastChange(candles: readonly Candle[]): { price: number; change: number; pct: number } | null {
  const last = candles.at(-1);
  if (last === undefined) return null;
  const previous = candles.length > 1 ? candles[candles.length - 2]!.c : last.o;
  const change = last.c - previous;
  return { price: last.c, change, pct: previous === 0 ? 0 : change / previous };
}

function sameCandle(a: Candle, b: Candle): boolean {
  return a.t === b.t && a.o === b.o && a.h === b.h && a.l === b.l && a.c === b.c && a.v === b.v;
}

function findIndex(candles: readonly Candle[], time: number): number {
  let low = 0;
  let high = candles.length - 1;
  while (low <= high) {
    const middle = (low + high) >> 1;
    const value = candles[middle]!.t;
    if (value === time) return middle;
    if (value < time) low = middle + 1;
    else high = middle - 1;
  }
  return -1;
}
