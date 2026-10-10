/**
 * Trade markers for the price chart: the session's fills, grouped per candle and side, shown
 * as arrows below (buys) or above (sells) the candle they happened in.
 *
 * The chart can only anchor a marker to a candle time, so each fill is placed on the candle
 * containing it; several fills of one side in one candle become one marker labelled with the
 * count and total quantity. The volume-weighted average price is in `text`, which the chart
 * legend shows for the hovered candle (on the chart itself it would run off the last candles).
 */

import type { Fill } from "@/lib/api/types";
import { formatPrice, formatQty } from "@/lib/format";
import { chartTime } from "@/lib/market/candles";
import { bucketStart } from "@/lib/market/timeframes";
import { num } from "@/lib/numbers";

export interface TradeMarker {
  id: string;
  /** Candle open time, in seconds (chart time). */
  time: number;
  position: "belowBar" | "aboveBar";
  shape: "arrowUp" | "arrowDown";
  side: "buy" | "sell";
  /** On-chart label, e.g. `B ×2 0.4`. */
  label: string;
  /** Full description, e.g. `B ×2 0.4 @ 103.000`. */
  text: string;
  quantity: number;
  price: number;
  count: number;
}

export interface MarkerOptions {
  symbol: string;
  timeframe: string;
  /** Only candles in this range exist on the chart (open times, ms, inclusive). */
  range: { from: number; to: number } | null;
}

export function buildMarkers(fills: readonly Fill[], options: MarkerOptions): TradeMarker[] {
  if (options.range === null) return [];
  const { from, to } = options.range;
  const groups = new Map<string, { side: "buy" | "sell"; bucket: number; qty: number; notional: number; count: number }>();
  for (const fill of fills) {
    if (fill.symbol !== options.symbol) continue;
    const bucket = bucketStart(fill.ts, options.timeframe);
    if (bucket < from || bucket > to) continue;
    const key = `${bucket}:${fill.side}`;
    const quantity = num(fill.quantity);
    const group = groups.get(key) ?? { side: fill.side, bucket, qty: 0, notional: 0, count: 0 };
    group.qty += quantity;
    group.notional += quantity * num(fill.price);
    group.count += 1;
    groups.set(key, group);
  }
  return [...groups.values()]
    .sort((a, b) => a.bucket - b.bucket || (a.side === "buy" ? -1 : 1))
    .map((group) => {
      const price = group.qty > 0 ? group.notional / group.qty : 0;
      const letter = group.side === "buy" ? "B" : "S";
      const count = group.count > 1 ? ` ×${group.count}` : "";
      return {
        id: `${group.bucket}:${group.side}`,
        time: chartTime(group.bucket),
        position: group.side === "buy" ? "belowBar" : "aboveBar",
        shape: group.side === "buy" ? "arrowUp" : "arrowDown",
        side: group.side,
        label: `${letter}${count} ${formatQty(group.qty)}`,
        text: `${letter}${count} ${formatQty(group.qty)} @ ${formatPrice(price)}`,
        quantity: group.qty,
        price,
        count: group.count,
      } satisfies TradeMarker;
    });
}

/** Approximate width of one label character on the chart (10–11 px monospace). */
const LABEL_CHAR_PX = 6.5;
const LABEL_GAP_PX = 6;

/**
 * Which markers keep their on-chart label at the current zoom. Labels on the same side (above
 * or below the candles) that would overprint each other are dropped, newest first kept — the
 * arrows always stay, and the legend shows the details of the hovered candle.
 *
 * @param barSpacing pixels per candle at the current zoom
 * @param timeframeMs candle length; markers are assumed to sit on consecutive candles
 */
export function labelledMarkers(
  markers: readonly TradeMarker[],
  barSpacing: number,
  timeframeMs: number,
): Set<string> {
  const kept = new Set<string>();
  if (!(barSpacing > 0) || !(timeframeMs > 0)) return kept;
  for (const position of ["aboveBar", "belowBar"] as const) {
    let leftEdge = Number.POSITIVE_INFINITY;
    const side = markers.filter((marker) => marker.position === position).sort((a, b) => b.time - a.time);
    for (const marker of side) {
      const x = ((marker.time * 1000) / timeframeMs) * barSpacing;
      const half = (marker.label.length * LABEL_CHAR_PX) / 2;
      if (x + half + LABEL_GAP_PX <= leftEdge) {
        kept.add(marker.id);
        leftEdge = x - half;
      }
    }
  }
  return kept;
}
