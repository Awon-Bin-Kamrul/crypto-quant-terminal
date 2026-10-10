"use client";

import type { Candle } from "@/lib/api/types";
import { formatCompact, formatPct, formatPrice } from "@/lib/format";
import type { TradeMarker } from "@/lib/market/markers";
import { cn } from "@/lib/utils";

/**
 * OHLCV of the hovered candle (or the newest one), with its change from open and the
 * session's executions in it (count, quantity and average price per side).
 */
export function ChartLegend({
  candle,
  executions = [],
  className,
}: {
  candle: Candle | null;
  executions?: readonly TradeMarker[];
  className?: string;
}) {
  if (candle === null) return null;
  const change = candle.o === 0 ? 0 : (candle.c - candle.o) / candle.o;
  const tone = candle.c >= candle.o ? "text-buy" : "text-sell";
  const item = (label: string, value: string, toneClass?: string) => (
    <span className="flex items-baseline gap-1">
      <span className="text-muted-foreground">{label}</span>
      <span className={cn("figure", toneClass)}>{value}</span>
    </span>
  );
  return (
    <div
      className={cn(
        "pointer-events-none flex flex-wrap items-center gap-x-3 gap-y-0.5 text-[11px]",
        className,
      )}
    >
      {item("O", formatPrice(candle.o), tone)}
      {item("H", formatPrice(candle.h), tone)}
      {item("L", formatPrice(candle.l), tone)}
      {item("C", formatPrice(candle.c), tone)}
      {item("Δ", formatPct(change, { signed: true }), tone)}
      {item("V", formatCompact(candle.v))}
      {executions.map((execution) => (
        <span
          key={execution.id}
          className={cn("figure", execution.side === "buy" ? "text-buy" : "text-sell")}
          data-testid="legend-execution"
        >
          {execution.side === "buy" ? "▲" : "▼"} {execution.text}
        </span>
      ))}
    </div>
  );
}
