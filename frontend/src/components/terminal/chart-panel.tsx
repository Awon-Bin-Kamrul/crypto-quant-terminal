"use client";

import * as React from "react";
import { AlertTriangle, Loader2 } from "lucide-react";
import { useShallow } from "zustand/react/shallow";

import { ChartLegend } from "@/components/terminal/chart-legend";
import { PriceChart } from "@/components/terminal/price-chart";
import { useServices, useTopic } from "@/components/terminal/services";
import { Badge } from "@/components/ui/badge";
import type { Candle } from "@/lib/api/types";
import { formatPct, formatPrice } from "@/lib/format";
import { lastChange } from "@/lib/market/candles";
import { buildMarkers } from "@/lib/market/markers";
import { topics } from "@/lib/stream/protocol";
import { num } from "@/lib/numbers";
import { workingOrders, type SessionView } from "@/lib/session/reducer";
import { cn } from "@/lib/utils";
import { candlesHandler } from "@/stores/bindings";
import { seriesOf, useMarketStore } from "@/stores/market";

const HISTORY_PAGE = 500;

export function ChartPanel({
  venue,
  symbol,
  timeframe,
  session,
}: {
  venue: string;
  symbol: string;
  timeframe: string;
  session: SessionView | null;
}) {
  const { api } = useServices();
  const topic = topics.candles(venue, symbol, timeframe);
  const handler = React.useMemo(() => candlesHandler(topic), [topic]);
  useTopic(topic, handler);

  const status = useMarketStore((state) => seriesOf(state, topic).status);
  const error = useMarketStore((state) => seriesOf(state, topic).error);
  const count = useMarketStore((state) => seriesOf(state, topic).candles.length);
  const change = useMarketStore(
    useShallow((state) => seriesOf(state, topic).candles.slice(-2)),
  );
  const [hovered, setHovered] = React.useState<Candle | null>(null);
  const frame = React.useRef<number | null>(null);
  /** The topic whose older page is being fetched (one request per chart at a time). */
  const loadingHistory = React.useRef<string | null>(null);

  const onHover = React.useCallback((candle: Candle | null) => {
    if (frame.current !== null) cancelAnimationFrame(frame.current);
    frame.current = requestAnimationFrame(() => setHovered(candle));
  }, []);

  const onNeedHistory = React.useCallback(
    (oldest: number) => {
      if (loadingHistory.current === topic) return;
      loadingHistory.current = topic;
      api
        .candles({ venue, symbol, timeframe, end: oldest, limit: HISTORY_PAGE })
        .then((page) => useMarketStore.getState().prependHistory(topic, page.candles))
        .catch((reason: unknown) => {
          console.warn("older candles could not be loaded", reason);
        })
        .finally(() => {
          if (loadingHistory.current === topic) loadingHistory.current = null;
        });
    },
    [api, venue, symbol, timeframe, topic],
  );

  const inSession = session !== null && session.info.symbols.includes(symbol);
  const fills = inSession ? session.fills : EMPTY_FILLS;
  const orders = React.useMemo(
    () => (inSession && session ? workingOrders(session, symbol) : []),
    [inSession, session, symbol],
  );
  const rawPosition = inSession ? session.positions[symbol] : undefined;
  const position = React.useMemo(
    () =>
      rawPosition === undefined
        ? null
        : { quantity: num(rawPosition.quantity), averagePrice: num(rawPosition.average_price) },
    [rawPosition],
  );

  const summary = lastChange(change);
  const shown = hovered ?? change.at(-1) ?? null;
  const shownTime = shown?.t ?? null;
  const executions = React.useMemo(
    () =>
      shownTime === null
        ? []
        : buildMarkers(fills, { symbol, timeframe, range: { from: shownTime, to: shownTime } }),
    [fills, symbol, timeframe, shownTime],
  );

  return (
    <section className="relative flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border bg-card">
      <header className="flex flex-wrap items-center gap-x-4 gap-y-1 border-b px-3 py-1.5">
        <div className="flex items-baseline gap-2">
          <span className="text-sm font-semibold tracking-tight">{symbol}</span>
          <span className="text-[11px] text-muted-foreground uppercase">
            {venue} · {timeframe}
          </span>
        </div>
        {summary && (
          <div className="flex items-baseline gap-2">
            <span
              className={cn(
                "figure text-sm font-semibold",
                summary.change >= 0 ? "text-buy" : "text-sell",
              )}
            >
              {formatPrice(summary.price)}
            </span>
            <span
              className={cn("figure text-[11px]", summary.change >= 0 ? "text-buy" : "text-sell")}
            >
              {formatPct(summary.pct, { signed: true })}
            </span>
          </div>
        )}
        <ChartLegend candle={shown} executions={executions} className="ml-auto" />
        {inSession && fills.some((fill) => fill.symbol === symbol) && (
          <Badge variant="muted" className="hidden md:inline-flex">
            ▲▼ executions
          </Badge>
        )}
      </header>
      <div className="relative min-h-0 flex-1">
        <PriceChart
          topic={topic}
          symbol={symbol}
          timeframe={timeframe}
          fills={fills}
          orders={orders}
          position={position}
          onHover={onHover}
          onNeedHistory={onNeedHistory}
        />
        {status === "loading" && (
          <Overlay>
            <Loader2 className="size-4 animate-spin" /> Loading candles…
          </Overlay>
        )}
        {status === "error" && (
          <Overlay tone="error">
            <AlertTriangle className="size-4" /> {error ?? "The candle stream failed."}
          </Overlay>
        )}
        {status === "ready" && count === 0 && (
          <Overlay>No candles stored for {symbol} yet — is the ingestor running?</Overlay>
        )}
      </div>
    </section>
  );
}

const EMPTY_FILLS: never[] = [];

function Overlay({ children, tone }: { children: React.ReactNode; tone?: "error" }) {
  return (
    <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
      <div
        className={cn(
          "flex items-center gap-2 rounded-md border bg-popover/90 px-3 py-2 text-xs text-muted-foreground shadow-sm",
          tone === "error" && "border-sell/40 text-sell",
        )}
      >
        {children}
      </div>
    </div>
  );
}
