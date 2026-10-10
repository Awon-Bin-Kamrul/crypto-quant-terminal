"use client";

import * as React from "react";
import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  LineStyle,
  createChart,
  createSeriesMarkers,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  type LogicalRange,
  type MouseEventParams,
  type SeriesMarker,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";

import type { Candle, Fill, Order } from "@/lib/api/types";
import { formatPrice, formatQty, priceDecimals } from "@/lib/format";
import { chartTime } from "@/lib/market/candles";
import { buildMarkers, labelledMarkers } from "@/lib/market/markers";
import { timeframeMs } from "@/lib/market/timeframes";
import { num } from "@/lib/numbers";
import { chartFont, palette } from "@/lib/theme";
import { seriesOf, useMarketStore, type CandleSeriesState } from "@/stores/market";

export interface PriceChartProps {
  topic: string;
  symbol: string;
  timeframe: string;
  /** Session fills (any symbol): shown as buy/sell markers on the candles they happened in. */
  fills: readonly Fill[];
  /** Working orders of this symbol: limit prices drawn as dashed lines. */
  orders: readonly Order[];
  /** Open position: its average entry price is drawn as a dotted line. */
  position: { quantity: number; averagePrice: number } | null;
  onHover(candle: Candle | null): void;
  onNeedHistory(oldest: number): void;
}

const VISIBLE_BARS = 150;
const HISTORY_TRIGGER_BARS = 20;

function toBar(candle: Candle): CandlestickData<Time> {
  return {
    time: chartTime(candle.t) as UTCTimestamp,
    open: candle.o,
    high: candle.h,
    low: candle.l,
    close: candle.c,
  };
}

function toVolume(candle: Candle): HistogramData<Time> {
  return {
    time: chartTime(candle.t) as UTCTimestamp,
    value: candle.v,
    color: candle.c >= candle.o ? palette.buySoft : palette.sellSoft,
  };
}

/** Crosshair data is a union over every series type; these narrow it to what we plotted. */
function isOhlc(data: unknown): data is { open: number; high: number; low: number; close: number } {
  if (typeof data !== "object" || data === null) return false;
  const bar = data as Record<string, unknown>;
  return (
    typeof bar.open === "number" &&
    typeof bar.high === "number" &&
    typeof bar.low === "number" &&
    typeof bar.close === "number"
  );
}

function valueOf(data: unknown): number {
  if (typeof data !== "object" || data === null) return 0;
  const value = (data as Record<string, unknown>).value;
  return typeof value === "number" ? value : 0;
}

function precisionFor(candles: readonly Candle[]): { precision: number; minMove: number } {
  const reference = candles.at(-1)?.c ?? 1;
  const precision = priceDecimals(reference);
  return { precision, minMove: 10 ** -precision };
}

/**
 * Candlesticks with volume, the session's executions as markers, working orders and the
 * position's entry as price lines. Live candles are applied straight to the chart from the
 * store (no React render per tick); scrolling to the left edge asks for older history.
 */
export function PriceChart({
  topic,
  symbol,
  timeframe,
  fills,
  orders,
  position,
  onHover,
  onNeedHistory,
}: PriceChartProps) {
  const containerRef = React.useRef<HTMLDivElement>(null);
  const chartRef = React.useRef<IChartApi | null>(null);
  const candlesRef = React.useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = React.useRef<ISeriesApi<"Histogram"> | null>(null);
  const markersRef = React.useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const orderLinesRef = React.useRef(new Map<string, IPriceLine>());
  const entryLineRef = React.useRef<IPriceLine | null>(null);
  const onHoverRef = React.useRef(onHover);
  const onNeedHistoryRef = React.useRef(onNeedHistory);
  const [range, setRange] = React.useState<{ from: number; to: number } | null>(null);
  /** Pixels per candle, rounded so that scrolling does not re-render on every frame. */
  const [barSpacing, setBarSpacing] = React.useState(0);

  onHoverRef.current = onHover;
  onNeedHistoryRef.current = onNeedHistory;

  // ------------------------------------------------------------------ chart lifecycle
  React.useEffect(() => {
    const container = containerRef.current;
    if (container === null) return undefined;
    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: palette.panel },
        textColor: palette.mutedText,
        fontFamily: chartFont,
        fontSize: 11,
        attributionLogo: true,
      },
      grid: {
        vertLines: { color: palette.grid },
        horzLines: { color: palette.grid },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: palette.crosshair, labelBackgroundColor: palette.popover },
        horzLine: { color: palette.crosshair, labelBackgroundColor: palette.popover },
      },
      rightPriceScale: { borderColor: palette.border, scaleMargins: { top: 0.08, bottom: 0.24 } },
      timeScale: {
        borderColor: palette.border,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 6,
        barSpacing: 7,
      },
      localization: { priceFormatter: (price: number) => formatPrice(price) },
    });
    const candles = chart.addSeries(CandlestickSeries, {
      upColor: palette.buy,
      downColor: palette.sell,
      borderUpColor: palette.buy,
      borderDownColor: palette.sell,
      wickUpColor: palette.buy,
      wickDownColor: palette.sell,
      priceLineColor: palette.mutedText,
    });
    const volume = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      priceFormat: { type: "volume" },
      lastValueVisible: false,
      priceLineVisible: false,
    });
    chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    const markers = createSeriesMarkers(candles, []);

    const onCrosshair = (param: MouseEventParams<Time>): void => {
      const bar = param.time === undefined ? undefined : param.seriesData.get(candles);
      if (bar === undefined || typeof param.time !== "number" || !isOhlc(bar)) {
        onHoverRef.current(null);
        return;
      }
      onHoverRef.current({
        t: param.time * 1000,
        o: bar.open,
        h: bar.high,
        l: bar.low,
        c: bar.close,
        v: valueOf(param.seriesData.get(volume)),
      });
    };
    chart.subscribeCrosshairMove(onCrosshair);

    chartRef.current = chart;
    candlesRef.current = candles;
    volumeRef.current = volume;
    markersRef.current = markers;
    const orderLines = orderLinesRef.current;
    return () => {
      chart.unsubscribeCrosshairMove(onCrosshair);
      orderLines.clear();
      entryLineRef.current = null;
      markersRef.current = null;
      candlesRef.current = null;
      volumeRef.current = null;
      chartRef.current = null;
      chart.remove();
    };
  }, []);

  // --------------------------------------------------------------------- live data
  React.useEffect(() => {
    const chart = chartRef.current;
    const candles = candlesRef.current;
    const volume = volumeRef.current;
    if (chart === null || candles === null || volume === null) return undefined;
    let version = -1;
    let first: number | null = null;
    let last: Candle | null = null;
    candles.setData([]);
    volume.setData([]);
    setRange(null);

    const apply = (state: CandleSeriesState): void => {
      if (state.status !== "ready") return;
      const newest = state.candles.at(-1) ?? null;
      if (state.version !== version) {
        const visible = chart.timeScale().getVisibleLogicalRange();
        const oldFirst = first;
        candles.applyOptions({ priceFormat: { type: "price", ...precisionFor(state.candles) } });
        candles.setData(state.candles.map(toBar));
        volume.setData(state.candles.map(toVolume));
        const initial = version === -1 || oldFirst === null;
        version = state.version;
        first = state.candles[0]?.t ?? null;
        last = newest;
        if (initial) {
          const count = state.candles.length;
          if (count > VISIBLE_BARS) {
            chart.timeScale().setVisibleLogicalRange({ from: count - VISIBLE_BARS, to: count + 5 });
          } else {
            chart.timeScale().fitContent();
          }
        } else if (visible !== null && first !== null && first < oldFirst) {
          // older history was merged in front: keep looking at the same candles
          const added = state.candles.findIndex((candle) => candle.t >= oldFirst);
          if (added > 0) {
            chart.timeScale().setVisibleLogicalRange({
              from: visible.from + added,
              to: visible.to + added,
            });
          }
        }
      } else if (newest !== null && newest !== last) {
        candles.update(toBar(newest));
        volume.update(toVolume(newest));
        last = newest;
      }
      const from = state.candles[0]?.t;
      const to = newest?.t;
      setRange((current) =>
        from === undefined || to === undefined
          ? null
          : current?.from === from && current.to === to
            ? current
            : { from, to },
      );
    };

    apply(seriesOf(useMarketStore.getState(), topic));
    const unsubscribe = useMarketStore.subscribe((state) => seriesOf(state, topic), apply);

    const onRange = (logical: LogicalRange | null): void => {
      if (logical !== null && logical.to > logical.from) {
        const spacing = chart.timeScale().width() / (logical.to - logical.from);
        setBarSpacing(Math.round(spacing * 2) / 2);
      }
      if (logical === null || first === null) return;
      const state = seriesOf(useMarketStore.getState(), topic);
      if (!state.complete && logical.from < HISTORY_TRIGGER_BARS) onNeedHistoryRef.current(first);
    };
    chart.timeScale().subscribeVisibleLogicalRangeChange(onRange);
    return () => {
      unsubscribe();
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(onRange);
    };
  }, [topic]);

  // ----------------------------------------------------------------------- markers
  React.useEffect(() => {
    const markers = markersRef.current;
    if (markers === null) return;
    const built = buildMarkers(fills, { symbol, timeframe, range });
    const labelled = labelledMarkers(built, barSpacing, timeframeMs(timeframe));
    markers.setMarkers(
      built.map(
        (marker): SeriesMarker<Time> => ({
          id: marker.id,
          time: marker.time as UTCTimestamp,
          position: marker.position,
          shape: marker.shape,
          color: marker.side === "buy" ? palette.buy : palette.sell,
          text: labelled.has(marker.id) ? marker.label : "",
          size: 1,
        }),
      ),
    );
  }, [fills, symbol, timeframe, range, barSpacing]);

  // ------------------------------------------------------------------- price lines
  React.useEffect(() => {
    const candles = candlesRef.current;
    if (candles === null) return;
    const lines = orderLinesRef.current;
    const wanted = new Map<string, Order>();
    for (const order of orders) {
      if (order.symbol === symbol && order.limit_price !== null) wanted.set(order.client_order_id, order);
    }
    for (const [id, line] of lines) {
      if (!wanted.has(id)) {
        candles.removePriceLine(line);
        lines.delete(id);
      }
    }
    for (const [id, order] of wanted) {
      const remaining = num(order.quantity) - num(order.filled);
      const options = {
        price: num(order.limit_price),
        color: order.side === "buy" ? palette.buy : palette.sell,
        lineWidth: 1 as const,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: `${order.side === "buy" ? "BUY" : "SELL"} ${formatQty(remaining)}`,
      };
      const existing = lines.get(id);
      if (existing) existing.applyOptions(options);
      else lines.set(id, candles.createPriceLine(options));
    }
  }, [orders, symbol]);

  React.useEffect(() => {
    const candles = candlesRef.current;
    if (candles === null) return;
    const open = position !== null && position.quantity !== 0 && position.averagePrice > 0;
    if (!open) {
      if (entryLineRef.current) candles.removePriceLine(entryLineRef.current);
      entryLineRef.current = null;
      return;
    }
    const options = {
      price: position.averagePrice,
      color: palette.warning,
      lineWidth: 1 as const,
      lineStyle: LineStyle.Dotted,
      axisLabelVisible: true,
      title: `${position.quantity > 0 ? "LONG" : "SHORT"} ${formatQty(Math.abs(position.quantity))}`,
    };
    if (entryLineRef.current) entryLineRef.current.applyOptions(options);
    else entryLineRef.current = candles.createPriceLine(options);
  }, [position]);

  return <div ref={containerRef} className="size-full min-h-0" data-testid="price-chart" />;
}
