"use client";

import * as React from "react";
import {
  BaselineSeries,
  ColorType,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";

import type { EquityPoint } from "@/lib/api/types";
import { formatMoney } from "@/lib/format";
import { equityLine, type EquityLinePoint } from "@/lib/session/metrics";
import { chartFont, palette } from "@/lib/theme";

/**
 * The session's equity curve (one point per base bar, as the engine reports it), green above
 * the starting capital and red below.
 */
export function EquityChart({
  points,
  baseline,
}: {
  points: readonly EquityPoint[];
  baseline: number;
}) {
  const containerRef = React.useRef<HTMLDivElement>(null);
  const chartRef = React.useRef<IChartApi | null>(null);
  const seriesRef = React.useRef<ISeriesApi<"Baseline"> | null>(null);
  const shownRef = React.useRef<{ count: number; last: number | null }>({ count: 0, last: null });
  const line = React.useMemo(() => equityLine(points), [points]);

  React.useEffect(() => {
    const container = containerRef.current;
    if (container === null) return undefined;
    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: palette.panel },
        textColor: palette.mutedText,
        fontFamily: chartFont,
        fontSize: 10,
        attributionLogo: false,
      },
      grid: { vertLines: { visible: false }, horzLines: { color: palette.grid } },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.15, bottom: 0.1 } },
      timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false },
      handleScale: false,
      handleScroll: false,
      crosshair: {
        vertLine: { color: palette.crosshair, labelBackgroundColor: palette.popover },
        horzLine: { color: palette.crosshair, labelBackgroundColor: palette.popover },
      },
      localization: { priceFormatter: (value: number) => formatMoney(value) },
    });
    const series = chart.addSeries(BaselineSeries, {
      topLineColor: palette.buy,
      topFillColor1: "rgba(53, 204, 134, 0.28)",
      topFillColor2: "rgba(53, 204, 134, 0.02)",
      bottomLineColor: palette.sell,
      bottomFillColor1: "rgba(248, 83, 90, 0.02)",
      bottomFillColor2: "rgba(248, 83, 90, 0.28)",
      lineWidth: 2,
      priceLineVisible: false,
    });
    chartRef.current = chart;
    seriesRef.current = series;
    // a new chart is empty, whatever an earlier one showed (effects re-run in Strict Mode)
    shownRef.current = { count: 0, last: null };
    return () => {
      chartRef.current = null;
      seriesRef.current = null;
      chart.remove();
    };
  }, []);

  React.useEffect(() => {
    seriesRef.current?.applyOptions({ baseValue: { type: "price", price: baseline } });
  }, [baseline]);

  React.useEffect(() => {
    const series = seriesRef.current;
    const chart = chartRef.current;
    if (series === null || chart === null) return;
    const shown = shownRef.current;
    const last = line.at(-1);
    const data = (point: EquityLinePoint) => ({ time: point.time as UTCTimestamp as Time, value: point.value });
    // The same curve grown by one point, or its last second revised: update in place.
    // Anything else (first load, a new session, a trimmed history): re-set.
    const appended = line.length === shown.count + 1 && shown.last !== null && last !== undefined && last.time > shown.last;
    const revised = line.length === shown.count && shown.last !== null && last?.time === shown.last;
    if (last !== undefined && (appended || revised)) series.update(data(last));
    else series.setData(line.map(data));
    // the curve cannot be scrolled or zoomed: it always shows the whole session
    chart.timeScale().fitContent();
    shownRef.current = { count: line.length, last: last?.time ?? null };
  }, [line]);

  return <div ref={containerRef} className="size-full min-h-0" data-testid="equity-chart" />;
}
