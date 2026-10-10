"use client";

import * as React from "react";

import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { formatMoney, formatPct, formatTime } from "@/lib/format";
import type { SessionMetrics } from "@/lib/session/metrics";
import { cn } from "@/lib/utils";

function tone(value: number | null | undefined): string {
  if (value === null || value === undefined || Math.abs(value) < 1e-9) return "text-foreground";
  return value > 0 ? "text-buy" : "text-sell";
}

function Tile({
  label,
  value,
  sub,
  valueClass,
  hint,
  children,
}: {
  label: string;
  value: string;
  sub?: string;
  valueClass?: string;
  hint?: string;
  children?: React.ReactNode;
}) {
  const body = (
    <div className="flex min-w-[112px] flex-1 flex-col gap-0.5 border-r px-3 py-1.5 last:border-r-0">
      <span className="text-[10px] font-medium tracking-wide text-muted-foreground uppercase">
        {label}
      </span>
      <span className={cn("figure text-sm leading-tight font-semibold", valueClass)}>{value}</span>
      {sub !== undefined && <span className="figure text-[10px] text-muted-foreground">{sub}</span>}
      {children}
    </div>
  );
  if (!hint) return body;
  return (
    <Tooltip>
      <TooltipTrigger asChild>{body}</TooltipTrigger>
      <TooltipContent>{hint}</TooltipContent>
    </Tooltip>
  );
}

/** A thin bar showing how much of a limit is used (red past 80 %). */
function LimitBar({ used, limit }: { used: number | null; limit: number | null }) {
  if (used === null || limit === null || limit <= 0) return null;
  const share = Math.min(1, used / limit);
  return (
    <div className="mt-0.5 h-1 w-full overflow-hidden rounded-full bg-muted">
      <div
        className={cn(
          "h-full rounded-full",
          share >= 0.8 ? "bg-sell" : share >= 0.5 ? "bg-warning" : "bg-buy",
        )}
        style={{ width: `${Math.max(2, share * 100)}%` }}
      />
    </div>
  );
}

/** Equity, PnL and risk usage of the selected session, marked to the live market. */
export function MetricsStrip({ metrics }: { metrics: SessionMetrics | null }) {
  if (metrics === null) {
    return (
      <div className="flex h-[58px] items-center rounded-md border bg-card px-3 text-xs text-muted-foreground">
        Select a trading session to see its equity, PnL and risk.
      </div>
    );
  }
  const quote = metrics.quote;
  const source =
    metrics.equitySource === "live"
      ? "marked to the live market"
      : metrics.equitySource === "reported"
        ? `reported by the engine at ${formatTime(metrics.reportedAt)} UTC`
        : "no equity reported yet";
  return (
    <div className="flex flex-wrap overflow-hidden rounded-md border bg-card">
      <Tile
        label={`Equity · ${quote}`}
        value={formatMoney(metrics.equity)}
        sub={`start ${formatMoney(metrics.initialCapital)}`}
        hint={`Cash plus open positions, ${source}.`}
      />
      <Tile
        label="Total PnL"
        value={formatMoney(metrics.totalPnl, { signed: true })}
        sub={formatPct(metrics.totalPnlPct, { signed: true })}
        valueClass={tone(metrics.totalPnl)}
      />
      <Tile
        label="Day PnL"
        value={formatMoney(metrics.dayPnl, { signed: true })}
        valueClass={tone(metrics.dayPnl)}
        sub={
          metrics.dailyLossLimit !== null
            ? `limit −${formatPct(metrics.dailyLossLimit, { decimals: 1 })}`
            : "no daily limit"
        }
        hint="Since 00:00 UTC. The daily loss limit halts new risk until the next UTC midnight."
      >
        <LimitBar used={metrics.dayLoss} limit={metrics.dailyLossLimit} />
      </Tile>
      <Tile
        label="Unrealized"
        value={formatMoney(metrics.unrealized, { signed: true })}
        valueClass={tone(metrics.unrealized)}
        sub={`${metrics.openPositions} open`}
      />
      <Tile
        label="Realized"
        value={formatMoney(metrics.realized, { signed: true })}
        valueClass={tone(metrics.realized)}
        sub={`fees ${formatMoney(metrics.fees)}`}
        hint="Closed-trade PnL before fees (average-cost accounting, as the engine books it)."
      />
      <Tile
        label="Drawdown"
        value={formatPct(metrics.drawdown)}
        valueClass={metrics.drawdown !== null && metrics.drawdown > 0 ? "text-sell" : undefined}
        sub={
          metrics.drawdownLimit !== null
            ? `stop at ${formatPct(metrics.drawdownLimit, { decimals: 1 })}`
            : "no drawdown stop"
        }
        hint="Below the equity high-water mark. The drawdown stop halts new risk and flattens."
      >
        <LimitBar used={metrics.drawdown} limit={metrics.drawdownLimit} />
      </Tile>
      <Tile
        label="Exposure"
        value={formatMoney(metrics.grossExposure)}
        sub={metrics.leverage === null ? "—" : `${metrics.leverage.toFixed(2)}× equity`}
      />
      <Tile
        label="Cash"
        value={formatMoney(metrics.cash)}
        sub={`${metrics.workingOrders} working order${metrics.workingOrders === 1 ? "" : "s"}`}
      />
    </div>
  );
}
