"use client";

import * as React from "react";

import { useTopic } from "@/components/terminal/services";
import { formatDuration, formatPrice, formatQty } from "@/lib/format";
import { ladder, type LadderRow } from "@/lib/market/book";
import { topics } from "@/lib/stream/protocol";
import { cn } from "@/lib/utils";
import { bookHandler } from "@/stores/bindings";
import { useMarketStore } from "@/stores/market";
import { useNow } from "@/hooks/use-now";

const LEVELS = 12;
const STALE_MS = 15_000;

/** Top of the order book: asks above, bids below, depth bars by cumulative size. */
export function OrderBook({ venue, symbol }: { venue: string; symbol: string }) {
  const topic = topics.book(venue, symbol);
  const handler = React.useMemo(() => bookHandler(topic), [topic]);
  useTopic(topic, handler);
  const book = useMarketStore((state) => state.books[topic] ?? null);
  const now = useNow(1_000);
  const rows = React.useMemo(() => ladder(book, LEVELS), [book]);
  const age = book?.ts ? now - book.ts : null;
  const stale = age !== null && age > STALE_MS;

  return (
    <section className="flex min-h-0 flex-col overflow-hidden rounded-md border bg-card">
      <header className="flex items-center justify-between border-b px-3 py-1.5">
        <span className="text-xs font-semibold tracking-wide text-muted-foreground uppercase">
          Order book
        </span>
        <span className={cn("text-[11px] text-muted-foreground", stale && "text-warning")}>
          {book?.ts ? (stale ? `stale · ${formatDuration(age)}` : "live") : "waiting…"}
        </span>
      </header>
      <div className="grid grid-cols-3 px-3 pt-1 text-[10px] text-muted-foreground uppercase">
        <span>Price</span>
        <span className="text-right">Size</span>
        <span className="text-right">Total</span>
      </div>
      <div className={cn("flex min-h-0 flex-1 flex-col px-1 pb-1", stale && "opacity-60")}>
        <div className="flex flex-1 flex-col-reverse justify-start">
          {rows.asks.map((row) => (
            <Level key={`a${row.price}`} row={row} side="ask" />
          ))}
        </div>
        <div className="my-0.5 flex items-center justify-between rounded-sm bg-muted/60 px-2 py-1 text-[11px]">
          <span className="figure font-semibold">{formatPrice(rows.mid)}</span>
          <span className="figure text-muted-foreground">
            spread {formatPrice(rows.spread)}
            {rows.spreadBps !== null && ` · ${rows.spreadBps.toFixed(1)} bp`}
          </span>
        </div>
        <div className="flex flex-1 flex-col">
          {rows.bids.map((row) => (
            <Level key={`b${row.price}`} row={row} side="bid" />
          ))}
        </div>
      </div>
    </section>
  );
}

function Level({ row, side }: { row: LadderRow; side: "bid" | "ask" }) {
  return (
    <div className="relative grid h-[18px] grid-cols-3 items-center px-2 text-[11px]">
      <div
        className={cn(
          "absolute inset-y-px right-0 rounded-l-sm",
          side === "bid" ? "bg-buy-muted" : "bg-sell-muted",
        )}
        style={{ width: `${Math.max(2, row.depth * 100)}%` }}
      />
      <span className={cn("figure relative", side === "bid" ? "text-buy" : "text-sell")}>
        {formatPrice(row.price)}
      </span>
      <span className="figure relative text-right">{formatQty(row.size, 4)}</span>
      <span className="figure relative text-right text-muted-foreground">
        {formatQty(row.total, 4)}
      </span>
    </div>
  );
}
