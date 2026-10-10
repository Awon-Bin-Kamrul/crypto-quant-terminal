"use client";

import * as React from "react";

import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { Fill, Order, OrderStatus, RiskEvent } from "@/lib/api/types";
import {
  formatDateTime,
  formatMoney,
  formatPct,
  formatPrice,
  formatQty,
  formatTime,
} from "@/lib/format";
import { num } from "@/lib/numbers";
import type { PositionMetrics } from "@/lib/session/metrics";
import { cn } from "@/lib/utils";
import type { BlotterTab } from "@/stores/selection";

export interface BlotterProps {
  tab: BlotterTab;
  onTab(tab: BlotterTab): void;
  positions: PositionMetrics[];
  working: Order[];
  history: Order[];
  fills: readonly Fill[];
  risk: readonly RiskEvent[];
  quote: string;
  onSelectSymbol(symbol: string): void;
}

/** Positions, open orders, fills, order history and risk events of the session. */
export function Blotter(props: BlotterProps) {
  const open = props.positions.filter((position) => position.side !== "flat");
  const fillsNewestFirst = React.useMemo(() => [...props.fills].reverse(), [props.fills]);
  const riskNewestFirst = React.useMemo(() => [...props.risk].reverse(), [props.risk]);
  return (
    <Tabs
      value={props.tab}
      onValueChange={(value) => props.onTab(value as BlotterTab)}
      className="flex min-h-0 flex-1 flex-col gap-0 overflow-hidden rounded-md border bg-card"
    >
      <div className="flex items-center gap-2 overflow-x-auto border-b px-2 py-1">
        <TabsList className="h-7 w-max shrink-0">
          <TabsTrigger value="positions">Positions {count(open.length)}</TabsTrigger>
          <TabsTrigger value="orders">Open orders {count(props.working.length)}</TabsTrigger>
          <TabsTrigger value="fills">Fills {count(props.fills.length)}</TabsTrigger>
          <TabsTrigger value="history">Order history</TabsTrigger>
          <TabsTrigger value="risk">Risk events {count(props.risk.length)}</TabsTrigger>
        </TabsList>
      </div>
      <ScrollArea className="min-h-0 flex-1">
        <TabsContent value="positions" className="m-0">
          <PositionsTable positions={props.positions} quote={props.quote} onSelect={props.onSelectSymbol} />
        </TabsContent>
        <TabsContent value="orders" className="m-0">
          <OrdersTable orders={props.working} empty="No working orders." live />
        </TabsContent>
        <TabsContent value="fills" className="m-0">
          <FillsTable fills={fillsNewestFirst} />
        </TabsContent>
        <TabsContent value="history" className="m-0">
          <OrdersTable orders={props.history} empty="No finished orders yet." />
        </TabsContent>
        <TabsContent value="risk" className="m-0">
          <RiskTable events={riskNewestFirst} />
        </TabsContent>
      </ScrollArea>
    </Tabs>
  );
}

function count(value: number) {
  return value > 0 ? <span className="figure text-[10px] text-muted-foreground">{value}</span> : null;
}

function Empty({ columns, children }: { columns: number; children: React.ReactNode }) {
  return (
    <TableRow className="hover:bg-transparent">
      <TableCell colSpan={columns} className="py-6 text-center text-xs text-muted-foreground">
        {children}
      </TableCell>
    </TableRow>
  );
}

function Side({ side }: { side: "buy" | "sell" | "long" | "short" | "flat" }) {
  const variant = side === "buy" || side === "long" ? "buy" : side === "flat" ? "muted" : "sell";
  return (
    <Badge variant={variant} className="uppercase">
      {side}
    </Badge>
  );
}

const Num = ({ children, className }: { children: React.ReactNode; className?: string }) => (
  <TableCell className={cn("figure text-right text-xs", className)}>{children}</TableCell>
);

function tone(value: number | null): string | undefined {
  if (value === null || Math.abs(value) < 1e-9) return undefined;
  return value > 0 ? "text-buy" : "text-sell";
}

function PositionsTable({
  positions,
  quote,
  onSelect,
}: {
  positions: PositionMetrics[];
  quote: string;
  onSelect(symbol: string): void;
}) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Symbol</TableHead>
          <TableHead>Side</TableHead>
          <TableHead className="text-right">Quantity</TableHead>
          <TableHead className="text-right">Entry</TableHead>
          <TableHead className="text-right">Mark</TableHead>
          <TableHead className="text-right">Value ({quote})</TableHead>
          <TableHead className="text-right">Unrealized</TableHead>
          <TableHead className="text-right">Realized</TableHead>
          <TableHead className="text-right">Fees</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {positions.length === 0 && <Empty columns={9}>No instruments in this session.</Empty>}
        {positions.map((position) => (
          <TableRow
            key={position.symbol}
            className={cn("cursor-pointer", position.side === "flat" && "text-muted-foreground")}
            onClick={() => onSelect(position.symbol)}
          >
            <TableCell className="text-xs font-medium">{position.symbol}</TableCell>
            <TableCell>
              <Side side={position.side} />
            </TableCell>
            <Num>{formatQty(position.quantity)}</Num>
            <Num>{position.side === "flat" ? "—" : formatPrice(position.averagePrice)}</Num>
            <Num>{formatPrice(position.mark)}</Num>
            <Num>{formatMoney(position.marketValue)}</Num>
            <Num className={tone(position.unrealized)}>
              {formatMoney(position.unrealized, { signed: true })}
              {position.unrealizedPct !== null && (
                <span className="ml-1 text-[10px] opacity-80">
                  {formatPct(position.unrealizedPct, { signed: true })}
                </span>
              )}
            </Num>
            <Num className={tone(position.realized)}>
              {formatMoney(position.realized, { signed: true })}
            </Num>
            <Num className="text-muted-foreground">{formatMoney(position.fees)}</Num>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

const STATUS_VARIANT: Record<OrderStatus, "info" | "warning" | "buy" | "muted" | "sell"> = {
  pending: "warning",
  unknown: "warning",
  open: "info",
  partially_filled: "info",
  cancel_pending: "warning",
  filled: "buy",
  cancelled: "muted",
  rejected: "sell",
  expired: "muted",
};

function OrdersTable({ orders, empty, live = false }: { orders: Order[]; empty: string; live?: boolean }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>{live ? "Placed" : "Updated"}</TableHead>
          <TableHead>Symbol</TableHead>
          <TableHead>Side</TableHead>
          <TableHead>Type</TableHead>
          <TableHead className="text-right">Price</TableHead>
          <TableHead className="text-right">Quantity</TableHead>
          <TableHead className="text-right">Filled</TableHead>
          <TableHead className="text-right">Avg price</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Intent / reason</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {orders.length === 0 && <Empty columns={10}>{empty}</Empty>}
        {orders.map((order) => {
          const quantity = num(order.quantity);
          const filled = num(order.filled);
          return (
            <TableRow key={order.client_order_id} title={order.client_order_id}>
              <TableCell className="figure text-xs text-muted-foreground">
                {formatTime(live ? order.created : order.ts)}
              </TableCell>
              <TableCell className="text-xs font-medium">{order.symbol}</TableCell>
              <TableCell>
                <Side side={order.side} />
              </TableCell>
              <TableCell className="text-xs uppercase">
                {order.type}
                {order.type === "limit" && <span className="text-muted-foreground"> · {order.tif}</span>}
                {order.reduce_only && <span className="text-muted-foreground"> · RO</span>}
              </TableCell>
              <Num>{order.limit_price === null ? "market" : formatPrice(num(order.limit_price))}</Num>
              <Num>{formatQty(quantity)}</Num>
              <Num>
                {formatQty(filled)}
                <span className="ml-1 text-[10px] text-muted-foreground">
                  {quantity > 0 ? formatPct(filled / quantity, { decimals: 0 }) : ""}
                </span>
              </Num>
              <Num>{order.average_price === null ? "—" : formatPrice(num(order.average_price))}</Num>
              <TableCell>
                <Badge variant={STATUS_VARIANT[order.status]}>{order.status.replace("_", " ")}</Badge>
              </TableCell>
              <TableCell className="max-w-[320px] truncate text-xs text-muted-foreground">
                {order.reason ?? order.intent}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

function FillsTable({ fills }: { fills: readonly Fill[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Time (UTC)</TableHead>
          <TableHead>Symbol</TableHead>
          <TableHead>Side</TableHead>
          <TableHead className="text-right">Quantity</TableHead>
          <TableHead className="text-right">Price</TableHead>
          <TableHead className="text-right">Notional</TableHead>
          <TableHead className="text-right">Fee</TableHead>
          <TableHead>Liquidity</TableHead>
          <TableHead>Trade</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {fills.length === 0 && <Empty columns={9}>No executions yet.</Empty>}
        {fills.map((fill) => {
          const quantity = num(fill.quantity);
          const price = num(fill.price);
          return (
            <TableRow key={`${fill.symbol}:${fill.trade_id}`} title={fill.client_order_id}>
              <TableCell className="figure text-xs text-muted-foreground">
                {formatDateTime(fill.ts)}
              </TableCell>
              <TableCell className="text-xs font-medium">{fill.symbol}</TableCell>
              <TableCell>
                <Side side={fill.side} />
              </TableCell>
              <Num>{formatQty(quantity)}</Num>
              <Num>{formatPrice(price)}</Num>
              <Num>{formatMoney(quantity * price)}</Num>
              <Num className="text-muted-foreground">
                {formatQty(num(fill.fee), 8)} {fill.fee_currency}
              </Num>
              <TableCell className="text-xs text-muted-foreground">{fill.liquidity}</TableCell>
              <TableCell className="figure max-w-[140px] truncate text-[11px] text-muted-foreground">
                {fill.trade_id}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

const RISK_VARIANT: Record<RiskEvent["kind"], "info" | "warning" | "sell" | "muted" | "buy"> = {
  rejected: "warning",
  adjusted: "info",
  breach: "sell",
  halt: "sell",
  resume: "buy",
  anomaly: "sell",
  market_data: "muted",
};

function RiskTable({ events }: { events: readonly RiskEvent[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Time (UTC)</TableHead>
          <TableHead>Kind</TableHead>
          <TableHead>Code</TableHead>
          <TableHead>Symbol</TableHead>
          <TableHead>Message</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {events.length === 0 && <Empty columns={5}>No risk events.</Empty>}
        {events.map((event, index) => (
          <TableRow key={`${event.ts}:${event.code}:${index}`}>
            <TableCell className="figure text-xs text-muted-foreground">
              {formatDateTime(event.ts)}
            </TableCell>
            <TableCell>
              <Badge variant={RISK_VARIANT[event.kind]}>{event.kind.replace("_", " ")}</Badge>
            </TableCell>
            <TableCell className="figure text-xs">{event.code}</TableCell>
            <TableCell className="text-xs">{event.symbol ?? "—"}</TableCell>
            <TableCell className="text-xs whitespace-normal text-muted-foreground">
              {event.message}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
