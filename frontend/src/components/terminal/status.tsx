"use client";

import * as React from "react";
import { Activity, AlertOctagon, Radio, ShieldAlert, Wifi, WifiOff } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import type { Engine, FeedHealth, Flatten, SessionStateView } from "@/lib/api/types";
import { formatDuration, formatTime } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useConnectionStore } from "@/stores/connection";
import { useNow } from "@/hooks/use-now";

function Dot({ tone }: { tone: "ok" | "warn" | "bad" | "idle" }) {
  return (
    <span
      className={cn(
        "inline-block size-1.5 rounded-full",
        tone === "ok" && "bg-buy shadow-[0_0_6px] shadow-buy/70",
        tone === "warn" && "bg-warning",
        tone === "bad" && "bg-sell",
        tone === "idle" && "bg-muted-foreground",
      )}
    />
  );
}

/** The stream connection: transport, latency, reconnect attempts. */
export function ConnectionStatus() {
  const status = useConnectionStore((state) => state.status);
  const transport = useConnectionStore((state) => state.transport);
  const latency = useConnectionStore((state) => state.latencyMs);
  const failures = useConnectionStore((state) => state.failures);
  const error = useConnectionStore((state) => state.error);
  const lastFrameAt = useConnectionStore((state) => state.lastFrameAt);
  const open = status === "open";
  const label = open
    ? `${transport === "sse" ? "SSE" : "WS"}${latency !== null ? ` · ${latency} ms` : ""}`
    : status === "reconnecting"
      ? `Reconnecting${failures > 1 ? ` (${failures})` : ""}`
      : status === "connecting"
        ? "Connecting"
        : status === "unauthorized"
          ? "Unauthorized"
          : "Offline";
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div
          className={cn(
            "flex h-7 items-center gap-1.5 rounded-md border px-2 text-[11px]",
            open ? "text-foreground" : "border-warning/40 text-warning",
          )}
          data-testid="connection-status"
        >
          {open ? <Wifi className="size-3.5 text-buy" /> : <WifiOff className="size-3.5" />}
          <span className="figure">{label}</span>
        </div>
      </TooltipTrigger>
      <TooltipContent>
        {open
          ? `Streaming over ${transport === "sse" ? "Server-Sent Events" : "WebSocket"}; last message ${formatTime(lastFrameAt)} UTC.`
          : (error ?? "Not connected to the gateway.")}
      </TooltipContent>
    </Tooltip>
  );
}

/** The ingestor feeding this venue: alive, and the state of its streams. */
export function FeedStatus({ health }: { health: FeedHealth | null }) {
  const now = useNow(2_000);
  if (health === null) return null;
  const unhealthy = health.streams.filter((stream) => stream.state !== "healthy");
  const tone = !health.alive ? "bad" : unhealthy.length > 0 ? "warn" : "ok";
  const age = health.ts === null ? null : now - health.ts;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div className="flex h-7 items-center gap-1.5 rounded-md border px-2 text-[11px]">
          <Radio className="size-3.5 text-muted-foreground" />
          <Dot tone={tone} />
          <span>{health.venue}</span>
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-sm">
        {!health.alive ? (
          <p>No ingestor heartbeat for {health.venue}: market data is not flowing.</p>
        ) : (
          <div className="flex flex-col gap-1">
            <p>
              Ingestor {health.instance} · heartbeat {formatDuration(age)} ago
            </p>
            {health.streams.map((stream) => (
              <p key={stream.name} className="figure">
                {stream.symbol} {stream.kind}: {stream.state}
                {stream.age_ms !== null && ` · ${formatDuration(stream.age_ms)}`}
                {stream.reconnects > 0 && ` · ${stream.reconnects} reconnects`}
              </p>
            ))}
          </div>
        )}
      </TooltipContent>
    </Tooltip>
  );
}

/** Mode, engine state, halts and flatten progress of the selected session. */
export function SessionBadges({
  mode,
  engine,
  state,
  flatten,
}: {
  mode: string;
  engine: Engine;
  state: SessionStateView;
  flatten: Flatten | null;
}) {
  const now = useNow(5_000);
  const halts = state.halts.filter((halt) => halt.until === null || halt.until > now);
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <Badge variant={mode === "live" ? "destructive" : mode === "sandbox" ? "warning" : "info"}>
        {mode.toUpperCase()}
      </Badge>
      <Tooltip>
        <TooltipTrigger asChild>
          <Badge
            variant={engine.state === "running" ? "buy" : engine.state === "stopping" ? "warning" : "muted"}
          >
            <Activity />
            {engine.state}
          </Badge>
        </TooltipTrigger>
        <TooltipContent>
          {engine.instance ? `Engine ${engine.instance}` : "No engine holds this session."}
        </TooltipContent>
      </Tooltip>
      {halts.map((halt) => (
        <Tooltip key={halt.reason}>
          <TooltipTrigger asChild>
            <Badge variant="sell">
              <ShieldAlert />
              {halt.reason.replace("_", " ")}
            </Badge>
          </TooltipTrigger>
          <TooltipContent className="max-w-sm">
            {halt.detail} (since {formatTime(halt.since)} UTC
            {halt.until !== null ? `, lifts at ${formatTime(halt.until)} UTC` : ""})
            {halt.resumable ? " — Resume lifts it." : ""}
          </TooltipContent>
        </Tooltip>
      ))}
      {(state.flatten !== null || flatten?.state === "started") && (
        <Badge variant="warning">
          <AlertOctagon />
          flattening
        </Badge>
      )}
    </div>
  );
}
