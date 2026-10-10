"use client";

import * as React from "react";
import { LogOut } from "lucide-react";

import { CommandBar } from "@/components/terminal/command-dialog";
import { ConnectionStatus, FeedStatus, SessionBadges } from "@/components/terminal/status";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import type { FeedHealth, SessionSummary } from "@/lib/api/types";
import { formatTime } from "@/lib/format";
import { TIMEFRAMES, type Timeframe } from "@/lib/market/timeframes";
import type { SessionView } from "@/lib/session/reducer";
import { useNow } from "@/hooks/use-now";

const NO_SESSION = "__market__";

export interface TopBarProps {
  sessions: SessionSummary[];
  sessionId: string | null;
  session: SessionView | null;
  symbols: string[];
  symbol: string | null;
  timeframe: Timeframe;
  health: FeedHealth | null;
  onSession(sessionId: string | null): void;
  onSymbol(symbol: string): void;
  onTimeframe(timeframe: Timeframe): void;
  onSignOut(): void;
}

export function TopBar(props: TopBarProps) {
  const now = useNow(1_000);
  const sessionIds = new Set(props.sessions.map((session) => session.id));
  const selectedMissing = props.sessionId !== null && !sessionIds.has(props.sessionId);
  return (
    <header className="flex flex-wrap items-center gap-2 rounded-md border bg-card px-2 py-1.5">
      <div className="flex items-center gap-2 pr-1">
        <Logo />
        <span className="hidden text-sm font-semibold tracking-tight sm:inline">Quant Terminal</span>
      </div>
      <Separator orientation="vertical" className="hidden h-5! sm:block" />

      <Select
        value={props.sessionId ?? NO_SESSION}
        onValueChange={(value) => props.onSession(value === NO_SESSION ? null : value)}
      >
        <SelectTrigger size="sm" className="min-w-[150px] font-medium" aria-label="Trading session">
          <SelectValue placeholder="Session" />
        </SelectTrigger>
        <SelectContent>
          <SelectGroup>
            <SelectLabel>Trading sessions</SelectLabel>
            {props.sessions.map((session) => (
              <SelectItem
                key={session.id}
                value={session.id}
                description={`${session.mode} · ${session.strategy} · ${session.status}`}
              >
                {session.id}
              </SelectItem>
            ))}
            {selectedMissing && props.sessionId !== null && (
              <SelectItem value={props.sessionId}>{props.sessionId}</SelectItem>
            )}
          </SelectGroup>
          <SelectSeparator />
          <SelectItem value={NO_SESSION}>Market data only</SelectItem>
        </SelectContent>
      </Select>

      <Select value={props.symbol ?? ""} onValueChange={props.onSymbol}>
        <SelectTrigger size="sm" className="min-w-[128px]" aria-label="Market">
          <SelectValue placeholder="Market" />
        </SelectTrigger>
        <SelectContent>
          {props.symbols.map((symbol) => (
            <SelectItem key={symbol} value={symbol}>
              {symbol}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <ToggleGroup
        type="single"
        size="xs"
        variant="outline"
        value={props.timeframe}
        onValueChange={(value) => {
          if (value) props.onTimeframe(value as Timeframe);
        }}
        aria-label="Timeframe"
      >
        {TIMEFRAMES.map((timeframe) => (
          <ToggleGroupItem key={timeframe} value={timeframe} className="figure px-2">
            {timeframe}
          </ToggleGroupItem>
        ))}
      </ToggleGroup>

      {props.session && (
        <>
          <Separator orientation="vertical" className="hidden h-5! md:block" />
          <SessionBadges
            mode={props.session.info.mode}
            engine={props.session.engine}
            state={props.session.state}
            flatten={props.session.flatten}
          />
        </>
      )}

      <div className="ml-auto flex flex-wrap items-center gap-1.5">
        {props.session && props.sessionId && (
          <CommandBar sessionId={props.sessionId} running={props.session.engine.state !== "stopped"} />
        )}
        <FeedStatus health={props.health} />
        <ConnectionStatus />
        <span className="figure hidden px-1 text-[11px] text-muted-foreground lg:inline">
          {formatTime(now)} UTC
        </span>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button size="icon-sm" variant="ghost" onClick={props.onSignOut} aria-label="Sign out">
              <LogOut />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Sign out (forget the token)</TooltipContent>
        </Tooltip>
      </div>
    </header>
  );
}

export function Logo({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" className={className ?? "size-6"} aria-hidden="true">
      <rect x="1" y="1" width="22" height="22" rx="5" fill="var(--primary)" opacity="0.14" />
      <path d="M6 15.5 9.5 11l3 2.5L18 7" fill="none" stroke="var(--primary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx="18" cy="7" r="1.6" fill="var(--buy)" />
    </svg>
  );
}
