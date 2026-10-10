"use client";

import * as React from "react";
import { AlertTriangle, CandlestickChart, Loader2 } from "lucide-react";
import { useShallow } from "zustand/react/shallow";

import { Blotter } from "@/components/terminal/blotter";
import { ChartPanel } from "@/components/terminal/chart-panel";
import { EquityChart } from "@/components/terminal/equity-chart";
import { MetricsStrip } from "@/components/terminal/metrics-strip";
import { OrderBook } from "@/components/terminal/order-book";
import { PanelBoundary } from "@/components/terminal/panel-boundary";
import { useServices, useTopic } from "@/components/terminal/services";
import { TopBar } from "@/components/terminal/top-bar";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ApiError, type ApiClient } from "@/lib/api/client";
import type { BookPayload, Market } from "@/lib/api/types";
import { formatMoney, formatPct } from "@/lib/format";
import { num } from "@/lib/numbers";
import { defaultSession, resolveMarket, type SessionScope } from "@/lib/selection";
import { positionMetrics, sessionMetrics, type Marks, type SessionMetrics } from "@/lib/session/metrics";
import { orderHistory, workingOrders, type SessionView } from "@/lib/session/reducer";
import { topics } from "@/lib/stream/protocol";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth";
import { bookHandler, healthHandler, sessionHandler } from "@/stores/bindings";
import { useCatalogStore } from "@/stores/catalog";
import { useMarketStore } from "@/stores/market";
import { useSelectionStore } from "@/stores/selection";
import { sessionEntry, useSessionStore, type SessionEntry } from "@/stores/session";

/** The session list is small and changes rarely (a session started or stopped elsewhere). */
const SESSIONS_REFRESH_MS = 15_000;
const MARKETS_RETRY_MS = 5_000;
/** Positions are re-marked to the order-book mids at most this often. */
const MARKS_INTERVAL_MS = 1_000;
/**
 * A book older than this no longer marks positions (the order book panel calls it stale at
 * the same age); the metrics fall back to the equity the engine reported.
 */
const MARK_MAX_AGE_MS = 15_000;
/** Marks are re-checked this often even without book traffic, so stale ones expire. */
const MARKS_EXPIRY_CHECK_MS = 5_000;
/**
 * Order books streamed to mark a session's positions. The gateway allows 32 topics per
 * connection; the chart, its book, the feed health and the session take four.
 */
const MAX_MARK_BOOKS = 16;
const REJECTED_TOKEN = "The gateway rejected the token. Sign in again.";

/** The signed-in terminal: picks what to show, subscribes to it and lays the panels out. */
export function Terminal() {
  const { api } = useServices();
  useCatalog(api);

  const markets = useCatalogStore((state) => state.markets);
  const sessions = useCatalogStore((state) => state.sessions);
  const marketsLoaded = useCatalogStore((state) => state.marketsLoaded);
  const sessionsLoaded = useCatalogStore((state) => state.sessionsLoaded);
  const catalogError = useCatalogStore((state) => state.error);
  const selection = useSelectionStore(
    useShallow((state) => ({
      sessionId: state.sessionId,
      venue: state.venue,
      symbol: state.symbol,
      timeframe: state.timeframe,
      tab: state.tab,
    })),
  );
  const { selectSession, selectMarket, selectTimeframe, selectTab } = useSelectionStore(
    useShallow((state) => ({
      selectSession: state.selectSession,
      selectMarket: state.selectMarket,
      selectTimeframe: state.selectTimeframe,
      selectTab: state.selectTab,
    })),
  );
  const signOut = useAuthStore((state) => state.signOut);

  // An operator who never chose a session is shown a running one once the list arrives.
  const offerDefault = React.useRef(useSelectionStore.getState().fresh);
  React.useEffect(() => {
    if (!offerDefault.current || !sessionsLoaded) return;
    offerDefault.current = false;
    const candidate = defaultSession(sessions);
    if (selection.sessionId === null && candidate !== null) selectSession(candidate);
  }, [sessionsLoaded, sessions, selection.sessionId, selectSession]);

  const sessionId = selection.sessionId;
  const onSessionTopic = React.useMemo(
    () => (sessionId === null ? null : sessionHandler(sessionId)),
    [sessionId],
  );
  useTopic(sessionId === null ? null : topics.session(sessionId), onSessionTopic);
  const entry = useSessionStore((state) => sessionEntry(state, sessionId));
  const view = sessionId === null ? null : entry.view;
  const summary = React.useMemo(
    () => sessions.find((candidate) => candidate.id === sessionId) ?? null,
    [sessions, sessionId],
  );

  const scopeVenue = view?.info.venue ?? summary?.venue ?? null;
  const scopeSymbols = view?.info.symbols ?? summary?.symbols ?? null;
  const scope = React.useMemo<SessionScope | null>(
    () => (scopeVenue !== null && scopeSymbols !== null ? { venue: scopeVenue, symbols: scopeSymbols } : null),
    [scopeVenue, scopeSymbols],
  );
  // Until the selected session's venue and instruments are known, the stored selection is shown
  // as is: resolved against the gateway's markets alone, a session instrument the gateway does
  // not list would be replaced (and the replacement written back) before the session arrives.
  const scopeKnown =
    sessionId === null || scope !== null || entry.status === "unavailable" || entry.status === "error";
  const resolved = React.useMemo(
    () =>
      resolveMarket({
        venue: selection.venue,
        symbol: selection.symbol,
        markets: scopeKnown ? markets : NO_MARKETS,
        session: scope,
      }),
    [selection.venue, selection.symbol, markets, scope, scopeKnown],
  );
  const { venue, symbol } = resolved;

  // Keep the stored selection (and the URL) on what is actually shown.
  React.useEffect(() => {
    if (!marketsLoaded || !scopeKnown || venue === null || symbol === null) return;
    if (venue !== selection.venue || symbol !== selection.symbol) selectMarket(venue, symbol);
  }, [marketsLoaded, scopeKnown, venue, symbol, selection.venue, selection.symbol, selectMarket]);

  const onHealthTopic = React.useMemo(() => (venue === null ? null : healthHandler(venue)), [venue]);
  useTopic(venue === null ? null : topics.health(venue), onHealthTopic);
  const health = useMarketStore((state) => (venue === null ? null : (state.health[venue] ?? null)));

  const marks = useSessionMarks(view?.info.venue ?? null, view?.info.symbols ?? NO_SYMBOLS);
  const metrics = React.useMemo(() => (view === null ? null : sessionMetrics(view, marks)), [view, marks]);
  const positions = React.useMemo(() => (view === null ? [] : positionMetrics(view, marks)), [view, marks]);
  const working = React.useMemo(() => (view === null ? [] : workingOrders(view)), [view]);
  const history = React.useMemo(() => (view === null ? [] : orderHistory(view)), [view]);

  const onSession = React.useCallback(
    (next: string | null) => {
      offerDefault.current = false;
      selectSession(next);
      const target = next === null ? undefined : sessions.find((candidate) => candidate.id === next);
      if (target === undefined) return;
      const current = useSelectionStore.getState();
      if (target.venue !== current.venue || !target.symbols.includes(current.symbol ?? "")) {
        const first = target.symbols[0];
        if (first !== undefined) selectMarket(target.venue, first);
      }
    },
    [sessions, selectSession, selectMarket],
  );
  const onSymbol = React.useCallback(
    (next: string) => {
      if (venue !== null) selectMarket(venue, next);
    },
    [venue, selectMarket],
  );
  const onSignOut = React.useCallback(() => signOut(), [signOut]);

  return (
    <div className="flex min-h-dvh flex-col gap-1.5 bg-background p-1.5 lg:h-dvh lg:min-h-0">
      <TopBar
        sessions={sessions}
        sessionId={sessionId}
        session={view}
        symbols={resolved.symbols}
        symbol={symbol}
        timeframe={selection.timeframe}
        health={health}
        onSession={onSession}
        onSymbol={onSymbol}
        onTimeframe={selectTimeframe}
        onSignOut={onSignOut}
      />
      {catalogError !== null && (
        <Banner>
          <AlertTriangle className="size-3.5 shrink-0" />
          {catalogError}
        </Banner>
      )}
      <PanelBoundary name="metrics" resetKey={sessionId ?? ""}>
        <SessionSummaryRow
          sessionId={sessionId}
          entry={entry}
          metrics={metrics}
          onMarketOnly={() => onSession(null)}
        />
      </PanelBoundary>

      <main className="grid min-h-0 flex-1 grid-cols-1 gap-1.5 lg:grid-cols-[minmax(0,1fr)_300px] 2xl:grid-cols-[minmax(0,1fr)_340px]">
        <div className="flex min-h-0 flex-col gap-1.5">
          <div className="flex h-[440px] min-h-0 flex-col lg:h-auto lg:flex-[3]">
            {venue !== null && symbol !== null ? (
              <PanelBoundary name="chart" resetKey={`${venue}:${symbol}:${selection.timeframe}`}>
                <ChartPanel venue={venue} symbol={symbol} timeframe={selection.timeframe} session={view} />
              </PanelBoundary>
            ) : (
              <Placeholder icon={marketsLoaded ? undefined : "loading"}>
                {marketsLoaded
                  ? "The gateway serves no markets: set CQ_INGEST_SYMBOLS for the ingestor and the gateway."
                  : "Loading markets…"}
              </Placeholder>
            )}
          </div>
          <div className="flex h-[360px] min-h-0 flex-col lg:h-auto lg:min-h-[180px] lg:flex-[2]">
            {view !== null ? (
              <PanelBoundary name="blotter" resetKey={view.info.id}>
                <Blotter
                  tab={selection.tab}
                  onTab={selectTab}
                  positions={positions}
                  working={working}
                  history={history}
                  fills={view.fills}
                  risk={view.risk}
                  quote={view.account.quote}
                  onSelectSymbol={onSymbol}
                />
              </PanelBoundary>
            ) : (
              <Placeholder>
                {sessionId === null
                  ? "Select a trading session to see its positions, orders, fills and risk events."
                  : "Waiting for the session…"}
              </Placeholder>
            )}
          </div>
        </div>

        <aside className="flex min-h-0 flex-col gap-1.5">
          <div className="flex h-[380px] min-h-0 flex-col lg:h-auto lg:flex-[3]">
            {venue !== null && symbol !== null ? (
              <PanelBoundary name="order book" resetKey={`${venue}:${symbol}`}>
                <OrderBook venue={venue} symbol={symbol} />
              </PanelBoundary>
            ) : (
              <Placeholder>No market selected.</Placeholder>
            )}
          </div>
          <div className="flex h-[240px] min-h-0 flex-col lg:h-auto lg:min-h-[180px] lg:flex-[2]">
            <PanelBoundary name="equity" resetKey={sessionId ?? ""}>
              <EquityPanel view={view} metrics={metrics} />
            </PanelBoundary>
          </div>
        </aside>
      </main>
    </div>
  );
}

const NO_SYMBOLS: readonly string[] = [];
const NO_MARKETS: readonly Market[] = [];
const NO_MARKS: Marks = {};

/** Load the markets once (retrying until they arrive) and the sessions every few seconds. */
function useCatalog(api: ApiClient): void {
  React.useEffect(() => {
    const catalog = useCatalogStore.getState();
    let cancelled = false;
    let marketsTimer: ReturnType<typeof setTimeout> | null = null;
    let sessionsTimer: ReturnType<typeof setTimeout> | null = null;

    const failed = (what: string, error: unknown): void => {
      if (cancelled) return;
      if (error instanceof ApiError && error.unauthorized) {
        useAuthStore.getState().signOut(REJECTED_TOKEN);
        return;
      }
      const reason = error instanceof Error ? error.message : String(error);
      catalog.setError(`Could not load ${what}: ${reason}`);
    };

    const loadMarkets = async (): Promise<void> => {
      try {
        const offered = await api.markets();
        if (!cancelled) catalog.setMarkets(offered.markets, offered.timeframes);
      } catch (error) {
        failed("the markets", error);
        if (!cancelled) marketsTimer = setTimeout(() => void loadMarkets(), MARKETS_RETRY_MS);
      }
    };

    const loadSessions = async (): Promise<void> => {
      try {
        const listed = await api.sessions();
        if (!cancelled) catalog.setSessions(listed);
      } catch (error) {
        failed("the sessions", error);
      } finally {
        if (!cancelled) sessionsTimer = setTimeout(() => void loadSessions(), SESSIONS_REFRESH_MS);
      }
    };

    void loadMarkets();
    void loadSessions();
    return () => {
      cancelled = true;
      if (marketsTimer !== null) clearTimeout(marketsTimer);
      if (sessionsTimer !== null) clearTimeout(sessionsTimer);
    };
  }, [api]);
}

/**
 * Streams the order books of a session's instruments and returns their mids, refreshed at most
 * once per {@link MARKS_INTERVAL_MS}: book updates arrive many times a second, but re-marking
 * the positions (and re-rendering the metrics) that often helps no one.
 */
function useSessionMarks(venue: string | null, symbols: readonly string[]): Marks {
  const { stream } = useServices();
  const key = venue === null ? "" : JSON.stringify([venue, symbols.slice(0, MAX_MARK_BOOKS)]);
  const [marks, setMarks] = React.useState<Marks>(NO_MARKS);

  React.useEffect(() => {
    if (key === "") {
      setMarks(NO_MARKS);
      return undefined;
    }
    const [bookVenue, bookSymbols] = JSON.parse(key) as [string, string[]];
    const books = bookSymbols.map((bookSymbol) => ({
      symbol: bookSymbol,
      topic: topics.book(bookVenue, bookSymbol),
    }));
    const releases = books.map(({ topic }) => stream.subscribe(topic, bookHandler(topic)));

    const read = (state: Readonly<Record<string, BookPayload>>): Marks => {
      const next: Record<string, number> = {};
      const now = Date.now();
      for (const { symbol: bookSymbol, topic } of books) {
        const book = state[topic];
        if (book === undefined || book.ts === null || now - book.ts > MARK_MAX_AGE_MS) continue;
        if (typeof book.mid === "number" && Number.isFinite(book.mid)) next[bookSymbol] = book.mid;
      }
      return next;
    };
    let timer: ReturnType<typeof setTimeout> | null = null;
    let publishedAt = 0;
    const publish = (): void => {
      timer = null;
      publishedAt = Date.now();
      const next = read(useMarketStore.getState().books);
      setMarks((previous) => (sameMarks(previous, next) ? previous : next));
    };
    const schedule = (): void => {
      if (timer !== null) return;
      const wait = publishedAt + MARKS_INTERVAL_MS - Date.now();
      if (wait <= 0) publish();
      else timer = setTimeout(publish, wait);
    };
    publish();
    const unsubscribe = useMarketStore.subscribe((state) => state.books, schedule);
    const expiry = setInterval(schedule, MARKS_EXPIRY_CHECK_MS);
    return () => {
      unsubscribe();
      clearInterval(expiry);
      if (timer !== null) clearTimeout(timer);
      for (const release of releases) release();
    };
  }, [stream, key]);

  return marks;
}

function sameMarks(left: Marks, right: Marks): boolean {
  const keys = Object.keys(left);
  if (keys.length !== Object.keys(right).length) return false;
  return keys.every((key) => left[key] === right[key]);
}

/** The metrics strip, or why there is nothing to show yet. */
function SessionSummaryRow({
  sessionId,
  entry,
  metrics,
  onMarketOnly,
}: {
  sessionId: string | null;
  entry: SessionEntry;
  metrics: SessionMetrics | null;
  onMarketOnly(): void;
}) {
  if (sessionId === null || metrics !== null) return <MetricsStrip metrics={metrics} />;
  if (entry.status === "unavailable" || entry.status === "error") {
    return (
      <Banner tone="error">
        <AlertTriangle className="size-3.5 shrink-0" />
        <span>
          Session <span className="font-semibold">{sessionId}</span>:{" "}
          {entry.error ?? "the session stream failed."}
        </span>
        <Button size="xs" variant="outline" className="ml-auto" onClick={onMarketOnly}>
          Market data only
        </Button>
      </Banner>
    );
  }
  return (
    <div className="flex h-[58px] items-center gap-3 rounded-md border bg-card px-3" aria-busy="true">
      {Array.from({ length: 6 }, (_, index) => (
        <div key={index} className="flex flex-1 flex-col gap-1.5">
          <Skeleton className="h-2 w-16" />
          <Skeleton className="h-3.5 w-24" />
        </div>
      ))}
    </div>
  );
}

/** The session's equity curve with its latest value. */
function EquityPanel({ view, metrics }: { view: SessionView | null; metrics: SessionMetrics | null }) {
  return (
    <section className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border bg-card">
      <header className="flex items-center justify-between gap-2 border-b px-3 py-1.5">
        <span className="text-xs font-semibold tracking-wide text-muted-foreground uppercase">Equity</span>
        {metrics !== null && metrics.equity !== null && (
          <span className="flex items-baseline gap-2">
            <span className="figure text-xs font-semibold">
              {formatMoney(metrics.equity)} {metrics.quote}
            </span>
            <span
              className={cn(
                "figure text-[11px]",
                (metrics.totalPnl ?? 0) >= 0 ? "text-buy" : "text-sell",
              )}
            >
              {formatPct(metrics.totalPnlPct, { signed: true })}
            </span>
          </span>
        )}
      </header>
      <div className="relative min-h-0 flex-1">
        {view === null ? (
          <Placeholder bare>The equity curve of the selected session shows here.</Placeholder>
        ) : view.equity.length === 0 ? (
          <Placeholder bare>No equity reported yet: the engine reports once per base bar.</Placeholder>
        ) : (
          <EquityChart
            key={view.info.id}
            points={view.equity}
            baseline={num(view.account.initial_capital)}
          />
        )}
      </div>
    </section>
  );
}

function Placeholder({
  children,
  icon,
  bare = false,
}: {
  children: React.ReactNode;
  icon?: "loading";
  bare?: boolean;
}) {
  return (
    <div
      className={cn(
        "flex min-h-0 flex-1 flex-col items-center justify-center gap-2 px-6 text-center text-xs text-muted-foreground",
        !bare && "rounded-md border border-dashed bg-card/60",
        bare && "absolute inset-0",
      )}
    >
      {icon === "loading" ? (
        <Loader2 className="size-5 animate-spin" />
      ) : (
        !bare && <CandlestickChart className="size-5 opacity-60" />
      )}
      <p className="max-w-sm">{children}</p>
    </div>
  );
}

function Banner({ children, tone = "warning" }: { children: React.ReactNode; tone?: "warning" | "error" }) {
  return (
    <div
      role="alert"
      className={cn(
        "flex items-center gap-2 rounded-md border px-3 py-1.5 text-xs",
        tone === "warning" ? "border-warning/40 bg-warning-muted text-warning" : "border-sell/40 bg-sell-muted text-sell",
      )}
    >
      {children}
    </div>
  );
}
