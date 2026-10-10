# Crypto Quant Terminal — web terminal

The operator's screen for the platform: live candles with executions marked on them, the order
book, and, for a trading session, its equity, PnL, risk limits, positions, working orders, fills
and risk events. It also carries the session's kill switches (halt, flatten, resume, stop).

![The web terminal on a paper session](../docs/images/web-terminal.png)

*A paper session on a synthetic market, streamed through the gateway.*

It is a client-rendered Next.js app. It holds no data and has no server-side logic of its own.
Everything it shows comes from the **terminal gateway** (`cq api serve`, Module 4 backend):

- **REST** for catalogs, history pages and commands;
- one **WebSocket** stream for everything live, falling back to **Server-Sent Events** where
  WebSockets are blocked.

```
browser ──REST──▶ /api/v1/{markets,candles,sessions,…}
        ──WS────▶ /api/v1/stream/ws   (or SSE: /api/v1/stream/sse?ticket=…&topic=…)
                     │
                     ▼
            gateway ── Redis (live bars, books, session events) + TimescaleDB (history)
```

## Run it

Prerequisites:

- the backend stack from the repository root: `make env up migrate`;
- market data: `make ingest`;
- optionally, a trading session: `make trade SESSION=… STRATEGY=… SYMBOL=…`;
- Node.js 22 or later.

```bash
make api            # terminal gateway on http://127.0.0.1:8000 (reads CQ_API_* from .env)
make web-install    # once
make web-dev        # http://localhost:3000
```

1. Sign in with the value of `CQ_API_TOKEN` from the root `.env`. `make env` generated it.
2. The token is checked against the gateway before it is stored.
3. By default the token is kept only for the browser tab. "Remember on this device" moves it to
   `localStorage`.

For production, run `make web-build`, then `make web-start`. The build is also emitted as a
self-contained server in `.next/standalone`: run `node server.js` there, and copy `.next/static`
and `public/` next to it as usual.

## Configuration

`frontend/.env.local` (see `.env.example`):

| Variable                          | Default                 | Meaning                                  |
| --------------------------------- | ----------------------- | ---------------------------------------- |
| `NEXT_PUBLIC_CQ_API_URL`          | `http://127.0.0.1:8000` | Base URL of the gateway                  |
| `NEXT_PUBLIC_CQ_STREAM_TRANSPORT` | `auto`                  | `auto` (WebSocket, SSE fallback), `ws`, `sse` |

`NEXT_PUBLIC_*` values are inlined at build time, so rebuild after changing them.

The gateway must allow the terminal's origin in `CQ_API_CORS_ORIGINS`. The default allows
`http://localhost:3000` and `http://127.0.0.1:3000`.

### Behind a reverse proxy

Streams are long-lived. The proxy must:

- pass WebSocket upgrades on `/api/v1/stream/ws`;
- disable response buffering on `/api/v1/stream/sse` (for nginx: `proxy_buffering off`). The
  gateway also sends `X-Accel-Buffering: no`.

Idle streams receive a heartbeat every `CQ_API_HEARTBEAT_S`, so proxy read timeouts above that
value are safe.

## What is on the screen

| Panel | Source | Notes |
| --- | --- | --- |
| Top bar | `GET /sessions` (every 15 s), `GET /markets`, `health:{venue}` | Session, market and timeframe pickers; session mode and engine state; halt badges; the kill switches; feed health; stream transport and latency |
| Metrics strip | `session:{id}` + `book:{venue}:{symbol}` of every session instrument | Equity, total and day PnL, unrealised, realised, drawdown against the stop, day loss against the limit, exposure and leverage, cash |
| Price chart | `candles:{venue}:{symbol}:{tf}`, `GET /candles` when scrolled back | Candles and volume, fill markers, working-order and entry price lines, OHLC legend |
| Order book | `book:{venue}:{symbol}` | 12 levels with depth bars, spread in basis points, stale warning |
| Equity | `session:{id}` | The engine's equity curve against the starting capital |
| Blotter | `session:{id}` | Positions (marked live), open orders, fills, order history, risk events |

**Marking.** Positions are re-marked to the order-book mids at most once a second. A book
older than 15 seconds no longer counts. When an instrument has no fresh book (the ingestor is
not streaming it, or the stream is down), the metrics fall back to the equity the engine last
reported, and the tooltip on the Equity tile says which one is shown.

**Fill markers.** Fills are grouped by candle and side into one arrow, labelled with the count
and the total quantity, for example `B ×2 0.4`. When the chart is zoomed out and labels would
overprint, the older ones are dropped; the arrows stay. The legend shows the hovered candle's
executions with their volume-weighted price, for example `▲ B ×2 0.4 @ 103.000`.

**History.** Scrolling the chart back loads older candles a page at a time. Pages skip over
gaps in the stored data, and a reconnect keeps the history already loaded.

**Failures stay local.** A panel that fails to render shows the error and a retry button. The
rest of the terminal, including the kill switches, keeps working.

**Commands.** Halt, flatten, resume and stop go to `POST /sessions/{id}/commands` with a
mandatory reason, which is recorded in the audit trail. A command sent while no engine is
running is queued, and the dialog says so.

## How it is built

```
src/
  app/                    Next.js entry (layout, page, icon, global styles and theme tokens)
  components/ui/          shadcn/ui primitives (new-york style, Radix, Tailwind v4)
  components/terminal/    the terminal panels
  lib/api/                wire types and the REST client
  lib/stream/             stream protocol and StreamClient (WS/SSE, tickets, resync)
  lib/market/             candle merging, fill markers, order-book ladder, timeframes
  lib/session/            session reducer (snapshot + deltas) and PnL/risk metrics
  stores/                 zustand stores: auth, selection, catalog, market, session, connection
```

### Stream client (`lib/stream/client.ts`)

- **Connection.** One connection carries every topic. Each connection first obtains a
  single-use ticket (`POST /stream/tickets`), so the bearer token never appears in a URL.
- **Transport.** It tries WebSocket first. After two failed handshakes it switches to SSE for
  the rest of the page's life.
- **Reconnects.** They back off exponentially with jitter. A watchdog reconnects when neither
  data nor heartbeats arrive for 2.5 heartbeat intervals.
- **Ordering.** Every topic starts with a snapshot, followed by sequence-numbered updates. A
  sequence gap re-requests a snapshot: a resubscribe on WebSocket, a reconnect on SSE. The views
  are never patched from an incomplete sequence.
- **Topic errors.** An unknown session or an unserved market arrives as a per-topic error frame,
  shown in the panel concerned.
- **Rejected token.** A token the gateway rejects signs the operator out.

### Rendering

- **Candles.** Candle updates bypass React. The chart subscribes to its store slice
  imperatively and calls `series.update()` for the newest candle. It re-sets the data only when
  the series is replaced: a snapshot, an older page merged in, or a corrected candle.
- **Session state.** The session reducer is idempotent. Orders keep their highest version,
  fills are keyed by trade id and equity points by time. A resent or reordered delta therefore
  never corrupts the view.
- **Selection.** The selection (session, market, timeframe, blotter tab) is remembered in
  `localStorage` and mirrored in the URL (`?session=…&venue=…&symbol=…&tf=…`), so views can be
  bookmarked. A stale selection is resolved against what the gateway serves.

## Checks

```bash
make web-check      # eslint, tsc --noEmit, unit tests (node:test via tsx)
```

The unit tests cover the logic that decides what the operator sees:

- the stream client: handshake, SSE fallback, sequence gaps, watchdog, tickets, backoff;
- the session reducer and PnL/risk metrics;
- candle merging and fill markers;
- the order-book ladder;
- number formatting;
- the REST client's error mapping;
- selection resolution.
