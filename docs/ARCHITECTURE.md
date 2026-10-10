# Crypto Quant Terminal — Architecture

Status: **Module 4 (terminal gateway and web terminal) delivered** on top of Modules 0–3. This document is the contract every later module is built
against. When code and this document disagree, one of them gets fixed in the same pull request.

---

## 1. Decisions that shape everything else

| #   | Decision | Why |
| --- | --- | --- |
| D1  | **One engine, three modes.** Paper, sandbox and live run the same `TradingCore` inside the same `ExecutionRunner`; only the venue adapter changes. Backtests run the M2 engines, which share the strategies and fill semantics, until M7 moves them onto the core. | Backtest/live divergence is the most expensive bug in quant systems. Parity becomes a test suite instead of a hope. |
| D2  | **Single-writer event loop per session.** All state mutation happens in one asyncio task draining one inbox. The `TradingCore` is synchronous and does no I/O; it answers each event with effects, which the runner carries out, and their results come back as events. | Deterministic replay, no locks, and a record (orders, fills, risk events) that fully explains every position. |
| D3  | **Look-ahead is prevented structurally.** Strategies only ever receive closed bars; indicators are backward-only (streaming, or Polars expressions over rows ≤ t); a decision at close *t* can only fill on prices after close *t* + latency, in both engines. | A test truncates future data and asserts no past signal, fill or equity value changes. Convention alone does not survive year two. |
| D4  | **`Decimal` for anything that becomes an order or a ledger entry, `float64` for research.** The conversion happens once, in risk sizing, where lot, tick and min-notional filters apply. The M2 backtest engines are research tools and account in `float64`; lot size and minimum notional are still simulated. | Exchanges reject mis-quantized orders, and float drift in a ledger never reconciles. |
| D5  | **Redis Streams for data that drives decisions, Pub/Sub only for latest-wins display data.** | Streams are ordered, acknowledged and replayable. Pub/Sub silently drops for slow consumers, which is fine for an order-book repaint and never fine for a fill. |
| D6  | **TimescaleDB, not QuestDB.** | One PostgreSQL holds time series *and* relational state (orders, fills, runs) with transactions across both. Compression and continuous aggregates cover crypto bar volumes comfortably. The `BarRepository` port keeps a QuestDB adapter possible, and migration 0002 applies TimescaleDB features only when the extension exists, so plain PostgreSQL works too. |
| D7  | **1-minute bars are canonical;** every higher timeframe is a continuous aggregate of them. | Every timeframe agrees with every other, and there is no mixing of venue-built and self-built candles. |
| D8  | **VectorBT-style and Backtrader-style engines are both native; `vectorbt` itself is not a dependency.** The vectorized engine evaluates every parameter set's signal in one Polars query and simulates each in NumPy with the event engine's exact fill rules, so the two agree to 1e-12 under shared assumptions. | Its pandas-based API would be a second strategy code path (breaks D1) and its pins would hold NumPy and Polars back. Vectorized gives breadth (parameter sweeps); event-driven gives truth (latency, limit orders, impact) for promotion decisions. |
| D9  | **Pydantic v2 at the edges, frozen slotted dataclasses in the core.** | Config, API schemas, exchange payload parsing and wire serialization get validation; the domain stays framework-free and cheap to construct on hot paths. |
| D10 | **Live trading is off by default and two-key armed:** `CQ_TRADING_MODE=live` *and* `CQ_LIVE_TRADING_ENABLED=true`, plus startup checks: sandbox off, credentials present, both USD hard caps set (`CQ_MAX_ORDER_NOTIONAL_USD`, `CQ_MAX_GROSS_EXPOSURE_USD`), a USD-stablecoin quote, a spot market, an API key without withdrawal rights where the venue exposes them, and every unfinished order reconciled before the first new one. A deployment never runs a session above its own mode. | Hard notional caps and the kill switch sit outside strategy control, so no strategy bug can remove them. |
| D11 | **Every run is reproducible.** Each run's manifest records the code version (git describe), every cost-model setting, the data's content hash and the RNG seed, and the run id carries a fingerprint of them; randomness only comes from an injected `numpy.random.Generator`. | Research results are only worth something if they can be re-derived. |
| D12 | **Migrations are plain SQL run by a small in-house migrator, not Alembic.** | Every schema object is TimescaleDB DDL with no ORM model. The runner keeps what matters — advisory lock, one transaction per migration, SHA-256 checksums of applied files — and drops two dependencies. |

---

## 2. Repository layout

`M<n>` marks the module that builds each part. Module 0 creates the layer packages, tooling and
infrastructure; everything else is filled in module by module.

```
crypto-quant-terminal/
├── Makefile                          M0   up · check · migrate · ingest · backfill · gaps · backtest · sweep
│                                     M3   trade · sessions · session · orders · halt · flatten · resume · stop
│                                     M4   api · web-install · web-dev · web-build · web-start · web-check
├── docker-compose.yml                M0   TimescaleDB + Redis (app services join in M9)
├── .env.example                      M0   one env contract for compose and backend
├── docs/
│   ├── ARCHITECTURE.md               M0   this document
│   └── adr/                          M2+  one record per hard-to-reverse decision
├── infra/
│   ├── timescaledb/initdb/           M0   extensions · roles · schemas · test database
│   ├── redis/redis.conf              M0   AOF, eviction policy, pub/sub buffer limits
│   ├── docker/                       M9   backend and frontend images
│   └── observability/                M9   Prometheus scrape config, Grafana dashboards
├── backend/
│   ├── pyproject.toml                M0   dependencies, tooling, import contracts
│   ├── uv.lock                       M0   generated by `uv lock`, committed
│   ├── src/crypto_quant/
│   │   ├── domain/                   PURE — stdlib, Decimal, Polars/NumPy compute only
│   │   │   ├── shared/               M1   UTC-ns time, errors · M3 exact decimal helpers
│   │   │   ├── market/               M1   InstrumentId · Timeframe · Bar · OrderBook · ranges
│   │   │   │                         M2   MarketFrame (columnar history) · resample
│   │   │   ├── instruments/          M3 ✓ InstrumentSpec: tick, lot, min/max quantity and notional
│   │   │   ├── execution/            M2   maker/taker fees · slippage (fixed, √-impact) · latency
│   │   │   ├── indicators/           M2   streaming, Polars-identical · M5 50+ Polars expressions
│   │   │   ├── orders/               M3 ✓ Order aggregate, forward-only state machine,
│   │   │   │                              deterministic client order ids
│   │   │   ├── portfolio/            M3 ✓ Ledger: cash, positions, fees, realised PnL (Decimal)
│   │   │   ├── risk/                 M3 ✓ limits · RiskManager (pre-trade, drawdown, daily loss) ·
│   │   │   │                              sizers (fixed, Kelly, volatility parity)
│   │   │   ├── strategy/             M2   Strategy · VectorizedStrategy · StrategyContext
│   │   │   └── metrics/              M2   Sharpe · Sortino · drawdown · Calmar · trades · M6 DSR, PBO
│   │   ├── strategies/               M2   mean_reversion · momentum — imports domain only
│   │   ├── application/              ORCHESTRATION — domain + ports, no frameworks
│   │   │   ├── ports/                M1+  feeds · history · storage · publisher · clock · artifacts
│   │   │   │                         M3   ExecutionVenue · TradingStore · TradingFeed · commands ·
│   │   │   │                              SessionLease · session events · trading metrics
│   │   │   │                         M4   live market data · feed health · session event tail ·
│   │   │   │                              stream tickets
│   │   │   ├── ingestion/            M1   live pipeline, candle tracker, gap fill, backfill
│   │   │   ├── backtest/             M2   event + vectorized engines, portfolio, price paths,
│   │   │   │                              grids, BacktestService, run manifests
│   │   │   ├── execution/            M3 ✓ TradingCore · OrderManager · ExecutionRunner ·
│   │   │   │                              TradingSession · PaperVenue · arming · Kelly inputs
│   │   │   ├── terminal/             M4 ✓ topics · candle series · session view · TopicHub ·
│   │   │   │                              topic feeds · queries
│   │   │   ├── research/             M6   walk-forward, Monte Carlo, promotion gates
│   │   │   └── ml/                   M6   triple-barrier labels, meta-model pipeline
│   │   ├── infrastructure/           DRIVEN ADAPTERS — implement application ports
│   │   │   ├── config/               M1   pydantic-settings, one section per env prefix
│   │   │   ├── observability/        M1   structlog, Prometheus metrics
│   │   │   ├── persistence/          M1   asyncpg pools, SQL migrations + migrator, repos
│   │   │   │                         M2   PostgresBarHistory (date_bin resampling)
│   │   │   │                         M3   PostgresTradingStore (fenced batches, versioned orders)
│   │   │   ├── messaging/            M1   Redis publisher, keys, codec
│   │   │   │                         M3   trading feed · commands · session lease · session events
│   │   │   │                         M4   Pub/Sub mux · live market · feed health · session event
│   │   │   │                              tail · stream tickets
│   │   │   ├── exchanges/            M1   CCXT live + history
│   │   │   │                         M3   CcxtExecutionVenue · error classification · instrument specs
│   │   │   ├── files/                M2   CSV history (.csv/.gz/.zip), run artifacts
│   │   │   └── artifacts/            M6   Parquet research runs, model registry
│   │   ├── bootstrap/                COMPOSITION ROOT — wires adapters to ports per mode;
│   │   │                                  M2 strategy registry · M3 trading sessions · M4 API
│   │   └── entrypoints/              DRIVING ADAPTERS — cli/ M1–M4 (`cq trade`, `cq api`) ·
│   │                                      api/ M4 ✓ REST · WebSocket · SSE (FastAPI)
│   └── tests/
│       ├── unit/                     domain + application against in-memory fakes
│       ├── contract/                 one suite per port, run against fake and real adapter
│       ├── integration/              real TimescaleDB + Redis from docker-compose
│       ├── parity (marker)           M2   vectorized ≡ event-driven · M7 ≡ paper replay
│       └── support/                  fakes, virtual-time clock, Redis helpers
└── frontend/                         M4 ✓ Next.js 15 · React 19 · TypeScript · Tailwind v4
    └── src/
        ├── app/                      layout · page · theme tokens (dark only)
        ├── components/ui/            shadcn/ui primitives (Radix)
        ├── components/terminal/      top bar · metrics strip · price chart (Lightweight Charts) ·
        │                             order book · equity curve · blotter · command dialog
        ├── stores/                   Zustand: auth · selection · catalog · market · session ·
        │                             connection
        └── lib/                      api/ (typed REST client) · stream/ (WS/SSE client, seq,
                                      resync) · market/ · session/ (reducer, PnL metrics)
                                      M8 backtest and research explorers
```

### Layer rules

Dependencies point inward only. The rules are checked by `lint-imports` (contracts live in
`backend/pyproject.toml`) and run in `make check`, so a violation fails the build.

| Layer | May import | Must not import |
| --- | --- | --- |
| `domain` | stdlib, `polars`, `numpy` | any other layer; `pydantic`, `structlog`, `pandas`, every I/O or web library |
| `strategies` | `domain` (Polars imported lazily, inside `vector_exposure`) | `application`, `infrastructure`, `bootstrap`, `entrypoints`, I/O libraries |
| `application` | `domain`, `polars`, `numpy`, `structlog` | `infrastructure`, `bootstrap`, `entrypoints`, `strategies`, `pydantic`, every I/O or web library |
| `infrastructure` | `application`, `domain`, any library | `bootstrap`, `entrypoints` |
| `bootstrap` | everything below it, including `strategies` (the registry) | `entrypoints` |
| `entrypoints` | `bootstrap`, `application`, `domain` | `infrastructure` (adapters are reached through the container) |

Strategies are isolated from I/O on purpose. A strategy that cannot reach the network or the
database behaves identically in a backtest and in production.

---

## 3. Runtime topology

Five process types around two stateful stores. Each engine process owns exactly one session and
holds a Redis lease for it, so there can never be two writers for the same book of positions.

```
                ┌───────────────────────────────────────────────────────────────┐
                │        EXCHANGES  ·  CCXT REST  +  ccxt.pro WebSocket         │
                └─┬───────────────────────▲─────────────┬─────────────────────┬─┘
                  │                       │             │                     │
                  │ public WS             │ REST        │ private WS          │ REST
                  │ bars · trades         │ orders      │ acks · fills        │ klines
                  │ order book            │             │                     │ backfill
  ┌───────────────▼──────────┐     ┌──────┴─────────────▼───────┐     ┌───────▼───────────────┐
  │ INGESTOR                 │     │ ENGINE                     │     │ RESEARCH WORKER       │
  │ one per venue            │     │ one per session            │     │ N processes           │
  │ normalize · validate     │     │ paper · sandbox · live     │     │ backfill · sweeps     │
  │ sequence · gap-detect    │     │ single-writer loop         │     │ backtest · WFO        │
  │ conflate book ≤ 10 Hz    │     │ Strategy→Risk→OMS→Venue    │     │ Monte Carlo · ML      │
  └─────┬───────────────┬────┘     └────▲─────────────┬───────▲─┘     └───────▲───────────────┘
        │ XADD          │ PUBLISH       │ XREAD       │ XADD  │ XREAD         │ XREADGROUP
        │ closed bars   │ bars, book    │ bars        │ events│ commands      │ jobs
        │               │               │             │       │               │
 ┏━━━━━━▼━━━━━━━━━━━━━━━▼━━━━━━━━━━━━━━━┷━━━━━━━━━━━━━▼━━━━━━━┷━━━━━━━━━━━━━━━┷━━━━━━━━━━━━━━━━┓
 ┃ REDIS 8                                                                                     ┃
 ┃ Streams   ordered · acked · replayable                                                      ┃
 ┃           md:bar:{venue}:{sym}:{tf}    md:status:{venue}    evt:session:{id}                ┃
 ┃           cmd:session:{id}             jobs:research                                        ┃
 ┃ Pub/Sub   latest-wins fan-out          live:bar:{venue}:{sym}:{tf}  live:book:{venue}:{sym} ┃
 ┃ Keys      md:*:last:*  md:heartbeat:{venue}  lock:session:{id}  cache:* (TTL · evictable)   ┃
 ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┯━━━━━━━━━━━━━━━━━━━━━━━━━▲━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
                                      │ XREAD · SUBSCRIBE       │ XADD commands
                                      │                         │
                          ┌───────────▼─────────────────────────┴──────────┐
                          │ API  ·  FastAPI  ·  stateless, N replicas      │
                          │ REST  queries · commands · stream tickets      │
                          │ WS    multiplexed topics  ·  SSE fallback      │
                          └───────────┬─────────────────────────▲──────────┘
                                      │ WS · SSE  seq-numbered  │ HTTPS
                                      │                         │
                          ┌───────────▼─────────────────────────┴──────────┐
                          │ WEB  ·  Next.js · Zustand · Lightweight Charts │
                          └────────────────────────────────────────────────┘

 ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
 ┃ TIMESCALEDB  (PostgreSQL 17)     schemas: market · trading · research · ref                 ┃
 ┃ ◄ writes   INGESTOR COPY micro-batches · ENGINE orders, fills, equity · WORKER runs         ┃
 ┃ ► reads    WORKER history for backtests · API queries · ENGINE warm-up bars                 ┃
 ┃ (every process talks to TimescaleDB directly; arrows omitted to keep the bus readable)      ┃
 ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
```

---

## 4. Event model

### Inside an engine

Events are frozen dataclasses (`application/execution/events.py`): market data
(`BarClosed`, `BookUpdated`, `FeedHealthChanged`), venue results (`OrderAccepted`,
`OrderSubmitFailed`, `OrderStateReported`, `TradeReported`, `OrderNotFound`, `CancelFailed`,
`BalancesReported`, `VenueFailed`), control (`CommandReceived`, `Tick`, `LeaseLost`,
`PersistenceDegraded`/`Restored`, `ShutdownRequested`). The `TradingCore` handles one at a
time and answers with **effects** — `Persist*`, `SubmitOrder`, `CancelOrder`, `QueryOrder`,
`QueryTrades`, `PublishEvent`, `StopSession` — the only way anything leaves it (§5).

### On the wire

A session publishes what happened on `evt:session:{id}` (Stream: the replayable tail) and
`live:session:{id}` (Pub/Sub: UIs), best effort — the database is the record (§11):

```
{"v": 1, "type": "session_event", "session": "mom-1", "kind": "fill",
 "data": {…}, "sid": "<engine instance>", "seq": 42}
```

`sid`/`seq` let a subscriber detect dropped messages and engine restarts.

### Envelope (M7)

When backtests move onto the core, every event in every mode gets one immutable envelope, so
a fill can be traced to the bar that caused it:

```
Envelope
  event_id        UUIDv7           time-ordered, globally unique
  event_type      str              "market.bar.closed", "order.filled", …
  session_id      str              backtest run id, or paper/live session id
  seq             int              strictly increasing per session → consumers detect gaps
  ts_event        int  ns, UTC     when it happened at the venue / in the market
  ts_init         int  ns, UTC     when this process created it → latency = ts_init − ts_event
  correlation_id  UUIDv7           root of the causal chain (the bar that started it)
  causation_id    UUIDv7 | None    direct parent event
  payload         typed event      frozen dataclass from domain/events
```

`correlation_id` and `causation_id` will let a single fill be traced back through its order,
risk decision and signal to the exact bar that triggered it.

### Catalog

| Event type | Emitted by | Consumed by | Carried on |
| --- | --- | --- | --- |
| `market.bar.closed` | Ingestor · gap filler (`recovered`) | Engine (`RedisTradingFeed`) · storage · UI | `md:bar:*` + `live:bar:*` |
| `market.bar.updated` (forming) | Ingestor | UI | `live:bar:*` + `md:bar:last:*` |
| `market.book.snapshot` | Ingestor (conflated top 25) | UI · Engine (PaperVenue fills, marks) | `live:book:*` + `md:book:last:*` |
| stream state change (healthy · stale · reconnecting · failed …) | Ingestor | alerts | `md:status:{venue}` |
| ingestor heartbeat | Ingestor | Engine (an unhealthy feed blocks new risk) · UI · alerts | `md:heartbeat:{venue}` (expiring key) |
| `market.trade` *(M7)* | Ingestor | PaperVenue · UI tape | `md:trade:*` |
| operator command `halt` · `flatten` · `resume` · `stop` | `cq trade`, the web terminal (M4) | Engine inbox, in order, applied once | `cmd:session:{id}` |
| session `session` — `started` · `stopping` | Engine | UI · alerts | `evt:session:{id}` + `live:session:{id}` |
| session `order` — every state change, with the reject reason | OrderManager | UI · blotter | same |
| session `fill` — price, quantity, fee, liquidity | OrderManager | UI · tape | same |
| session `equity` — equity, cash, gross exposure, drawdown, day PnL per base bar | Ledger | UI · equity curve | same |
| session `risk` — `rejected` · `adjusted` · `breach` · `halt` · `resume` · `anomaly` · `market_data` | RiskManager · reconciliation | UI · alerts · audit | same (and `trading.risk_events`) |
| session `flatten` — `started` · `completed` · `incomplete` (what was left open) · `ended` | Engine | UI · alerts | same |

---

## 5. Hot path inside one engine (paper, sandbox and live)

```
      RedisTradingFeed              venue workers + stream          commands · timer · lease
  md:bar:* · md:book:last:*         acks · trades · states           halt · flatten · resume
 md:heartbeat → feed health           rejects · not found            stop · Tick · LeaseLost
              │                                │                                │
┏━━━━━━━━━━━━━▼━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━▼━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━▼━━━━━━━━━━━━┓
┃ INBOX   priority: control > venue > timer > market · one event at a time                   ┃
┃         the feed yields before its next read, so market data never starves commands        ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┯━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
                                               │
┌──────────────────────────────────────────────▼─────────────────────────────────────────────┐
│ TradingCore.handle(event) → effects       synchronous · no I/O · time arrives with events  │
│                                                                                            │
│ ① BarClosed (base 1m) ─► strategy timeframe bar ─► Strategy.on_bar ─► signal s             │
│ ② sizer (fixed · Kelly · vol parity) ─► target = s × allocation × equity                   │
│      same signal and drift inside the rebalance band → nothing to do                       │
│ ③ delta = target − (position + working orders); working orders → cancel first              │
│ ④ RiskManager.check: halts · kill switch · data age · price band · order rate ·            │
│      open orders · position / gross / leverage / order caps · funds                        │
│      → approve | trim | reject (every trim and refusal is a risk event)                    │
│ ⑤ OrderManager.create   client id = H(session, intent) — refused if already known          │
│      ─► PersistOrder (PENDING) ─► SubmitOrder (held until that row is durable)             │
│ ⑥ venue report ─► OrderManager (forward-only FSM; a fill counts once per trade id)         │
│      ─► Ledger ─► equity mark ─► RiskManager.mark ─► drawdown / daily-loss breach          │
│      ─► halt new risk ─► flatten (reduce-only market orders) or cancel only                │
└──────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                               │
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━▼━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ EFFECTS                                                                                    ┃
┃   Persist*        → writer: one transaction per batch, fenced by session ownership         ┃
┃   SubmitOrder     → submit worker, only once its PENDING row committed (else NOT_SENT)     ┃
┃   Cancel · Query  → control worker, cancels before lookups; results re-enter the INBOX     ┃
┃   PublishEvent    → evt:session:{id} + live:session:{id} (best effort, never blocks)       ┃
┃   StopSession     → cancel working orders, settle, persist state, release the lease        ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
```

The core never waits: slow work (venue calls, database writes, publication) happens in the
runner's tasks and comes back as events. Venue calls run on two workers so a stuck submission
can never delay a cancel, and a rate-limited call is retried with backoff before it counts as
a failure. Model inference (M6) will follow the same rule — an executor whose result re-enters
the inbox.

---

## 6. One engine, three modes

```
    ┌────────────────────────────────────────────────────────────────────────────────────┐
    │ TradingSession ─► ExecutionRunner ─► TradingCore — identical code in every mode    │
    │ Strategy · Sizer · RiskManager · OrderManager · Ledger                             │
    └────────────────────────────────────────────────────────────────────────────────────┘

┌─────────┬────────────────────────────┬──────────────────────────┬──────────────────────────┐
│ mode    │ ExecutionVenue             │ instrument specs         │ may start when (D10)     │
├─────────┼────────────────────────────┼──────────────────────────┼──────────────────────────┤
│ paper   │ PaperVenue — fills against │ the venue's public       │ always; no credentials   │
│         │ the live book, else the    │ market metadata          │ are read                 │
│         │ last close + slippage      │                          │                          │
├─────────┼────────────────────────────┼──────────────────────────┼──────────────────────────┤
│ sandbox │ CcxtExecutionVenue on the  │ the venue's markets      │ CQ_TRADING_MODE ≥ sandbox│
│         │ venue's test network       │ (spot only)              │ · spot · API keys        │
├─────────┼────────────────────────────┼──────────────────────────┼──────────────────────────┤
│ live    │ CcxtExecutionVenue on the  │ the venue's markets      │ both keys · sandbox off ·│
│         │ real account               │ (spot only)              │ USD hard caps · USD quote│
│         │                            │                          │ · key cannot withdraw    │
└─────────┴────────────────────────────┴──────────────────────────┴──────────────────────────┘
  shared by every mode: RedisTradingFeed (md:*) · PostgresTradingStore (trading.*) ·
  Redis commands, lease and session events · SystemClock · Prometheus cq_trade_*
  backtest: the M2 engines run the same strategies; M6 moves them onto TradingCore
```

The parity suite (tests marked `parity`) runs one strategy through the vectorized engine and
the event-driven backtester and asserts identical signals, fills, trades and equity (to 1e-12)
under the vectorized engine's assumptions. The paper venue follows the backtester's fill rules
(a resting limit fills only when the market trades *through* it; market orders pay slippage),
and M7 closes the loop: the backtester's price paths and cost models become a `SimulatedVenue`
behind the same `ExecutionVenue` port, and a paper session replayed from recorded data must
match its backtest.

---

## 7. Order lifecycle

```
                     ack                   fill                    fill
   PENDING ───────────────► OPEN ───────────────► PARTIALLY_FILLED ───────────────► FILLED
    │   │                    │                           │                            ▲
    │   │ timeout or         │ cancel()                  │ cancel()                   │
    │   │ lost connection    ▼                           ▼                            │
    │   │              CANCEL_PENDING ───────────────────────────────────► CANCELLED  │
    │   │                    └── fills keep counting; the last one completes it ──────┘
    │   ▼
    │  UNKNOWN ── look up by client order id ──► OPEN | PARTIALLY_FILLED | FILLED | CANCELLED
    │                                            | REJECTED | EXPIRED
    ▼
   REJECTED   venue refusal · NOT_SENT (its PENDING write failed); a venue can also refuse
              an order it acknowledged, so OPEN · PARTIALLY_FILLED · CANCEL_PENDING → REJECTED

   terminal     FILLED · CANCELLED · REJECTED · EXPIRED (time in force ran out, e.g. FOK)
   forward-only a report can only move an order forward; a stale one is ignored
   idempotent   a fill is keyed (session, venue, symbol, trade_id) and applied once
   never twice  an UNKNOWN order is reconciled, never re-submitted (invariant 5)
   not orders   pre-trade refusals are risk events (trading.risk_events), not REJECTED orders
```

Client order ids are deterministic: `cq` + a 6-character session tag + 24 characters of
`H(session, intent)`, where the intent names the decision (instrument, decision bar, purpose,
attempt). Replaying a decision after a crash derives the same id, finds it already recorded and
submits nothing; the venue deduplicates on the id as a second line of defence (a duplicate-id
error is treated as *uncertain* and looked up, never as a rejection); reconciliation finds the
order by it; and the session tag tells this session's orders apart from everything else on the
account.

---

## 8. Research promotion funnel

```
┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐
│ 1 BACKFILL        │    │ 2 FEATURES        │    │ 3 VECTOR SWEEP    │    │ 4 EVENT CONFIRM   │
│ CCXT REST → TS    │───►│ Polars lazy       │───►│ Polars + NumPy    │───►│ same engine as    │
│ gap + outlier     │    │ closed bars only  │    │ 10⁴–10⁶ configs   │    │ live; parity      │
│ audit             │    │ shift-guarded     │    │ fill @ next open  │    │ vs sweep ≤ ε      │
└───────────────────┘    └───────────────────┘    └───────────────────┘    └─────────┬─────────┘
                                                                                     │
                                                                                     │
┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐    ┌─────────▼─────────┐
│ 8 LIVE            │    │ 7 PAPER           │    │ 6 ROBUSTNESS      │    │ 5 WALK-FORWARD    │
│ two-key armed     │◄───│ ≥ N days live     │◄───│ Monte Carlo       │◄───│ rolling IS/OOS    │
│ notional caps     │    │ data; drift vs    │    │ DSR · PBO         │    │ purge + embargo   │
│ kill switch       │    │ backtest ≤ tol    │    │ param stability   │    │ OOS-only stats    │
└───────────────────┘    └───────────────────┘    └───────────────────┘    └───────────────────┘

Every gate writes a verdict row to research.promotions. A strategy reaches LIVE only
with a passing verdict at all seven prior gates for the same code hash + config hash.
```

Sweeps test thousands of configurations, so raw Sharpe ratios are inflated by multiple testing.
Gate 6 applies the Deflated Sharpe Ratio (corrected for the number of trials actually run) and
the Probability of Backtest Overfitting (combinatorially symmetric cross-validation). Walk-forward
windows are purged and embargoed so labels that overlap a split boundary cannot leak.

---

## 9. Streaming to the UI

```
┌────────────────────────────┐   ┌─────────────────────────────────────────────────────────────┐
│ Pub/Sub  live:bar:*:1m     │   │ GATEWAY  TopicHub · one feed per topic, shared by           │
│ Pub/Sub  live:book:*       │──►│          every connection that subscribes to it             │
│ Key      md:heartbeat:*    │   │ candles:{venue}:{sym}:{tf}  1m bars folded into tf          │
│ Stream   evt:session:*     │   │          candles; a closed candle is checked against        │
└────────────────────────────┘   │          storage and re-snapshotted if it differs           │
                                 │ book · health    latest-wins, conflated per key             │
┌────────────────────────────┐   │ session:{id}     DB snapshot + evt:session tail;            │
│ TimescaleDB                │   │          reliable: backlog overflow ⇒ fresh snapshot        │
│ candle history · sessions  │──►│ every topic      snapshot, then {topic, seq, kind, data}    │
│ orders · fills · equity    │   │ auth             bearer token ⇒ single-use stream ticket    │
│ risk events                │   └──────────────────────────────┬──────────────────────────────┘
└────────────────────────────┘                                  │
                                                                │ WS · SSE fallback · JSON
                                                                │
┌───────────────────────────────────────────────────────────────▼──────────────────────────────┐
│ BROWSER  ·  Next.js · Zustand · Lightweight Charts                                           │
│ StreamClient  one connection · ticket per connect · topics multiplexed                       │
│               WebSocket; SSE after two failed handshakes · backoff with jitter               │
│               watchdog: silence for 2.5 heartbeats ⇒ reconnect                               │
│               seq gap ⇒ resubscribe (WS) / reconnect (SSE) ⇒ fresh snapshot                  │
│ stores        market: candles (versioned) · books · health                                   │
│               session: snapshot + idempotent deltas (order version, fill id)                 │
│ chart         store subscription outside React: series.update(candle)                        │
│               fills ⇒ buy/sell markers · working orders and entry ⇒ price lines              │
│ metrics       positions marked to book mids at most once a second                            │
│ REST          catalog · older candles when scrolled back · operator commands                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

The gateway (`cq api serve`) is stateless: everything it streams is rebuilt from Redis and
TimescaleDB, so replicas can sit behind any load balancer. Every topic starts with a
**snapshot** and continues with **sequence-numbered updates**; a client never applies an update
to state it did not get a snapshot for (invariant 15). Two delivery policies keep slow clients
from hurting fast ones:

| Topic | Policy | When a client falls behind |
| --- | --- | --- |
| `candles:{venue}:{sym}:{tf}` · `book:{venue}:{sym}` · `health:{venue}` | latest wins | updates are conflated per key (a candle's open time, the book); beyond 256 pending keys the oldest are dropped |
| `session:{id}` | reliable | nothing is dropped; past 5 000 pending updates the backlog is replaced by a fresh snapshot of that topic |

Only the lagging topic of the lagging connection is re-snapshotted; the connection stays open.
A feed that fails (Redis or the database unreachable) is restarted with backoff while its
subscribers keep their last state; an unknown session or an unserved market is an `error` frame
for that topic alone. §15 has the protocol and the operating limits.

---

## 10. Redis keyspace

| Key | Type | Retention | Purpose |
| --- | --- | --- | --- |
| `md:bar:{venue}:{sym}:{tf}` | Stream | `MAXLEN ~ 10000` | closed bars, live and recovered → engines (the buffer behind Pub/Sub) |
| `md:bar:last:{venue}:{sym}:{tf}` | String | TTL 2 bars | latest bar, for clients that join between updates |
| `live:bar:{venue}:{sym}:{tf}` | Pub/Sub | none | every bar update, forming and closed |
| `live:book:{venue}:{sym}` | Pub/Sub | none | conflated top-25 snapshots, ≤ 10 Hz |
| `md:book:last:{venue}:{sym}` | String | TTL 30 s | latest snapshot for late joiners |
| `md:status:{venue}` | Stream | `MAXLEN ~ 1000` | stream state transitions |
| `md:heartbeat:{venue}` | String | TTL 3 × heartbeat | ingestor liveness: absent means the ingestor is down |
| `md:trade:{venue}:{sym}` *(M7)* | Stream | `MAXLEN ~ 100000` | prints → PaperVenue, trade tape |
| `evt:session:{id}` **(M3)** | Stream | `MAXLEN ~ 10000` | session events, replayable tail for UIs |
| `live:session:{id}` **(M3)** | Pub/Sub | none | the same events, pushed |
| `cmd:session:{id}` **(M3)** | Stream | `MAXLEN ~ 1000` | operator commands; the engine stores the last applied offset, so each applies once, across restarts |
| `jobs:research` | Stream + consumer group | acked, then trimmed | backtest / sweep / walk-forward jobs |
| `lock:session:{id}` **(M3)** | String, `SET NX PX` | 10 s lease, renewed every 3 s (Lua compare-and-renew) | one engine per session; the value names the owning instance. The fence itself lives in `trading.sessions.fence` |
| `api:ticket:{sha256}` **(M4)** | String, `SET NX PX` | `CQ_API_TICKET_TTL_S` (30 s) | single-use stream tickets, stored as a hash of the ticket and consumed with `GETDEL` |
| `cache:*` | String / Hash | always has a TTL | the only evictable keys |

`{sym}` is the symbol slug (`BTC/USDT` → `BTC-USDT`, `BTC/USDT:USDT` → `BTC-USDT-USDT`).
Messages are JSON with a schema version, **millisecond** timestamps (nanosecond integers exceed
JavaScript's safe-integer range) and a per-channel `sid`/`seq` pair, so a subscriber can detect
dropped messages and publisher restarts.

Redis runs with `maxmemory-policy volatile-lru`. Only keys with a TTL (the cache) can be evicted,
so streams are never silently dropped. If memory fills with streams, writes fail loudly instead.

---

## 11. TimescaleDB layout

| Schema | Object | Kind | Notes |
| --- | --- | --- | --- |
| `ref` | `instruments` | table, versioned by `valid_from` | precision, lot, tick, min notional and fees as of a date, so old backtests use old rules |
| `market` | `bars_1m` **(M1)** | hypertable, 7-day chunks; plain table on PostgreSQL | PK `(venue, symbol, ts)`; `source` 1 = WebSocket, 2 = REST; CHECKs for alignment and OHLC consistency; compressed after 14 days, segmented by `(venue, symbol)` |
| `market` | `data_gaps` **(M1)** | table | gap-fill audit; ranges marked `unfillable` (venue has no data) are skipped by later detection |
| `market` | `bars_5m` … `bars_1d` (M5) | continuous aggregates | derived from `bars_1m` with refresh policies (D7); until then backtests aggregate on read with `date_bin` |
| `market` | `trades` (M3) | hypertable, 1-day chunks | optional; 30-day retention |
| `trading` | `sessions` **(M3)** | table | configuration (`SessionConfig.describe()`, including Kelly inputs), `status`, `fence`, restart state (JSONB: high-water mark, day start, halts, acted signals, command offset) |
| `trading` | `orders` **(M3)** | table | latest version of every order, keyed by client order id; replaced only by a higher `version`; written *before* the order is sent |
| `trading` | `fills` **(M3)** | table | every execution once, PK `(session_id, venue, symbol, trade_id)`; positions and cash are rebuilt from these rows |
| `trading` | `risk_events` **(M3)** | table | audit trail: refusals and trims, breaches, halts, resumes, reconciliation anomalies |
| `trading` | `equity` **(M3)** | hypertable (plain table on PostgreSQL) | equity, cash, gross exposure, drawdown and day PnL per base bar |
| `research` | `runs` · `run_metrics` · `run_trades` · `run_equity` | tables + hypertable | backtests and sweeps |
| `research` | `experiments` · `promotions` | tables | funnel gates and verdicts |

Schema history lives in `public.cq_schema_migrations` (version, name, SHA-256, duration).

Two database roles: `cq_owner` owns the schemas and runs migrations; `cq_app` is what the running
services use, with DML-only rights granted through default privileges. A compromised API process
cannot drop a table.

---

## 12. Market data ingestion (Module 1)

```
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│ VENUE   ccxt.pro WebSocket: watch_ohlcv · watch_order_book     CCXT REST: fetch_ohlcv        │
└───────────────────┬─────────────────────────────────────────────────────────┬────────────────┘
                    │                                                         │
                    │ 1m candles                                              │ L2 deltas → book
                    │                                                         │
┌───────────────────▼──────────────────────────────────┐    ┌─────────────────▼────────────────┐
│ BAR STREAM  (one task per symbol)                    │    │ BOOK STREAM  (one per symbol)    │
│ ResilientStreamRunner   backoff · health · reconnect │    │ ResilientStreamRunner + health   │
│ CandleTracker  closes a candle when the next opens,  │    │ top-N copy, crossed = rejected   │
│                withholds one cut by a disconnect     │    │ LatestByKey conflation           │
└─────────┬──────────────────────────┬────────────────┬┘    └───────────────────┬──────────────┘
          │                          │                │                         │
          │ closed                   │ gaps           │ bars                    │ every 100 ms
          │                          │                │                         │
┌─────────▼────────────┐   ┌─────────▼──────────┐   ┌─▼─────────────────────────▼──────────────┐
│ BarWriteBuffer       │   │ GapFillScheduler   │   │ RedisMarketDataPublisher                 │
│ batch · retry        │   │ coalesce requests  │   │ one pipelined round trip per batch       │
│ overflow → REST fill │   │ 1 fill per symbol  │   │ best-effort: never blocks ingestion      │
└─────────┬────────────┘   └─────────┬──────────┘   └─────────────────────┬────────────────────┘
          │                          │                                    │
          │                          │                                    │
┏━━━━━━━━━▼━━━━━━━━━━━━┓   ┌─────────▼──────────┐   ┏━━━━━━━━━━━━━━━━━━━━━▼━━━━━━━━━━━━━━━━━━━━┓
┃ TIMESCALEDB          ┃   │ GapFillService     │   ┃ REDIS                                    ┃
┃ market.bars_1m       ┃◄──│ SQL gap scan       │   ┃ XADD     md:bar:*          (closed bars) ┃
┃ upsert: REST > WS    ┃   │ REST pages→upsert  │   ┃ PUBLISH  live:bar:*  live:book:*         ┃
┗━━━━━━━━━━━━━━━━━━━━━━┛   └────────────────────┘   ┃ SET PX   md:bar:last:*  md:book:last:*   ┃
                                                    ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│ BACKGROUND LOOPS                                                                             │
│ watchdog  1 s    stream silent past threshold → STALE + status event → venue reconnect       │
│ heartbeat 5 s    SET md:heartbeat:{venue} PX 15 s; the key vanishes if the ingestor dies     │
│ reconcile 5 min  re-check the last 2 h against REST; catches anything the stream missed      │
│ startup   once   fill the downtime window (default 7 days) before streaming                  │
│ shutdown         SIGTERM → stop streams → drain write buffer → close venue, Redis, pool      │
│ rule             only closed bars are stored; REST values replace WebSocket values           │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Resilience, layer by layer

| Failure | Detected by | Response |
| --- | --- | --- |
| Dead TCP connection, lost pongs | ccxt.pro ping every `CQ_INGEST_WS_KEEPALIVE_MS` (15 s) | connection failed; pending watches raise; next watch reconnects and resubscribes |
| Stream error (network, 5xx, maintenance, rate limit) | `ResilientStreamRunner` | `RECONNECTING`, jittered exponential backoff (0.5 s → 30 s), venue `Retry-After` honoured; attempt counter resets after 60 s healthy |
| Socket alive but silent | watchdog, per-stream staleness threshold | `STALE` + status event, then one venue-wide reconnect per cooldown (120 s) |
| Candle closed while disconnected | `CandleTracker` (uncertain flag) | the bar is withheld from the stream and fetched from REST instead |
| Skipped candles / downtime | tracker jump detection, startup window, 5-min reconcile | `GapFillScheduler` → REST pages → upsert; recent recovered bars also go to the bus flagged `recovered` |
| Venue has no data (maintenance, pre-listing) | empty REST page or holes between returned bars | recorded as `unfillable`; not re-requested unless `--recheck-unfillable` |
| Database outage | `TransientStorageError` from the repository | write buffer keeps bars and retries forever; on overflow the oldest bars are dropped *and* scheduled for REST refill |
| Redis outage | publisher | counted and logged (rate-limited); ingestion and storage continue |
| Unknown symbol, geo-block, library incompatibility | CCXT error translation | fatal: the stream (or startup) fails with a clear message and exit code 2 — never an endless retry |

### Operations

```
make migrate                                   # apply SQL migrations (owner role)
make ingest SYMBOLS="BTC/USDT ETH/USDT"         # live pipeline; Ctrl-C / SIGTERM stops cleanly
make backfill START=2020-01-01                 # history for CQ_INGEST_SYMBOLS, resumable
make gaps START=2024-01-01                     # report missing bars; exit 1 if any
```

Prometheus metrics (`cq_ingest_*`) are served on `CQ_METRICS_PORT`: update and close counts,
close-detection lag, write buffer size, reconnects, stream state, gaps, backfilled and
unfillable bars, invalid payloads and publish failures.

---

## 13. Backtesting engine (Module 2)

```
  cq backtest run │ sweep │ strategies        entrypoints/cli ──▶ bootstrap/backtest.py
          │                                   (strategy registry · history source · artifact root)
          ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │ BacktestService                                                 application/backtest │
  │ align window → load history → run engine off the event loop → warnings → manifest    │
  └───────┬─────────────────────────────┬──────────────────────────────────┬─────────────┘
          │ load()                      │ run() · sweep()                  │ save()
          ▼                             ▼                                  ▼
  ┌────────────────────┐   ┌─────────────────────────────────────┐   ┌─────────────────────┐
  │ BarHistoryReader   │   │ EventDrivenBacktester               │   │ ArtifactStore       │
  │ · Postgres         │   │   bar loop over OHLC or tick paths  │   │   manifest.json     │
  │   date_bin 1m → tf │   │   latency · market / limit + TTL    │   │   metrics.json      │
  │ · CSV .gz .zip     │   │   maker/taker fees · slippage model │   │   equity.csv  (PnL) │
  │   NumPy resample   │   ├─────────────────────────────────────┤   │   trades · fills    │
  └─────────┬──────────┘   │ VectorizedBacktester                │   │   sweep · skipped   │
            │ MarketFrame  │   Polars: k parameter sets, 1 query │   └─────────────────────┘
            └─────────────▶│   NumPy: fixed units, loop / trade  │
                           ├─────────────────────────────────────┤
                           │ SimPortfolio → compute_performance  │
                           │   Sharpe · Sortino · MaxDD · Calmar │
                           │   CAGR · win rate · PnL curve       │
                           └─────────────────────────────────────┘
```

### Two engines, one set of semantics

| | Event-driven — `EventDrivenBacktester` | Vectorized — `VectorizedBacktester` |
| --- | --- | --- |
| Purpose | fidelity: execution research, promotion decisions | breadth: parameter sweeps, screening |
| Strategy input | `Strategy.on_bar`, once per closed bar | `VectorizedStrategy.vector_exposure`, one Polars expression per parameter set |
| Orders | market, or limit with TTL, penetration and marketable-on-arrival → taker; lot size; minimum notional | market, at the next open |
| Timing | latency with seeded jitter; fills on an intrabar path (synthetic OHLC or replayed ticks) | none (next open) |
| Costs | maker/taker fees; fixed or square-root-impact slippage | taker fee; fixed slippage |
| Cost of a run | a Python loop per bar (≈ 10 µs/bar) | one shared Polars query per batch of parameter sets, then a loop per *trade* |

Both engines size an order at the decision close as `target × equity / close`, hold the
position in units until the target changes (exposure drifts with price, as in a real account),
and book fills through the same `SimPortfolio`. Both apply the same ruin rule: a close that
marks equity at or below zero liquidates the position at that close and ends trading, instead
of sizing from negative equity. Under the vectorized engine's assumptions the two are
identical: the parity tests assert equal fills, trades and equity to 1e-12. Settings
the vectorized engine cannot honour (latency, limit orders, impact slippage, lot sizes) are
listed as warnings in the run manifest instead of being silently dropped.

### Writing a strategy

A strategy maps closed bars to a **target exposure**: the fraction of equity to hold, `+1` fully
long, `-1` fully short, `0` flat, `None` keep the current target (hysteresis without state).
Engines clamp it to the configured leverage and, for spot accounts, to `>= 0`; they own sizing,
execution, costs and accounting.

```python
@dataclass(frozen=True, slots=True)
class MeanReversionParams(StrategyParams):
    lookback: int = 48
    entry_z: float = 2.0
    exit_z: float = 0.5

class MeanReversion(VectorizedStrategy[MeanReversionParams]):
    name = "mean_reversion"
    params_type = MeanReversionParams

    def on_bar(self, bar, ctx):                    # event engine, and later paper/live
        self._stats.update(bar.close)              # streaming, identical to Polars rolling_*
        ...                                        # → 1.0 / -1.0 / 0.0 / None

    @classmethod
    def vector_exposure(cls, params) -> pl.Expr:   # optional: unlocks fast sweeps
        z = (pl.col("close") - pl.col("close").rolling_mean(params.lookback)) / ...
        return pl.when(z <= -params.entry_z).then(1.0)...
```

`strategies/mean_reversion.py` and `strategies/momentum.py` are the complete references. A test
replays `on_bar` over synthetic data and asserts the Polars expression produces the same
decision on every bar. New strategies are registered in `bootstrap/backtest.py`.

### One bar in the event-driven engine

```
  open_k                                                    close_k = open_k+1
    │◀───────────────── bar k: intrabar price path ────────────▶│◀──── bar k+1 ────▶
    │  1. orders arriving during bar k fill against the path    │
    │     market: path price at arrival + slippage, taker fee   │
    │     limit : its own price once the path trades through    │
    │             it, maker fee (marketable on arrival → taker) │
    │                                      2. mark equity       │
    │                                      3. on_bar → target   │
    │                                      4. target changed?   │
    │                                         size the order    │
    │                                         at close_k ───────┼──▶ arrives at close_k + latency;
    │                                                           │    fills in step 1 of the bar
    │                                                           │    it lands in

  OHLC path (no ticks): open → low → high → close on an up bar, open → high → low → close
  on a down bar, at 0, ⅓, ⅔ and 1 of the bar. A decision at close_k can only fill on prices
  after close_k + latency: look-ahead is impossible by construction.
```

### Metrics

Computed from the equity curve sampled at every bar close (`r` = per-bar returns, `N` = bars per
year — 365 days for crypto — and `rf` the per-bar risk-free rate):

| Metric | Definition |
| --- | --- |
| Total return · CAGR | `E_end / E_start − 1` · `(E_end / E_start)^(N / bars) − 1` |
| Sharpe | `mean(r − rf) / stdev(r − rf) · √N` (sample stdev) |
| Sortino | `mean(r − rf) / √mean(min(r − rf, 0)²) · √N` (downside deviation over all bars) |
| Max drawdown | `min(E / running_max(E) − 1)`, plus the longest time under water in bars |
| Calmar | `CAGR / |max drawdown|` |
| Win rate | winning round trips / closed round trips (a trade runs flat → flat; a flip closes one and opens the next) |
| Trade stats | profit factor, expectancy, average win and loss, best, worst, average bars held — returns measured against equity at entry |
| PnL curve | equity, cumulative PnL, drawdown and exposure at every bar close (`equity.csv`) |
| Exposure · turnover | share of bars with a position · annualised notional traded per unit of average equity |

Ratios with a zero denominator are reported as `None`, never ±∞. A ruined account reports no
Sharpe, Sortino or Calmar (returns past the ruin would divide by non-positive equity). Sweeps
rank combinations that never held a position or wiped out the account last, whatever the
metric, and warn about multiple testing once ten or more combinations are ranked.

### Data, reproducibility and artifacts

`PostgresBarHistory` aggregates `market.bars_1m` in the database with `date_bin` — epoch-aligned
buckets, identical to the NumPy `resample` used for CSV input (an integration test compares
them bar for bar) — and returns a single row of column arrays, so multi-year 1-minute history
loads without materialising a record per bar. Buckets built from incomplete minutes are counted
as *partial*; buckets with no minutes at all are *missing*. Both appear as warnings in the
manifest. `CsvBarHistory` reads Binance dumps (header-less, millisecond or microsecond epochs)
and headed files with ISO or epoch times, `.csv`, `.csv.gz` or `.zip`.

Each run writes `artifacts/backtests/<run_id>/`: `manifest.json` (code version, strategy and
parameters, every cost and latency setting, seed, data source, window and content hash,
warnings), `metrics.json`, `equity.csv`, `trades.csv` and `fills.csv`; a sweep writes
`sweep.csv` and `skipped.csv`. Run ids read `20260928T101500Z-mean_reversion-3f9a1c2b0d`: the
suffix fingerprints the inputs, so two runs with the same suffix used the same code path,
settings and data.

### Operations

```
cq backtest strategies                                         # parameters, defaults, engines
cq backtest run mean_reversion -s BTC/USDT -t 1h --start 2023-01-01 --end 2024-01-01 \
    -p lookback=48 -p entry_z=2.0
cq backtest run momentum -s BTC/USDT -t 4h -p trend_window=50 \
    --latency-ms 150 --jitter-ms 50 --order-type limit --limit-ttl-bars 2 --slippage sqrt
cq backtest sweep mean_reversion -s BTC/USDT -t 1h -g lookback=24:96:12 \
    -g entry_z=1.5:3:0.25 -g exit_z=0,0.5 --rank sortino --top 20
cq backtest run momentum -s BTC/USDT -t 1h --data ~/data/BTCUSDT-1m-2024-01.zip    # no database
make backtest STRATEGY=momentum SYMBOL=BTC/USDT PARAMS="lookback=24 threshold=0.01"
```

Exit code 2 means the request or the data is wrong (unknown strategy, invalid parameter, empty
window, malformed file); 1 means a runtime failure — database, disk or network — worth a retry.

---

## 14. Execution & risk management (Module 3)

The hot path is in §5, the modes in §6 and the order lifecycle in §7. This section covers what
surrounds them: how a session starts, stops and survives restarts, the risk controls, sizing,
and the failure handling that keeps the database, the engine and the venue in agreement.

### A session's life

```
cq trade run mom-1 ─► arming checks (D10) ─► lease lock:session:mom-1 ─► claim: fence + 1
   ─► restore orders and fills ─► rebuild the ledger from the fills
   ─► restore stops, halts, acted signals and the command offset
   ─► warm the strategy up from stored bars ─► reconcile every unfinished order by client id
   ─► first decision on the next closed bar
```

A session is created by the first `cq trade run` that names a strategy and symbols; its
configuration is stored and wins from then on. A later run with a different configuration is
refused with the differing keys listed; creation options passed when resuming are refused
rather than ignored. **Stopping** (`cq trade stop`, Ctrl-C, SIGTERM) refuses new orders, cancels
working ones, waits for the venue to confirm, persists the state, marks the session `stopped`
and releases the lease. A second signal skips the graceful part; that is safe because the venue
never knows an order the database does not (invariant 11).

**Restarting** resumes where the session stopped. Positions come back from the fills, and the
signal each instrument last acted on is stored, so a strategy that still wants the position it
already has trades nothing. Decisions about the session survive a restart — drawdown stop, daily
loss limit (until UTC midnight), kill switch, reconciliation halt — while conditions of one
process do not (shutdown, lost lease, storage outage, venue refusal).

### Risk controls

| Control | Fires when | Default | Then |
| --- | --- | --- | --- |
| Drawdown stop | equity ≤ (1 − `max_drawdown`) × high-water mark | 20 % | halt new risk; `flatten` closes every position with reduce-only market orders, `halt` only cancels working orders; latched until `cq trade resume`, which rebases the high-water mark |
| Daily loss limit | equity ≤ (1 − `daily_loss_limit`) × equity at the UTC day's start | 5 % | same actions; lifts at the next UTC midnight (or on resume) |
| Kill switch | `cq trade halt` · `cq trade flatten` | — | cancel working orders and block new risk (and close everything, for flatten) until resume |
| Hard caps | `CQ_MAX_ORDER_NOTIONAL_USD` · `CQ_MAX_GROSS_EXPOSURE_USD` | required for live | tighten every session's own limits at start; never stored, so a session cannot carry a looser cap past a deployment change |
| Reconciliation | a fill carrying this session's id for an order it does not know, an order of this session on the venue that it did not track, a working order the venue no longer knows, a cancelled order that comes back, balances that disagree with the ledger twice in a row | — | halt new risk and record an anomaly; an operator resumes after checking |
| Feed health | ingestor heartbeat missing or a stream unhealthy; no closed bar for too long; reference price older than 3 min | — | no new risk on that instrument (on every instrument when the whole feed is down); exits and cancels still go out (invariant 7) |

Every order passes `RiskManager.check` before it exists, in this order: a valid quantity; no
short sale on spot (or without `allow_short`); a limit price within the price band (5 %) of the
reference; the quantity rounded down to the lot, and refused below the venue minimum. Orders
that **add** risk must also clear the kill switch and every halt, a fresh reference price, the
order rate (30 a minute) and open-order (20) limits and positive equity; they are then **trimmed**
to the position, gross-exposure, leverage and per-order caps and to the funds available (spot
buying power, keeping 0.2 % for fees). Orders that **reduce** a position skip the halt and
data-quality gates, so a halted or data-starved session can always get flat. Every refusal and
every trim is a risk event: published, persisted in `trading.risk_events` and counted in
`cq_trade_risk_rejections`.

### Sizing

A strategy's signal `s` (target exposure, `+1` fully long) becomes
`target = s × allocation × equity`, which the risk checks then clip.

| `--sizing` | allocation per instrument | inputs |
| --- | --- | --- |
| `fixed` | `f` (`--fraction`, default 1.0) | — |
| `kelly` | `kelly_fraction × max(0, p/L − q/W)`, at most `max_allocation` | win probability `p`, average win `W` and loss `L` (as fractions of the position), estimated when the session is created by an event-driven backtest of the same strategy, parameters and fees over `--kelly-lookback-days` (180); fewer than 30 closed trades refuses the session; stored with it, so a restart sizes identically. Half Kelly by default |
| `vol_parity` | `(target_vol / √N) / σᵢ`, at most `max_allocation` | `σᵢ`: annualised volatility of the last `--vol-lookback` (30) strategy-bar log returns; `target_vol` 20 %; no position until `σᵢ` is known |

Sizing runs in `float` (research space, D4); the quantity becomes a `Decimal` on the venue's lot
grid only when the order is built. An unchanged signal whose position has drifted less than the
rebalance band (5 % of equity) is left alone, so a session does not churn fees on price noise.

### Idempotency and failure handling

| Failure | What happens |
| --- | --- |
| Crash between deciding and sending | the order is `PENDING` in the database; on restart it is looked up by client id: found → adopted with its fills, still not found after a 2-minute grace → `REJECTED` ("never reached the venue") |
| Database slow or down when an order is due | the order is not sent (`NOT_SENT`); after repeated failures new risk halts until storage recovers |
| Submission times out or the connection drops | `UNKNOWN`; looked up by client id until the venue answers; never re-submitted (invariant 5) |
| Venue says "duplicate client order id" | uncertain, not rejected: the earlier attempt may have landed; looked up |
| Rate limited | retried with backoff before it counts as a failure |
| Trade reported before the acknowledgement | parked until the venue order id is known, then applied — never dropped, never guessed |
| Same report twice, or out of order | ignored: order state only moves forward, fills are keyed by trade id (invariant 13) |
| Order WebSocket drops | reconnect with backoff; every working order is re-queried for what the outage hid |
| Two engines for one session | the second cannot take the lease; an engine that loses its lease is fenced out of the database and stops submitting and cancelling (invariant 12) |
| Credentials revoked, account suspended | halt, stop the session, exit 1 |

Venue errors are classified once, in `ccxt_trading_errors.py`: *rejected* only for explicit
refusals (insufficient funds, invalid order, bad request, not supported); *uncertain* for
timeouts, transport failures, duplicate ids and unclassified exchange errors; *rate limited*
and *auth* on their own. When in doubt, uncertain — a wrong "rejected" would let the engine
trade twice.

### Paper, sandbox, live

Paper sessions need no credentials: instrument specs come from the venue's public market
metadata, and the `PaperVenue` keeps its own simulated account (§6). Sandbox and live use the
CCXT venue with the session's client order ids, `watch_orders`/`watch_my_trades` streams with
periodic lookups as a fallback, and a balance check against the ledger every 30 s. Their
start-up also verifies that the API key cannot withdraw, where the venue reports it, and warns
when the session's capital exceeds the account's free balance (purchases are then capped by the
funds check). This release trades **spot** only; margin, funding and shorting on derivatives come
with M7.

### Operations

```
cq trade run mom-1 --strategy momentum -s BTC/USDT -t 1h -p lookback=24 \
    --capital 10000 --sizing vol_parity --target-vol 0.25 \
    --max-drawdown 0.15 --daily-loss 0.03 --max-order 2000     # create and run (paper)
cq trade run mom-1                                              # resume with the stored config
cq trade sessions                                               # every session, newest first
cq trade status mom-1                                           # engine, equity, halts, positions,
                                                                #   working orders, risk events
cq trade orders mom-1 --all
cq trade halt mom-1 -r "exchange incident"                      # kill switch, keep positions
cq trade flatten mom-1 -r "end of experiment"                   # kill switch and close all
cq trade resume mom-1 -r "checked"                              # lift stops and the kill switch
cq trade stop mom-1                                             # graceful stop
make trade SESSION=mom-1 STRATEGY=momentum SYMBOL=BTC/USDT PARAMS="lookback=24"
```

Commands are queued on `cmd:session:{id}` and work whether or not an engine is running: one
that arrives while the session is stopped applies when it next starts. Exit codes follow §13:
2 for a request to fix (unknown session, configuration mismatch, live not armed), 1 for a
condition worth retrying (database, Redis or venue unreachable, the session owned by another
engine, a session that stopped on a failure).

Prometheus metrics (`cq_trade_*`, labelled with the session) are served on
`CQ_TRADE_METRICS_PORT` (9109; `--metrics-port` per engine, 0 = off): orders submitted and
rejected, fills and filled notional, risk rejections by code, equity, drawdown, halted, venue
call latency by operation and outcome, persistence backlog and reconciliation anomalies.

---

## 15. Terminal gateway and web terminal (Module 4)

The operator's screen: live candles with the session's executions on them, the order book,
equity, PnL and risk usage marked to the market, positions, working orders, fills, risk events,
and the kill switches. The browser talks to one process, the **gateway** (`cq api serve`,
FastAPI under uvicorn); it never touches Redis or the database.

### Endpoints

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| GET | `/api/v1/health` | — | liveness; database and Redis reachability |
| GET | `/api/v1/auth/check` | bearer | validates the operator token at sign-in |
| POST | `/api/v1/stream/tickets` | bearer | single-use ticket for one stream connection |
| GET | `/api/v1/markets` | bearer | markets the ingestor publishes, and the chart timeframes |
| GET | `/api/v1/candles?venue&symbol&tf&end&limit` | bearer | a page of completed candles before `end`: scrolling a chart back |
| GET | `/api/v1/sessions` · `/api/v1/sessions/{id}` | bearer | the session list; one session's snapshot (the same one its stream starts with) |
| POST | `/api/v1/sessions/{id}/commands` | bearer | `halt` · `flatten` · `resume` · `stop` with a mandatory reason; queued on `cmd:session:{id}` (202) |
| WS | `/api/v1/stream/ws?ticket=…` | ticket | the multiplexed stream |
| GET | `/api/v1/stream/sse?ticket=…&topic=…` | ticket | the same stream as Server-Sent Events, topics fixed per connection |

OpenAPI documentation is served at `/api/docs` (`CQ_API_DOCS`).

### Authentication

The deployment has one operator and one secret, `CQ_API_TOKEN` (at least 16 characters; the
gateway refuses to start without it), compared in constant time. Browsers cannot put headers on
a WebSocket or an `EventSource`, and tokens in URLs end up in logs, so a stream connection
presents a **ticket** instead: 32 random bytes, valid for 30 seconds and for one connection,
stored in Redis only as a SHA-256 digest. The web terminal keeps the token for the tab
(`sessionStorage`) unless the operator chooses to be remembered on the device.

### Stream protocol

```
client → server   {"op": "subscribe" | "unsubscribe", "topic": "…"}   {"op": "ping", "id": …}
server → client   {"type": "hello", "v": 1, "connection": "c1", "heartbeat_s": 15}
                  {"type": "subscribed" | "unsubscribed", "topic": "…"}
                  {"type": "snapshot", "topic": "…", "seq": 0, "data": {…}}
                  {"type": "update",   "topic": "…", "seq": n, "kind": "candle" | "book" |
                   "health" | "order" | "fill" | "position" | "account" | "equity" | "risk" |
                   "state" | "flatten" | "engine", "data": {…}}
                  {"type": "error", "topic": "…", "code": "…", "message": "…"}
                  {"type": "heartbeat", "ts": …}   {"type": "pong", "id": …, "ts": …}
```

The client checks every topic's `seq`. A gap means a message was lost; it re-subscribes (WS)
or reconnects (SSE) and starts again from a fresh snapshot. A connection that hears nothing,
not even a heartbeat, for 2.5 heartbeat intervals is presumed dead and replaced. After two
failed WebSocket handshakes the client switches to SSE for the rest of the page's life.

| Limit | Value | Exceeded |
| --- | --- | --- |
| Stream connections per gateway process | `CQ_API_MAX_CONNECTIONS` (200) | WS close `4429`, SSE `429` |
| Topics per connection | 32 | `error` frame for that topic |
| Client frames | 300 per 10 s | WS close `4429` |
| Invalid or expired ticket | — | WS close `4401`, SSE `401` |
| Candles per snapshot or history page | 1 000, fewer on long timeframes (at most 200 000 minutes of history) | a larger `limit` is clamped; above 2 000 it is a `422` |

### Web terminal

Next.js 15 with React 19, TypeScript in strict mode, Tailwind v4 and shadcn/ui on Radix, Zustand
for state and TradingView Lightweight Charts v5 for the price and equity charts. It is rendered
on the client only: every byte of data comes from the gateway at run time, so the build needs no
secrets and the output is a self-contained Node server (`output: "standalone"`).

- **Candles** never re-render React. The chart subscribes to its slice of the market store and
  calls `series.update()` with the newest candle. It re-sets the whole series only when the
  store replaces it: a snapshot, a page of older history, or a corrected candle.
- **Executions** are grouped per candle and side into one arrow (below the candle for buys,
  above for sells) labelled with the count and quantity; labels that would overprint at the
  current zoom are dropped, the arrows stay. The legend shows each group's volume-weighted
  price for the hovered candle. Working limit orders and the average entry price are price
  lines.
- **PnL** is marked to the live market. Positions are re-marked to order-book mids at most once
  a second, and a book older than 15 s no longer counts. Equity, drawdown and the day's loss
  are measured against the engine's high-water mark and day-start equity, so the limit bars
  show how close the session is to its stops. When an instrument has no fresh book, the
  figures fall back to the equity the engine last reported, and the screen says which one it
  shows.
- **History** loads a page at a time as the chart is scrolled back; a page that falls in a gap
  of the stored data reaches back past it, and a reconnect keeps what was already loaded.
- **The session view** is a reducer over the snapshot and its deltas. It is idempotent: orders
  keep their highest version, fills are keyed by trade id, equity points by time.
- **Commands** require a reason, which lands in the audit trail. A command sent while no engine
  runs is queued and applied when the session next starts, and the dialog says so.
- **Failures stay local.** A panel that fails to render shows an error with a retry button,
  while the rest of the terminal, the kill switches included, keeps working.

### Operations

```
make api                 # gateway on CQ_API_HOST:CQ_API_PORT (127.0.0.1:8000)
make web-install         # npm ci
make web-dev             # http://localhost:3000 (NEXT_PUBLIC_CQ_API_URL → the gateway)
make web-build && make web-start
make web-check           # eslint · tsc --noEmit · unit tests (node:test)
```

The gateway allows the browser origins in `CQ_API_CORS_ORIGINS` and sends idle streams a
heartbeat every `CQ_API_HEARTBEAT_S` (15 s), so proxy read timeouts above that value are safe.
Behind nginx, `/api/v1/stream/sse` needs `proxy_buffering off`; the gateway also sends
`X-Accel-Buffering: no`. Exit codes follow §13: 2 when `CQ_API_TOKEN` is missing or too short.

---

## 16. Invariants (each one gets a test)

1. A strategy never sees a bar before its close time. Truncating the future never changes a past signal.
2. Engine handlers are synchronous and perform no I/O. Replaying a session's event log reproduces its ledger exactly.
3. Order quantities and ledger amounts are `Decimal`, quantized to the instrument's rules, and never below min notional.
4. All timestamps are timezone-aware UTC (ns integers in events). Naive datetimes fail lint (`DTZ`).
5. A timed-out submission moves to `UNKNOWN` and is reconciled by client id. It is never blindly resubmitted.
6. At most one engine holds a session's lease. Losing the lease blocks new orders immediately.
7. A degraded feed blocks new entries. Exits and cancels are always allowed.
8. Live mode needs both keys and passing startup checks. Risk caps cannot be modified by strategy code.
9. Randomness comes only from an injected, seeded `numpy.random.Generator` (the global `random` module is banned by lint).
10. Every backtest and research run stores git SHA, config hash, data-snapshot hash and seed.
11. An order is sent only after its `PENDING` record is durable (write-ahead). The venue never knows an order the database does not.
12. Every write carries the session fence; an engine that lost ownership can no longer write, submit or cancel.
13. Order state only moves forward, and a fill is applied at most once per `(session, venue, symbol, trade_id)`. Replaying reports in any order converges on the same orders and ledger.
14. A spot session never holds a negative position, and orders that reduce a position always pass the halt and data-quality gates.
15. A stream client never applies an update to state it has no snapshot for: every topic starts with a snapshot, and a sequence gap or a lagging reliable topic brings a fresh one.

---

## 17. Build plan

| Module | Scope | Done when |
| --- | --- | --- |
| **M0** Foundation ✓ | layout, tooling, import contracts, TimescaleDB + Redis | `make up` healthy, `make check` green |
| **M1** Market data ingestion ✓ | settings, logging, market domain, SQL migrations, bar repository and gap ledger, Redis publisher, CCXT live + history adapters, live pipeline, gap filling, `cq` CLI | contract, integration and end-to-end tests green against real PostgreSQL and Redis |
| **M2** Backtesting core ✓ | columnar market frames, strategy API with an optional vectorized form, streaming indicators, fee/slippage/latency models, event-driven and Polars-vectorized engines, metrics, parameter sweeps, history readers (TimescaleDB, CSV), run manifests and artifacts, `cq backtest` | parity suite green; metrics match hand-computed values; SQL resampling ≡ NumPy resampling on real PostgreSQL |
| **M3** Execution & risk ✓ | deterministic `TradingCore`, `ExecutionRunner` (write-ahead, venue workers, fenced writer), instrument specs, `OrderManager` + forward-only order FSM, `Decimal` ledger, `RiskManager` (pre-trade checks, drawdown stop, daily loss limit, kill switch, flatten), sizing (fixed, Kelly from a backtest, volatility parity), `PaperVenue`, `CcxtExecutionVenue` (paper · sandbox · live), reconciliation, two-key arming, `trading.*` schema, Redis feed/commands/lease/events, `cq trade` | restart, crash and lease-loss scenarios green; paper session end to end against real PostgreSQL and Redis |
| **M4** Terminal gateway & web terminal ✓ | FastAPI gateway (REST, WebSocket, SSE fallback, single-use stream tickets), TopicHub with per-topic delivery policies, candle/book/health/session feeds, Next.js terminal: dark shadcn/ui layout, Lightweight Charts candles with execution markers and order lines, live positions, orders, fills, PnL and risk usage, operator commands, `cq api` | gateway end to end over uvicorn against real PostgreSQL and Redis; stream client resync suite green; terminal driven in a browser against a live paper session |
| **M5** Indicators & features | 50+ Polars indicators with streaming twins, look-ahead guards, higher-timeframe continuous aggregates | truncation tests pass for every indicator |
| **M6** Research & ML | walk-forward, Monte Carlo, DSR/PBO, promotion gates, meta-labeling | funnel runs end-to-end on one strategy |
| **M7** Engine unification & soak | event envelope, trades stream, backtests on `TradingCore` through a `SimulatedVenue`, paper replay parity, derivatives (margin, funding, shorting), portfolio-level risk | replay parity green; 72 h paper soak plus a testnet session with zero reconciliation mismatches |
| **M8** Research API & explorers | job queue, backtest and research endpoints, session start from the UI, backtest and research explorers in the web terminal | a sweep submitted and inspected from the browser |
| **M9** Operations | Dockerfiles, CI, Prometheus/Grafana, runbooks | one-command deploy, dashboards live |

### Mapping from the original README

| README feature | Module |
| --- | --- |
| Historical data engine, 6+ years, data validation, gap checks | M1 ✓ |
| 50+ indicators with anti-look-ahead protection | M5 (streaming + Polars pairs start in M2 ✓) |
| Strategy research and automated discovery | M2 ✓ (parameter sweeps), M6 |
| Event-driven backtesting with realistic execution | M2 ✓ (M7 runs it on the live engine's core) |
| Risk management, dynamic position sizing | M3 ✓ |
| ML trade-probability prediction (XGBoost) | M6 |
| Walk-forward validation, Monte Carlo | M6 |
| Paper trading; live trading on Binance, disabled by default | M3 ✓ (spot; derivatives in M7) |
| CLI (`cq`, `python -m crypto_quant`) | M1–M3 ✓ (grows with each module) |
| Dashboards | M4 ✓ (live markets, sessions, PnL, risk), M8 (backtests, research) |

The README asks for Python 3.12+, but this brief specifies 3.11. The codebase targets **3.11 as
its floor**: `requires-python = ">=3.11,<3.14"`, with ruff and mypy pinned to 3.11 syntax. It
runs unchanged on 3.12 and 3.13.

---

## 18. Quality gates

`make check` runs, in order: `ruff format --check`, `ruff check`, `mypy --strict`,
`lint-imports`, and the unit and in-memory contract tests. CI adds `make test-integration`
against the compose stack (integration tests, contract tests against PostgreSQL, and the
end-to-end pipeline test). The parity suite runs with the unit tests; its vectorized half
needs Polars, which the dev environment installs. Coverage must stay at or above 85%.
`make web-check` runs the web terminal's gates: eslint, `tsc --noEmit` in strict mode, and the
unit tests of its stream client, reducers, metrics and formatting. Nothing merges red.
