-- Trading sessions: configuration and restart state, orders, fills, risk events, equity.
-- Portable PostgreSQL; the equity curve becomes a hypertable when TimescaleDB is installed.
--
-- Idempotency lives in the keys (ARCHITECTURE §14):
-- * an order is keyed by its deterministic client order id and only ever replaced by a
--   higher version (the upsert's WHERE clause), so repeated or reordered writes cannot bring
--   back an older state;
-- * a fill is keyed by (session, venue, symbol, trade_id) — venues number trades per market,
--   and a sandbox and a live session on the same venue name live in different id spaces —
--   and is inserted at most once;
-- * every write batch first checks the session's fence in the same transaction, so an engine
--   that lost ownership of the session can no longer write anything.
--
-- Timestamps that the engine produces are stored as epoch nanoseconds (exact, and what the
-- engine compares), with a generated timestamptz column next to them for humans and tools.

CREATE SCHEMA IF NOT EXISTS trading;

CREATE TABLE trading.sessions (
    session_id       text        PRIMARY KEY,
    mode             text        NOT NULL,
    venue            text        NOT NULL,
    strategy         text        NOT NULL,
    timeframe        text        NOT NULL,
    symbols          text[]      NOT NULL,
    quote_currency   text        NOT NULL,
    initial_capital  numeric     NOT NULL,
    config           jsonb       NOT NULL,               -- SessionConfig.describe()
    status           text        NOT NULL DEFAULT 'created',
    fence            bigint      NOT NULL DEFAULT 0,
    state            jsonb,                              -- stops, halts, command offset …
    last_equity      numeric,
    halts            text[]      NOT NULL DEFAULT '{}',
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT sessions_id_format CHECK (session_id ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
    CONSTRAINT sessions_mode_known CHECK (mode IN ('paper', 'sandbox', 'live')),
    CONSTRAINT sessions_status_known CHECK (status IN ('created', 'running', 'stopped')),
    CONSTRAINT sessions_capital_positive CHECK (initial_capital > 0),
    CONSTRAINT sessions_fence_non_negative CHECK (fence >= 0),
    CONSTRAINT sessions_has_symbols CHECK (cardinality(symbols) > 0)
);

COMMENT ON COLUMN trading.sessions.fence IS
    'Incremented by every engine that takes the session over; writes with an older fence are '
    'refused, so a stalled engine that lost its lease cannot write after its successor started.';

CREATE INDEX sessions_updated_idx ON trading.sessions (updated_at DESC);

CREATE TABLE trading.orders (
    client_order_id        text        PRIMARY KEY,
    session_id             text        NOT NULL REFERENCES trading.sessions (session_id),
    venue                  text        NOT NULL,
    symbol                 text        NOT NULL,
    side                   text        NOT NULL,
    order_type             text        NOT NULL,
    quantity               numeric     NOT NULL,
    limit_price            numeric,
    time_in_force          text        NOT NULL,
    post_only              boolean     NOT NULL,
    reduce_only            boolean     NOT NULL,
    intent                 text        NOT NULL,
    status                 text        NOT NULL,
    venue_order_id         text,
    filled_quantity        numeric     NOT NULL,
    filled_notional        numeric     NOT NULL,
    venue_filled_quantity  numeric     NOT NULL,
    fees                   jsonb       NOT NULL DEFAULT '{}',   -- {currency: decimal string}
    reject_reason          text,
    created_ns             bigint      NOT NULL,
    updated_ns             bigint      NOT NULL,
    version                integer     NOT NULL,
    created_at             timestamptz GENERATED ALWAYS AS
                               (to_timestamp(created_ns::double precision / 1e9)) STORED,
    updated_at             timestamptz GENERATED ALWAYS AS
                               (to_timestamp(updated_ns::double precision / 1e9)) STORED,
    CONSTRAINT orders_side_known CHECK (side IN ('buy', 'sell')),
    CONSTRAINT orders_type_known CHECK (order_type IN ('market', 'limit')),
    CONSTRAINT orders_tif_known CHECK (time_in_force IN ('gtc', 'ioc', 'fok')),
    CONSTRAINT orders_status_known CHECK (status IN (
        'pending', 'unknown', 'open', 'partially_filled', 'cancel_pending',
        'filled', 'cancelled', 'rejected', 'expired'
    )),
    CONSTRAINT orders_quantity_positive CHECK (quantity > 0),
    CONSTRAINT orders_limit_price_shape CHECK ((order_type = 'limit') = (limit_price IS NOT NULL)),
    CONSTRAINT orders_filled_non_negative CHECK (
        filled_quantity >= 0 AND filled_notional >= 0 AND venue_filled_quantity >= 0
    ),
    CONSTRAINT orders_version_positive CHECK (version >= 1)
);

COMMENT ON TABLE trading.orders IS
    'Latest version of every order. The engine writes an order as pending BEFORE sending it '
    '(write-ahead), so the venue never knows an order this table does not.';

CREATE INDEX orders_session_idx ON trading.orders (session_id, created_ns);
CREATE INDEX orders_working_idx ON trading.orders (session_id)
    WHERE status IN ('pending', 'unknown', 'open', 'partially_filled', 'cancel_pending');
CREATE INDEX orders_venue_order_idx ON trading.orders (venue, symbol, venue_order_id)
    WHERE venue_order_id IS NOT NULL;

CREATE TABLE trading.fills (
    venue            text        NOT NULL,
    symbol           text        NOT NULL,
    trade_id         text        NOT NULL,
    client_order_id  text        NOT NULL REFERENCES trading.orders (client_order_id),
    session_id       text        NOT NULL REFERENCES trading.sessions (session_id),
    side             text        NOT NULL,
    quantity         numeric     NOT NULL,
    price            numeric     NOT NULL,
    fee              numeric     NOT NULL,
    fee_currency     text        NOT NULL,
    liquidity        text        NOT NULL,
    ts_ns            bigint      NOT NULL,
    ts               timestamptz GENERATED ALWAYS AS
                         (to_timestamp(ts_ns::double precision / 1e9)) STORED,
    recorded_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fills_pkey PRIMARY KEY (session_id, venue, symbol, trade_id),
    CONSTRAINT fills_side_known CHECK (side IN ('buy', 'sell')),
    CONSTRAINT fills_liquidity_known CHECK (liquidity IN ('maker', 'taker')),
    CONSTRAINT fills_positive CHECK (quantity > 0 AND price > 0)
);

COMMENT ON TABLE trading.fills IS
    'Every execution, exactly once. The session ledger is rebuilt from these rows on restart.';

CREATE INDEX fills_session_time_idx ON trading.fills (session_id, ts_ns);
CREATE INDEX fills_order_idx ON trading.fills (client_order_id);

CREATE TABLE trading.risk_events (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id  text        NOT NULL REFERENCES trading.sessions (session_id),
    ts_ns       bigint      NOT NULL,
    ts          timestamptz GENERATED ALWAYS AS (to_timestamp(ts_ns::double precision / 1e9)) STORED,
    kind        text        NOT NULL,
    code        text        NOT NULL,
    message     text        NOT NULL,
    symbol      text,
    detail      jsonb       NOT NULL DEFAULT '{}',
    CONSTRAINT risk_events_kind_known CHECK (kind IN (
        'rejected', 'adjusted', 'breach', 'halt', 'resume', 'anomaly', 'market_data'
    ))
);

COMMENT ON TABLE trading.risk_events IS
    'Audit trail of pre-trade refusals and trims, stops, halts, resumes and reconciliation '
    'anomalies. Refused orders never become orders: they are recorded here only.';

CREATE INDEX risk_events_session_idx ON trading.risk_events (session_id, id DESC);

CREATE TABLE trading.equity (
    session_id      text        NOT NULL,
    ts              timestamptz NOT NULL,
    equity          numeric     NOT NULL,
    cash            numeric     NOT NULL,
    gross_exposure  numeric     NOT NULL,
    drawdown        numeric     NOT NULL,
    day_pnl         numeric     NOT NULL,
    CONSTRAINT equity_pkey PRIMARY KEY (session_id, ts)
);

COMMENT ON TABLE trading.equity IS 'Equity curve of each session, one point per base bar.';

DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'trading.equity',
            by_range('ts', INTERVAL '30 days'),
            create_default_indexes => FALSE,
            if_not_exists => TRUE
        );
        RAISE NOTICE 'trading.equity is a TimescaleDB hypertable';
    ELSE
        RAISE NOTICE 'timescaledb is not installed; trading.equity remains a plain table';
    END IF;
END
$migration$;
