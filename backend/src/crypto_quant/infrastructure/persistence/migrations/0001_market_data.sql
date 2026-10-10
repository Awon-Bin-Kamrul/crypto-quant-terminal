-- Market data: canonical 1-minute bars and the gap-fill audit ledger.
-- Portable PostgreSQL; TimescaleDB-specific storage is configured in 0002.

CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE market.bars_1m (
    venue        text             NOT NULL,
    symbol       text             NOT NULL,
    ts           timestamptz      NOT NULL,          -- bar open time, UTC
    open         double precision NOT NULL,
    high         double precision NOT NULL,
    low          double precision NOT NULL,
    close        double precision NOT NULL,
    volume       double precision NOT NULL,          -- base-asset volume
    source       smallint         NOT NULL,          -- 1 websocket, 2 REST (authoritative)
    ingested_at  timestamptz      NOT NULL DEFAULT now(),
    CONSTRAINT bars_1m_pkey PRIMARY KEY (venue, symbol, ts),
    CONSTRAINT bars_1m_minute_aligned CHECK (mod(extract(epoch FROM ts), 60) = 0),
    CONSTRAINT bars_1m_ohlc_consistent CHECK (
        low > 0 AND low <= least(open, close) AND high >= greatest(open, close)
    ),
    CONSTRAINT bars_1m_volume_non_negative CHECK (volume >= 0),
    CONSTRAINT bars_1m_source_known CHECK (source IN (1, 2))
);

COMMENT ON TABLE market.bars_1m IS
    'Canonical closed 1-minute OHLCV bars. Higher timeframes are derived, never ingested.';

CREATE TABLE market.data_gaps (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    venue         text        NOT NULL,
    symbol        text        NOT NULL,
    timeframe     text        NOT NULL,
    range_start   timestamptz NOT NULL,
    range_end     timestamptz NOT NULL,              -- exclusive
    status        text        NOT NULL,
    reason        text        NOT NULL,
    bars_missing  integer     NOT NULL,
    bars_filled   integer     NOT NULL DEFAULT 0,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT data_gaps_range_valid CHECK (range_end > range_start),
    CONSTRAINT data_gaps_status_known CHECK (status IN ('filled', 'unfillable')),
    CONSTRAINT data_gaps_counts_valid CHECK (bars_missing > 0 AND bars_filled BETWEEN 0 AND bars_missing)
);

COMMENT ON TABLE market.data_gaps IS
    'Gap-fill audit. Rows with status = unfillable are ranges the venue has no data for; '
    'gap detection skips them.';

CREATE INDEX data_gaps_unfillable_idx
    ON market.data_gaps (venue, symbol, timeframe, range_start)
    WHERE status = 'unfillable';

CREATE INDEX data_gaps_recorded_idx ON market.data_gaps (recorded_at DESC);
