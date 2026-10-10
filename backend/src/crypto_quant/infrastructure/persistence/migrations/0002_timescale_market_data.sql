-- TimescaleDB storage for market.bars_1m, applied only when the extension is installed.
-- On plain PostgreSQL the table stays a regular table: everything still works, just without
-- chunking and compression.
--
-- * 7-day chunks: ~10k rows per symbol per chunk, small enough to stay cached while hot.
-- * Columnar compression after 14 days, segmented by instrument and ordered by time, so a
--   backtest reading one symbol decompresses only that symbol's segments. Recent chunks stay
--   uncompressed because live reconciliation still rewrites the last hours.
-- * No default time index: the primary key (venue, symbol, ts) serves every query we run.

DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'market.bars_1m',
            by_range('ts', INTERVAL '7 days'),
            create_default_indexes => FALSE,
            if_not_exists => TRUE
        );
        EXECUTE $ddl$
            ALTER TABLE market.bars_1m SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'venue, symbol',
                timescaledb.compress_orderby = 'ts DESC'
            )
        $ddl$;
        PERFORM add_compression_policy('market.bars_1m', INTERVAL '14 days', if_not_exists => TRUE);
        RAISE NOTICE 'market.bars_1m is a compressed TimescaleDB hypertable';
    ELSE
        RAISE NOTICE 'timescaledb is not installed; market.bars_1m remains a plain table';
    END IF;
END
$migration$;
