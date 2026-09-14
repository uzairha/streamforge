CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS aggregates (
    chain        TEXT NOT NULL,
    metric       TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    labels       JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Deterministic, sorted "k=v,k=v" rendering of labels. jsonb has no
    -- default btree operator class, so it can't sit in a primary key
    -- directly; this scalar column stands in for it.
    labels_key   TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (chain, metric, window_start, labels_key)
);

SELECT create_hypertable('aggregates', 'window_start', if_not_exists => TRUE);
