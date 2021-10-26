CREATE TABLE ticker
(
    `exchange` String,
    `market` String,
    `price` Float64,
    `timestamp` DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (exchange, market, timestamp);

CREATE TABLE trade
(
    `exchange` String,
    `market` String,
    `trade_id` String,
    `side` String,
    `size` Float64,
    `price` Float64,
    `timestamp` DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (exchange, market, side, timestamp);

CREATE TABLE candle
(
    `exchange` String,
    `market` String,
    `open` Float64,
    `high` Float64,
    `low` Float64,
    `close` Float64,
    `volume` Float64,
    `timestamp` DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (exchange, market, timestamp)