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