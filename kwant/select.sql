SELECT
    mp.id AS id,
    mp.symbol AS symbol,
    bc.symbol AS base_currency_symbol,
    qc.symbol AS quote_currency_symbol
FROM
    market_pairs mp
INNER JOIN cryptocurrencies bc ON mp.base_currency_id = bc.id
INNER JOIN cryptocurrencies qc ON mp.quote_currency_id = qc.id
WHERE qc.symbol = 'USDT';

DELETE FROM ohlcv WHERE TRue;

ALTER TABLE ohlcv
ALTER COLUMN open_price TYPE DECIMAL(40, 20),
ALTER COLUMN high_price TYPE DECIMAL(40, 20),
ALTER COLUMN low_price TYPE DECIMAL(40, 20),
ALTER COLUMN close_price TYPE DECIMAL(40, 20),
ALTER COLUMN volume TYPE DECIMAL(40, 20);


SELECT * FROM ohlcv JOIN public.market_pairs mp on mp.id = ohlcv.market_pair_id join public.cryptocurrencies c on c.id = mp.base_currency_id
WHERE c.symbol = 'DOT';

SELECT
    day,
    market_pair_id,
    MIN(open_price) AS daily_open,
    MAX(high_price) AS daily_high,
    MIN(low_price) AS daily_low,
    MAX(close_price) AS daily_close,
    SUM(volume) AS daily_volume
FROM (
    SELECT
        DATE(open_time AT TIME ZONE 'UTC') AS day,
        market_pair_id,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        ROW_NUMBER() OVER (PARTITION BY market_pair_id, DATE(open_time AT TIME ZONE 'UTC') ORDER BY open_time ASC) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY market_pair_id, DATE(open_time AT TIME ZONE 'UTC') ORDER BY open_time DESC) AS rn_desc
    FROM
        ohlcv
) sub
WHERE
    rn_asc = 1 OR rn_desc = 1
GROUP BY
    day,
    market_pair_id;




SELECT
    mp.id,
    mp.symbol,
    AVG(ohlcv.volume) as avg_daily_volume
FROM
    market_pairs mp
JOIN
    ohlcv ON mp.id = ohlcv.market_pair_id
WHERE
    ohlcv.timeframe = '1d' -- considering daily data
    AND ohlcv.open_time >= '2022-01-01' -- starting from 1st Jan 2022
    AND (
        mp.symbol NOT LIKE '%UP%'
        AND mp.symbol NOT LIKE '%DOWN%'
        AND mp.symbol NOT LIKE '%BULL%'
        AND mp.symbol NOT LIKE '%BEAR%'
    )
GROUP BY
    mp.id, mp.symbol
ORDER BY
    avg_daily_volume DESC LIMIT  50;



WITH DailyRecords AS (
    SELECT
        market_pair_id,
        COUNT(*) AS total_days
    FROM
        ohlcv
    WHERE
        timeframe = '1d'
    GROUP BY
        market_pair_id
    HAVING
        COUNT(*) >= 365
)

SELECT
    mp.id,
    mp.symbol,
    AVG(ohlcv.volume) as avg_daily_volume
FROM
    market_pairs mp
JOIN
    ohlcv ON mp.id = ohlcv.market_pair_id
JOIN
    DailyRecords dr ON mp.id = dr.market_pair_id
WHERE
    ohlcv.timeframe = '1d'
    AND ohlcv.open_time >= '2022-01-01'
    AND (
        mp.symbol NOT LIKE '%UP%'
        AND mp.symbol NOT LIKE '%DOWN%'
        AND mp.symbol NOT LIKE '%BULL%'
        AND mp.symbol NOT LIKE '%BEAR%'
        AND mp.symbol NOT LIKE '%BUSD%'
        AND mp.symbol NOT LIKE '%USDC%'
    ) AND mp.symbol LIKE '%BTC'
GROUP BY
    mp.id, mp.symbol
ORDER BY
    avg_daily_volume DESC limit 50;
