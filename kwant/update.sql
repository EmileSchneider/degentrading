SELECT market_pair_id,mp.symbol, c.symbol, c2.symbol, MAX(open_time)
FROM ohlcv
         join public.market_pairs mp on mp.id = ohlcv.market_pair_id
         join public.cryptocurrencies c on c.id = mp.base_currency_id
         join public.cryptocurrencies c2 on c2.id = mp.quote_currency_id
WHERE c2.symbol = 'USDT'
  and ohlcv.timeframe = '1d'
  and open_time = (SELECT MAX(open_time)
                   from degentrading.public.ohlcv
                            join public.market_pairs mp on mp.id = ohlcv.market_pair_id
                            join public.cryptocurrencies c on c.id = mp.base_currency_id
                            join public.cryptocurrencies c2 on c2.id = mp.quote_currency_id
                   WHERE c2.symbol = 'USDT')
GROUP BY market_pair_id, c.symbol, c2.symbol, mp.symbol;


SELECT mp.id,
           mp.symbol,
           COUNT(DISTINCT(ohlcv.open_time))
        FROM
            market_pairs mp
        JOIN
            ohlcv ON mp.id = ohlcv.market_pair_id
        WHERE
            ohlcv.open_time >= (TIMESTAMP WITH TIME ZONE '2023-01-01 00:00:00+00')
        GROUP BY
            mp.id, mp.symbol;

WITH SelectedSymbols as(
    WITH DailyRecords AS (
        SELECT mp.id,
           mp.symbol,
           COUNT(DISTINCT(ohlcv.open_time))
        FROM
            market_pairs mp
        JOIN
            ohlcv ON mp.id = ohlcv.market_pair_id
        WHERE
            ohlcv.open_time >= (TIMESTAMP WITH TIME ZONE '2023-01-01 00:00:00+00')
        GROUP BY
            mp.id, mp.symbol
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
        DailyRecords dr ON mp.id = dr.id
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
        avg_daily_volume DESC limit 50
    )
    SELECT
        ss.symbol,
        ohlcv.open_time,
        ohlcv.close_price
    FROM
        ohlcv
    JOIN
        SelectedSymbols ss ON ohlcv.market_pair_id = ss.id
    WHERE
        ohlcv.timeframe = '1d'
        AND ohlcv.open_time >= (CURRENT_DATE - INTERVAL '75 days')
    ORDER BY
        ss.symbol, ohlcv.open_time DESC;
