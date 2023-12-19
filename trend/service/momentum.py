import numpy as np
import psycopg2
from kwant.models.momentum import momentum


def get_momentum_ranking():
    conn = psycopg2.connect(
        dbname="degentrading",
        user="postgres",
        password="postgres",
        host="192.168.8.105"
    )
    cursor = conn.cursor()

    query = """WITH SelectedSymbols as(
        WITH DailyRecords AS (
        SELECT mp.id,
           mp.symbol
    FROM
        market_pairs mp
    JOIN
        ohlcv ON mp.id = ohlcv.market_pair_id
    WHERE
        ohlcv.open_time >= (TIMESTAMP WITH TIME ZONE '2023-01-01 00:00:00+00')
    GROUP BY
        mp.id, mp.symbol
    HAVING
        COUNT(DISTINCT ohlcv.open_time) >= 330
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
    AND
        ohlcv.open_time >= ((CURRENT_DATE - EXTRACT(DOW FROM CURRENT_DATE)::INTEGER) - INTERVAL '75 days')
    AND 
       ohlcv.open_time <= CURRENT_DATE - EXTRACT(DOW FROM CURRENT_DATE)::INTEGER
    ORDER BY
        ss.symbol, ohlcv.open_time DESC;"""

    cursor.execute(query)

    res = cursor.fetchall()
    # Close the database connection
    conn.close()

    data_dict = {}

    # Populate the dictionary
    for row in res:
        symbol, open_time, close_price = row
        if symbol not in data_dict:
            data_dict[symbol] = []
        data_dict[symbol].append((open_time, np.float64(close_price)))

    scores = []
    for k in data_dict.keys():
        scores.append((k, momentum([i[1] for i in data_dict[k][:-14]])))

    return scores
