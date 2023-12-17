from datetime import datetime

import psycopg2
import pandas
import requests

from kwant.dataloader import scrape

CONN_PARAMS = {
    'dbname': "degentrading",
    'user': "postgres",
    'password': "postgres",
    'host': "192.168.8.105"
}
# Connect to your database
conn = psycopg2.connect(
    dbname="degentrading",
    user="postgres",
    password="postgres",
    host="192.168.8.105"
)
cursor = conn.cursor()

cursor.execute("""SELECT
    mp.id AS id,
    mp.symbol AS symbol,
    bc.symbol AS base_currency_symbol,
    qc.symbol AS quote_currency_symbol
FROM
    market_pairs mp
INNER JOIN cryptocurrencies bc ON mp.base_currency_id = bc.id
INNER JOIN cryptocurrencies qc ON mp.quote_currency_id = qc.id
WHERE qc.symbol = 'BTC';""")

result = cursor.fetchall()


import psycopg2
from psycopg2.extras import execute_values


def bulk_insert_ohlcv(data, market_pair_id, connection_params):
    """
    Insert multiple OHLCV records into the database.

    :param data: List of OHLCV data
    :param market_pair_id: ID of the market pair
    :param connection_params: Database connection parameters
    """
    insert_query = """
    INSERT INTO ohlcv (
        market_pair_id, data_type, timeframe, open_price, high_price, low_price, close_price, volume, open_time, close_time
    ) VALUES %s;
    """

    # Transform data for insertion
    transformed_data = []
    for record in data:
        transformed_data.append((
            market_pair_id, 'OHLCV', '1d',  # Assuming '1d' is your timeframe
            record[1], record[2], record[3], record[4],  # OHLCV values
            record[7],  # Volume
            datetime.fromtimestamp(record[0] / 1000),  # Open time (convert from ms to seconds)
            datetime.fromtimestamp(record[6] / 1000)  # Close time (convert from ms to seconds)
        ))

    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, transformed_data)


max_l = len(result)
i = 1
for r in result:
    id = r[0]
    symbol = r[1]
    print(symbol, i, max_l)
    i += 1
    ohlcv_data = scrape(r[1])  # Assuming this is your function to retrieve OHLCV data
    market_pair_id = id  # Example market pair ID
    bulk_insert_ohlcv(ohlcv_data, market_pair_id, CONN_PARAMS)
