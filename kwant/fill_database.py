import sys
from datetime import datetime

import psycopg2
import requests
from psycopg2.extras import execute_values

from kwant.dataloader import scrape


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


if __name__ == "__main__":
    dbname = sys.argv[1]
    user = sys.argv[2]
    password = sys.argv[3]
    host = sys.argv[4]
    timeframe = sys.argv[5]
    quote_asset = sys.argv[6]

    print(dbname, user, password, host, timeframe, quote_asset)
    CONN_PARAMS = {
        'dbname': dbname,
        'user': user,
        'password': password,
        'host': host
    }

    conn = psycopg2.connect(
        **CONN_PARAMS
    )

    cursor = conn.cursor()

    response = requests.get('https://api.binance.com/api/v3/exchangeInfo')
    data = response.json()

    pairs = [(pair['baseAsset'], pair['quoteAsset'], pair['symbol']) for pair in data['symbols'] if
             pair['quoteAsset'] in ['USDT', 'BTC']]


    def get_or_create_currency(symbol):
        cursor.execute("SELECT id FROM cryptocurrencies WHERE symbol = %s", (symbol,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            cursor.execute("INSERT INTO cryptocurrencies (name, symbol, is_active) VALUES (%s, %s, %s) RETURNING id",
                           (None, symbol, True))
            return cursor.fetchone()[0]


    for base, quote, symbol in pairs:
        base_id = get_or_create_currency(base)
        quote_id = get_or_create_currency(quote)

        # Assuming you have a way to determine the exchange_id for Binance
        exchange_id = 1  # Replace with actual exchange_id for Binance

        cursor.execute(
            "INSERT INTO market_pairs (base_currency_id, quote_currency_id, exchange_id, symbol) VALUES (%s, %s, %s, %s)",
            (base_id, quote_id, exchange_id, symbol)
        )

    # Commit changes and close the connection
    conn.commit()
    cursor.close()

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

    conn.close()