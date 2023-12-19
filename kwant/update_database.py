import psycopg2
import requests
import datetime

from psycopg2.extras import execute_values


def get_or_create_currency(cursor, symbol):
    cursor.execute("SELECT id FROM cryptocurrencies WHERE symbol = %s", (symbol,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO cryptocurrencies (name, symbol, is_active) VALUES (%s, %s, %s) RETURNING id",
                       (None, symbol, True))
        return cursor.fetchone()[0]


def get_connection(host, db, user, password):
    return psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=db
    )


def bulk_insert_ohlcv(data, market_pair_id, cursor):
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
            datetime.datetime.fromtimestamp(record[0] / 1000),  # Open time (convert from ms to seconds)
            datetime.datetime.fromtimestamp(record[6] / 1000)  # Close time (convert from ms to seconds)
        ))

    execute_values(cursor, insert_query, transformed_data)


def select_last_entries(conn, timeframe, quote_asset):
    query = f"""SELECT market_pair_id, mp.symbol, MAX(open_time)
FROM ohlcv
         join public.market_pairs mp on mp.id = ohlcv.market_pair_id
         join public.cryptocurrencies c on c.id = mp.base_currency_id
         join public.cryptocurrencies c2 on c2.id = mp.quote_currency_id
WHERE c2.symbol = '{quote_asset}'
  and ohlcv.timeframe = '{timeframe}'
  and open_time = (SELECT MAX(open_time)
                   from degentrading.public.ohlcv
                            join public.market_pairs mp on mp.id = ohlcv.market_pair_id
                            join public.cryptocurrencies c on c.id = mp.base_currency_id
                            join public.cryptocurrencies c2 on c2.id = mp.quote_currency_id
                   WHERE c2.symbol = '{quote_asset}')
GROUP BY market_pair_id, mp.symbol;"""

    cursor: psycopg2.cursor = conn.cursor()

    cursor.execute(query)

    return cursor.fetchall()


def request(symbol, last_entry_time):
    timestamp = int((last_entry_time + datetime.timedelta(days=1)).timestamp() * 1000)
    res = requests.get(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&startTime={timestamp}")

    if res.status_code == 200:
        print(f"retrieved {len(res.json()) - 1} values")
        return res.json()[:-1]
    else:
        print(f"something went wrong for {symbol}")
        print(res.content)
        return []


def run(conn, timeframe, quote_asset):
    cursor = conn.cursor()
    last_entries = select_last_entries(conn, timeframe, quote_asset)

    for entry in last_entries:
        print(entry)
        values = request(entry[1], entry[2])
        bulk_insert_ohlcv(values, entry[0], cursor)