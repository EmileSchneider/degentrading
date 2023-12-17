import psycopg2
import pandas
import requests

# Connect to your database
conn = psycopg2.connect(
    dbname="degentrading",
    user="postgres",
    password="postgres",
    host="192.168.8.105"
)
cursor = conn.cursor()

response = requests.get('https://api.binance.com/api/v3/exchangeInfo')
data = response.json()

pairs = [(pair['baseAsset'], pair['quoteAsset'], pair['symbol']) for pair in data['symbols'] if pair['quoteAsset'] in ['USDT', 'BTC']]


def get_or_create_currency(symbol):
    cursor.execute("SELECT id FROM cryptocurrencies WHERE symbol = %s", (symbol,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO cryptocurrencies (name, symbol, is_active) VALUES (%s, %s, %s) RETURNING id", (None, symbol, True))
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
conn.close()
