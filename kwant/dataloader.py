import requests
from datetime import datetime, timedelta

baseurl = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=1000"

r = requests.get(baseurl).json()


class Ohlcv:
    def __init__(self, opentime, openprice, highprice, lowprice, closeprice, volume):
        self.opentime = opentime
        self.openprice = openprice
        self.highprice = highprice
        self.lowprice = lowprice
        self.closeprice = closeprice
        self.volume = volume

    @staticmethod
    def from_list(l):
        return Ohlcv(
            make_datetime(l[0]),
            float(l[1]),
            float(l[2]),
            float(l[3]),
            float(l[4]),
            float(l[7])
        )

    def __str__(self):
        print(
            f"{self.opentime} o: {self.openprice} h: {self.highprice} l: {self.lowprice} c: {self.closeprice} v: {self.volume}")


def make_datetime(ts):
    return datetime.fromtimestamp(int(ts / 1000))


def get_ohlcv(symbol, start, end, interval):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&startTime={start}&endTime={end}&interval={interval}"
    return [Ohlcv.from_list(l) for l in requests.get(url).json()]


def scrape(symbol, interval='1d'):
    base = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=1000'
    result = []
    res = requests.get(base).json()
    result.extend(res[:-1])
    while True:
        res = requests.get(base + f"&endTime={res[0][0]}").json()
        result.extend(res[:-1])

        if len(res) == 1:
            result.extend(res)
            break

    return result
