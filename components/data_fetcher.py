import datetime
import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
import sys
import requests
import time

symbol_map = {
    "ethereum": "ETHUSDT",
}


class DataFetcher:
    def __init__(self, coin_name="ethereum", interval="1d", year=2025):
        self.coin = coin_name
        self.interval = interval
        self.year = year
        self.symbol = symbol_map[self.coin]
        self.url = "https://api.binance.com/api/v3/klines"

    def fetch_klines(
        self
    ) -> pd.DataFrame:
        """
        Fetch historical kline/candlestick data from Binance API.

        Parameters:
        - symbol: trading pair (e.g., "ETHUSDT").
        - interval: data interval (e.g., "1d", "1h").
        - limit: number of rows to retrieve (max 1000 per request).

        Returns:
        - DataFrame with timestamp, open, high, low, close, volume, etc.
        """
        start_time = int(datetime.datetime(self.year, 1, 1).timestamp() * 1000)
        end_time = int(datetime.datetime(self.year + 1, 1, 1).timestamp() * 1000)
        all_data = []
        while True:
            params = {
                "symbol": self.symbol,
                "interval": self.interval,
                "startTime": start_time,
                "endTime": end_time,
                "limit": 1000,
            }

            resp = requests.get(self.url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break

            all_data.extend(data)
            last_time = data[-1][0]
            if last_time >= end_time or len(data) < 1000:
                break

            start_time = last_time + 1
            time.sleep(0.5)

        cols = [
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
        df = pd.DataFrame(all_data, columns=cols)


        return df
    
