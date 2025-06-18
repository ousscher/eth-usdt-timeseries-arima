import pandas as pd
import os

class DataPreprocessor:
    @staticmethod
    def process_klines(data):
        """Convert Binance API data to a DataFrame and clean it."""
        df = data

        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
        print(df.head())
        num_cols = ["open", "high", "low", "close", "volume", 
                    "quote_asset_volume", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"]

        df[num_cols] = df[num_cols].astype(float)
        df.drop(columns=['close_time', 'ignore'], inplace=True)
        df.sort_index(inplace=True)
        return df

    @staticmethod
    def save_to_csv(df, year):
        """Save the DataFrame to a CSV file."""
        file_path = f"artifacts/{year}_crypto_data.csv"
        os.makedirs(os.path.dirname(file_path), exist_ok=True) 
        df.to_csv(file_path, index=False)
