import requests
import pandas as pd
from datetime import timedelta


def fetch_price_history_coingecko(coin_id, start_time):
    end_time = start_time + timedelta(days=1)
    start_timestamp = int(start_time.timestamp())
    end_timestamp = int(end_time.timestamp())

    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": start_timestamp, "to": end_timestamp}

    response = requests.get(url, params=params)
    data = response.json()

    if response.status_code != 200:
        raise Exception(f"Error {response.status_code}: {data}")

    prices = data.get("prices", [])
    if not prices:
        raise Exception(
            f"No price data found for {coin_id} from {start_time} to {end_time}."
        )

    df = pd.DataFrame(prices, columns=["timestamp", "price"])
    df["timestamp"] = df["timestamp"] // 1000
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    df.set_index("datetime", inplace=True)
    df.drop(columns=["timestamp"], inplace=True)

    return df
