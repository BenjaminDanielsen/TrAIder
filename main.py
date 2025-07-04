import os
import json
import asyncio
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from utils.logger import setup_logger
from utils.telegram_fetcher import telegram_fetcher
from utils.crypto_detector import load_coins_from_coingecko, extract_cryptos
from utils.price_history_fetcher import fetch_price_history_coingecko


load_dotenv()
logger = setup_logger()

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_GROUPS = ["https://t.me/bob_alpha"] # Swap out this for the telegram chats you want to test
POST_LIMIT = 100


async def main():
    logger.info("Fetching coin list...")
    coin_map = load_coins_from_coingecko()

    logger.info("Fetching Telegram messages...")

    all_posts = pd.DataFrame()

    for group in TELEGRAM_GROUPS:
        posts = await telegram_fetcher(
            TELEGRAM_API_ID, TELEGRAM_API_HASH, group, POST_LIMIT
        )
        all_posts = pd.concat([all_posts, posts], ignore_index=True)

    results = []

    logger.info(
        f"Processing {len(all_posts)} messages with full post-to-present price history..."
    )

    for _, post in all_posts.iterrows():
        message = post["message_text"]
        cryptos = extract_cryptos(message, coin_map)
        post_date = pd.to_datetime(post["message_date"], utc=True)

        if not cryptos:
            continue

        try:
            record = {
                "sender_id": post["sender_id"],
                "message_timestamp": post_date.isoformat(),
                "message_text": post["message_text"],
                "mentioned_cryptos": [],
                "price_histories": {},
            }

            for symbol, coin_id in cryptos:
                price_df = fetch_price_history_coingecko(coin_id, post_date)
                if not price_df.empty:
                    price_df.index = price_df.index.tz_localize("UTC")

                initial_price = price_df.iloc[0]["price"]
                current_price = price_df.iloc[-1]["price"]
                max_price = price_df["price"].max()
                min_price = price_df["price"].min()
                hours_to_peak = (
                    price_df["price"].idxmax() - post_date
                ).total_seconds() / 3600
                percent_change = ((current_price - initial_price) / initial_price) * 100

                record["mentioned_cryptos"].append(symbol)
                record["price_histories"][symbol] = {
                    "coin_id": coin_id,
                    "initial_price": initial_price,
                    "current_price": current_price,
                    "max_price": max_price,
                    "min_price": min_price,
                    "hours_to_peak": hours_to_peak,
                    "percent_change": percent_change,
                    "timestamps": price_df.index.astype("int64").tolist(),
                    "prices": price_df["price"].tolist(),
                    "time_elapsed_hours": (
                        price_df.index[-1] - post_date
                    ).total_seconds()
                    / 3600,
                }

            results.append(record)

        except Exception as e:
            logger.warning(
                f"Failed to process message from {post['sender_id']}: {str(e)}"
            )
            continue

    class NumpyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return json.JSONEncoder.default(self, obj)

    with open("full_history.json", "w") as f:
        json.dump(results, f, cls=NumpyEncoder, indent=2)

    logger.info(f"Saved {len(results)} records with full price histories")


if __name__ == "__main__":
    asyncio.run(main())
    