import re
import requests


def load_coins_from_coingecko():
    url = "https://api.coingecko.com/api/v3/coins/list"
    response = requests.get(url)
    coins = response.json()
    symbol_map = {coin["symbol"].upper(): coin["id"] for coin in coins}
    return symbol_map


def extract_cryptos(text, symbol_map):
    found = set()
    clean_text = re.sub(r"[^\x00-\x7F]+", "", text)
    matches = re.findall(r"[\$#]([A-Za-z]{2,8})\b", clean_text)
    for match in matches:
        if match in symbol_map:
            found.add((match, symbol_map[match]))
    return list(found)
