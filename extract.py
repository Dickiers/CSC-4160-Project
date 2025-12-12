import requests
import pandas as pd
import random
import time

API_KEYS = [
    "34IY3KX670WFPGRH",  
    "43BOJ4PKNT5N3EF5",
    "23HYQH570BF26QPQ",
    "AWYW0KW8324RRW8P"
]

def get_random_key():
    return random.choice(API_KEYS)

def extract_data(ignored_key, ticker):
    try:
        current_key = get_random_key()
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&outputsize=compact&apikey={current_key}"
        print(f"Requesting price for {ticker}")

        r = requests.get(url)
        data = r.json()

        if "Time Series (Daily)" not in data:
            print(f"API error for {ticker}: {data.get('Note', 'Unknown Error')}")
            return None, None

        df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient='index')
        df = df.rename(columns={
            "1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close", "5. volume": "volume"
        })
        df = df.astype(float)
        df.index = pd.to_datetime(df.index)

        return df, None

    except Exception as e:
        print(f"Error extracting price for {ticker}: {e}")
        return None, None

def extract_profile_data(ticker):
    try:
        current_key = get_random_key()
        url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={current_key}"
        print(f"Requesting profile for {ticker}")

        r = requests.get(url)
        data = r.json()

        if not data or "Symbol" not in data:
            print(f"Profile not found for {ticker}")
            return None

        return data

    except Exception as e:
        print(f"Error extracting profile for {ticker}: {e}")
        return None

if __name__ == "__main__":
    TEST_SYMBOL = "NVDA"
    print(f"Testing extraction for {TEST_SYMBOL}")

    df, _ = extract_data(None, TEST_SYMBOL)
    if df is not None:
        print("Price data fetched:")
        print(df.head(3))
        print(f"Latest date: {df.index[0]}")
    else:
        print("Price data failed.")

    print("Testing profile extraction")
    profile = extract_profile_data(TEST_SYMBOL)
    if profile:
        print("Profile data fetched:")
        print(f"Name: {profile.get('Name')}")
        print(f"MarketCap: {profile.get('MarketCapitalization')}")
    else:
        print("Profile data failed.")
