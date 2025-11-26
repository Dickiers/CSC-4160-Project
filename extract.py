import requests
import pandas as pd

API_KEY = "34IY3KX670WFPGRH" 

def extract_data(api_key_ignored, ticker):
    """
    Fetches the last 100 days of data from Alpha Vantage (Compact Mode).
    """
    try:

        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&outputsize=compact&apikey={API_KEY}"
        print(f"   ... (AlphaVantage) Requesting {ticker}...")

        r = requests.get(url)
        data = r.json()

        if "Time Series (Daily)" not in data:
            print(f"❌ API Error for {ticker}: {data.get('Note', data.get('Information', 'Unknown Error'))}")
            return None, None

        df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient='index')

        df = df.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume"
        })

        df = df.astype(float)
        df.index = pd.to_datetime(df.index)

        return df, None

    except Exception as e:
        print(f"❌ EXTRACTION ERROR ({ticker}): {str(e)}")
        return None, None
