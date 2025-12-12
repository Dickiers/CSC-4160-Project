import yfinance as yf

def extract_data(api_key, ticker):
    try:
        print(f"Fetching data for {ticker}")

        stock = yf.Ticker(ticker)

        raw_daily_data = stock.history(period="max", auto_adjust=True)

        if raw_daily_data.empty:
            print(f"No price data found for {ticker}")
            return None, None

        try:
            raw_profile_data = stock.info
        except Exception:
            print(f"Could not fetch profile for {ticker}, using defaults")
            raw_profile_data = {'symbol': ticker}

        return raw_daily_data, raw_profile_data

    except Exception as e:
        print(f"Error extracting data for {ticker}: {e}")
        return None, None
