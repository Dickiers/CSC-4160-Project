import yfinance as yf

def extract_data(api_key, ticker):
    """
    Fetches data using standard yfinance (Fast Mode for Local Backfill).
    """
    try:
        print(f"   ... Fetching {ticker} data...")

        stock = yf.Ticker(ticker)

        raw_daily_data = stock.history(period="max", auto_adjust=True)

        if raw_daily_data.empty:
            print(f"❌ ERROR: No price data found for {ticker}")
            return None, None

        try:
            raw_profile_data = stock.info
        except Exception:
            print(f"⚠️ Warning: Could not fetch profile for {ticker}, using defaults.")
            raw_profile_data = {'symbol': ticker} 

        return raw_daily_data, raw_profile_data

    except Exception as e:
        print(f"❌ EXTRACTION ERROR ({ticker}): {str(e)}")
        return None, None