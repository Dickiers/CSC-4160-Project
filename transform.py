import pandas as pd
import numpy as np 
import extract 

def calculate_rsi(data, window=14):
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()

    rs = gain / loss.replace(0, np.nan) 
    rsi = 100 - (100 / (1 + rs))

    return rsi

def performance_index(df):
    try:
        if df.empty: return None

        latest_price = df['close'].iloc[-1]

        def get_change(days_ago):
            if len(df) > days_ago:
                past_price = df['close'].iloc[-(days_ago + 1)]
                if past_price == 0: return 0.0 
                return ((latest_price - past_price) / past_price) * 100
            return 0.0

        snapshot = {
            '1day_change_percent': get_change(1),
            '5day_change_percent': get_change(5),
            'month_change_percent': get_change(21),
            'year_change_percent': get_change(252),
            '5year_change_percent': get_change(252 * 5),
            'ytd_change_percent': get_change(252) 
        }
        return snapshot
    except Exception as e:
        print(f"Error calculating performance: {e}")
        return None

def transform_profile_data(raw_profile, symbol):
    try:
        clean_data = {
            'Symbol': symbol,
            'Name': raw_profile.get('longName', symbol),
            'Industry': raw_profile.get('industry', 'N/A'),
            'MarketCapitalization': raw_profile.get('marketCap', 0),
            'PERatio': raw_profile.get('trailingPE', 0),
            '52WeekHigh': raw_profile.get('fiftyTwoWeekHigh', 0),
            '52WeekLow': raw_profile.get('fiftyTwoWeekLow', 0),
            'Description': raw_profile.get('longBusinessSummary', 'No description available.')
        }
        return clean_data
    except Exception as e:
        print(f"Profile Transform Error: {e}")
        return {}

def transform_all(raw_daily_data, raw_profile_data, symbol="Unknown"):

    clean_profile = transform_profile_data(raw_profile_data, symbol)

    if raw_daily_data is None or raw_daily_data.empty:
        return None, None, None, None

    df = raw_daily_data.copy()

    df.columns = [c.lower() for c in df.columns]
    df.index = pd.to_datetime(df.index)

    df = df.drop(columns=['dividends', 'stock splits'], errors='ignore')

    df['sma_50'] = df['close'].rolling(window=50).mean()
    df['sma_200'] = df['close'].rolling(window=200).mean()
    df['rsi_14'] = calculate_rsi(df, window=14)

    performance_snapshot = performance_index(df)

    df = df.round(2)

    if len(df) > 5000:
        df = df.iloc[-5000:]

    df = df.replace([np.inf, -np.inf], np.nan)

    transformed_daily_data = df.where(pd.notnull(df), None)

    latest_day = transformed_daily_data.iloc[-1]

    return transformed_daily_data, clean_profile, performance_snapshot, latest_day
