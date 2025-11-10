import pandas as pd

import extract

def calculate_rsi(data, window = 14):
    delta = data['close'].diff()
    
    gain = delta.copy()
    loss = delta.copy()
    gain[gain < 0] = 0
    loss[loss > 0] = 0
    
    avg_gain = gain.rolling(window = window, min_periods = 1).mean()
    avg_loss = loss.abs().rolling(window = window, min_periods = 1).mean()
    
    rs = avg_gain / avg_loss
    
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def performance_index(df):
    try:
        latest_data = df.iloc[-1]
        
        price_today = latest_data['close']
        price_1_day_ago = df['close'].iloc[-2]
        price_5_days_ago = df['close'].iloc[-6]
        price_1_month_ago = df['close'].iloc[-22]
        price_1_year_ago = df['close'].iloc[-253]
        price_5_years_ago = df['close'].iloc[-1261]
        
        year_start_price = df[df.index.year == latest_data.name.year]['close'].iloc[0]
        
        change_1_day = (price_today - price_1_day_ago) / price_1_day_ago * 100
        change_5_day = (price_today - price_5_days_ago) / price_5_days_ago * 100
        change_1_month = (price_today - price_1_month_ago) / price_1_month_ago * 100
        change_1_year = (price_today - price_1_year_ago) / price_1_year_ago * 100
        change_5_year = (price_today - price_5_years_ago) / price_5_years_ago * 100
        change_ytd = (price_today - year_start_price) / year_start_price * 100
        
        snapshot = {
            '1day_change_percent': change_1_day,
            '5day_change_percent': change_5_day,
            'month_change_percent': change_1_month,
            'year_change_percent': change_1_year,
            '5year_change_percent': change_5_year,
            'ytd_change_percent': change_ytd
        }
        
        return snapshot
    except:
        print("Error calculating performance!")
        return None

def transform_profile_data(raw_profile_data):
    if raw_profile_data is None:
        print("Received No Profile Data")
    
    wanted_keys = ['Symbol', 'Name', 'Industry', 'MarketCapitalization', 'PERatio', '52WeekHigh', '52WeekLow', 'Description']
    
    clean_data = {}
    for key in wanted_keys:
        clean_data[key] = raw_profile_data.get(key, None)
    
    return clean_data

def transform_all(raw_daily_data, raw_profile_data):
    
    tramsformed_profile_data = transform_profile_data(raw_profile_data)
    
    if raw_daily_data is None:
        print("Received No Daily Data")
        return None
    
    df = raw_daily_data.copy()
    
    df.rename(columns = {
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. volume': 'volume'
    }, inplace = True)
    
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col])
    
    df.sort_index(ascending = True, inplace = True)
    df.index = pd.to_datetime(df.index)
    
    transformed_performance_index = performance_index(df)
    
    df['sma_50'] = df['close'].rolling(window = 50).mean()
    df['sma_200'] = df['close'].rolling(window = 200).mean()
    df['rsi_14'] = calculate_rsi(df, window=14)
    
    df.sort_index(ascending=False, inplace=True)
    
    transformed_daily_data = df.dropna().round(4)
    
    return transformed_daily_data, transform_profile_data, transformed_performance_index

if __name__ == "__main__":
    API_KEY = 'M0DGWQX51UXIZODZ' 
    STOCK_TICKER = 'MSFT'
    
    raw_daily_data, raw_profile_data = extract.extract_data(API_KEY, STOCK_TICKER)
    
    if raw_daily_data is not None and raw_profile_data is not None:
        transformed_data, clean_profile, performance_snapshot = transform_all(raw_daily_data, raw_profile_data)
        
        print(transformed_data.head())

        print(clean_profile)

        print(performance_snapshot)
    else:
        print("Transform Failed")