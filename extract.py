import time
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData

def fetch_daily_prices(ts, ticker):
    try:
        daily_data, meta_data = ts.get_daily(symbol = ticker, outputsize = 'full')
        return daily_data
    except:
        print("Extract Daily Prices Error!")
        return None

def fetch_company_profile(fd, ticker):
    try:
        profile_data, meta_data = fd.get_company_overview(symbol = ticker)
        return profile_data
    except:
        print("Extract Company Profile Error!")
        return None

def extract_data(api_key, ticker):
    ts = TimeSeries(key = api_key, output_format = 'pandas')
    fd = FundamentalData(key = api_key)
    
    raw_daily_data = fetch_daily_prices(ts, ticker)
    
    time.sleep(15)
    
    raw_profile_data = fetch_company_profile(fd, ticker)
    
    return raw_daily_data, raw_profile_data

if __name__ == "__main__":
    API_KEY = 'M0DGWQX51UXIZODZ'
    STOCK_TICKER = "MSFT"
    
    daily_data, profile_data = extract_data(API_KEY, STOCK_TICKER)

    print(daily_data.head())
    
    print(profile_data)