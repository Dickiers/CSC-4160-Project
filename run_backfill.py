import logging
import json
import requests
import google.auth.transport.requests
from google.oauth2 import service_account
from datetime import datetime
import math

import extract_backfill as extract
import transform

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROJECT_ID = "stock-market-etl-2f750"
FIRESTORE_URL = f"https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/(default)/documents/stock_data"

def to_firestore_value(value):
    if value is None:
        return {"nullValue": None}
    
    elif isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return {"nullValue": None}
        return {"doubleValue": value}
    
    elif isinstance(value, bool): return {"booleanValue": value}
    elif isinstance(value, int): return {"integerValue": str(value)}
    elif isinstance(value, str): return {"stringValue": value}
    elif isinstance(value, list):
        return {"arrayValue": {"values": [to_firestore_value(v) for v in value]}}
    elif isinstance(value, dict):
        return {"mapValue": {"fields": {k: to_firestore_value(v) for k, v in value.items()}}}
    else: return {"stringValue": str(value)}

def save_to_firebase_rest(symbol, daily_data, profile, performance):
    try:
        creds = service_account.Credentials.from_service_account_file('serviceAccountKey.json', scopes=['https://www.googleapis.com/auth/datastore'])
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        headers = {"Authorization": f"Bearer {creds.token}"}

        daily_df = daily_data.copy()
        daily_df.index.name = 'date'
        daily_df.index = daily_df.index.strftime('%Y-%m-%d')
        daily_records = daily_df.reset_index().to_dict(orient='records')

        doc_data = {
            "fields": {
                "symbol": to_firestore_value(symbol),
                "clean_profile": to_firestore_value(profile),
                "transformed_performance_index": to_firestore_value(performance),
                "transformed_daily_data": to_firestore_value(daily_records),
                "last_updated": {"timestampValue": datetime.utcnow().isoformat() + "Z"}
            }
        }
        
        response = requests.patch(f"{FIRESTORE_URL}/{symbol}", headers=headers, json=doc_data)
        
        if response.status_code == 200:
            print(f"Saved {symbol}")
            return True
        else:
            print(f"API error for {symbol}: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error saving {symbol}: {e}")
        return False

def main():
    print("Starting ETL pipeline")
    
    COMPANIES = ["NVDA", "AAPL", "MSFT", "GOOG", "AMZN", "AVGO", "META", "TSLA", "BRK-B", "JPM", "WMT", "V"]
    
    for symbol in COMPANIES:
        try:
            print(f"Processing {symbol}")
            
            raw_daily, raw_profile = extract.extract_data(None, symbol)
            
            if raw_daily is not None:
                trans_daily, trans_profile, trans_perf, _ = transform.transform_all(raw_daily, raw_profile, symbol)
                
                if trans_daily is not None:
                    save_to_firebase_rest(symbol, trans_daily, trans_profile, trans_perf)
                else:
                    print(f"Failed to transform {symbol}")
            else:
                print(f"Skipped {symbol} due to extraction failure")

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    print("Backfill complete")

if __name__ == "__main__":
    main()
