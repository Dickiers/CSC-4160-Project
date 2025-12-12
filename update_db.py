import logging
import json
import requests
import pandas as pd
import numpy as np
import google.auth.transport.requests
from google.oauth2 import service_account
from datetime import datetime
import math

import extract
import transform

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROJECT_ID = "stock-market-etl-2f750"
FIRESTORE_URL = f"https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/(default)/documents/stock_data"

def parse_firestore_field(field):
    key = list(field.keys())[0]
    value = field[key]
    if key == "mapValue": return {k: parse_firestore_field(v) for k, v in value["fields"].items()}
    elif key == "arrayValue": return [parse_firestore_field(v) for v in value.get("values", [])]
    elif key == "stringValue": return value
    elif key == "integerValue": return int(value)
    elif key == "doubleValue": return float(value)
    elif key == "booleanValue": return value
    elif key == "timestampValue": return value
    elif key == "nullValue": return None
    return str(value)

def to_firestore_value(value):
    if value is None: return {"nullValue": None}
    elif isinstance(value, float):
        if math.isnan(value) or math.isinf(value): return {"nullValue": None}
        return {"doubleValue": value}
    elif isinstance(value, bool): return {"booleanValue": value}
    elif isinstance(value, int): return {"integerValue": str(value)}
    elif isinstance(value, str): return {"stringValue": value}
    elif isinstance(value, list): return {"arrayValue": {"values": [to_firestore_value(v) for v in value]}}
    elif isinstance(value, dict): return {"mapValue": {"fields": {k: to_firestore_value(v) for k, v in value.items()}}}
    else: return {"stringValue": str(value)}

def get_current_data(symbol, headers):
    try:
        response = requests.get(f"{FIRESTORE_URL}/{symbol}", headers=headers)
        if response.status_code == 200:
            doc = response.json()
            if "fields" in doc:
                fields = doc["fields"]
                daily_raw = parse_firestore_field(fields.get("transformed_daily_data", {}))
                profile_raw = parse_firestore_field(fields.get("clean_profile", {}))
                if daily_raw:
                    df = pd.DataFrame(daily_raw)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                    return df, profile_raw
        return None, None
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None, None

def save_to_firebase_rest(symbol, daily_data, profile, performance, headers):
    try:
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
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Save error for {symbol}: {e}")
        return False

def lambda_handler(event, context):
    print("Starting daily update")
    
    creds = service_account.Credentials.from_service_account_file('serviceAccountKey.json', scopes=['https://www.googleapis.com/auth/datastore'])
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    headers = {"Authorization": f"Bearer {creds.token}"}
    
    COMPANIES = ["NVDA", "AAPL", "MSFT", "GOOG", "AMZN", "AVGO", "META", "TSLA", "BRK-B", "JPM", "WMT", "V"]
    
    for symbol in COMPANIES:
        try:
            print(f"Updating {symbol}")
            
            existing_df, existing_profile = get_current_data(symbol, headers)
            
            new_df, _ = extract.extract_data(None, symbol)
            new_profile_raw = extract.extract_profile_data(symbol)
            
            profile_to_use = new_profile_raw if new_profile_raw else existing_profile

            if new_df is not None and not new_df.empty:
                if existing_df is not None and not existing_df.empty:
                    combined_df = pd.concat([existing_df, new_df])
                    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                    combined_df = combined_df.sort_index()
                else:
                    combined_df = new_df

                trans_daily, trans_profile, trans_perf, _ = transform.transform_all(combined_df, profile_to_use, symbol)
                
                if trans_daily is not None:
                    save_to_firebase_rest(symbol, trans_daily, trans_profile, trans_perf, headers)
                    print(f"Updated {symbol}")
            else:
                print(f"No new price data for {symbol}")

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    return {'statusCode': 200, 'body': 'Update Complete'}

if __name__ == "__main__":
    lambda_handler(None, None)
