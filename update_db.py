import logging
import concurrent.futures
import time as time_module
from datetime import (
    datetime,
    timezone,
    time as dt_time,
)
from sqlalchemy import text
import extract
import transform_all
from db_connect import get_db_connection

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Multiple API keys for parallel processing
API_KEYS = [
    "9H50R39DJBQRD5O2",  # Your current key
    "M0DGWQX51UXIZODZ",  # Second key
    "Y5G02HX61IIRPSNH",  # Third key
    "43BOJ4PKNT5N3EF5",  # Fourth key
    "AWYW0KW8324RRW8P",  # Fifth key
    "23HYQH570BF26QPQ",  # Sixth key
]


def is_market_closed():
    """Check if US stock market is closed"""
    try:
        # Get current UTC time and convert to ET
        now_utc = datetime.now(timezone.utc)
        et_hour = (now_utc.hour - 5) % 24  # UTC-5 for ET
        current_et_time = dt_time(et_hour, now_utc.minute)

        # Market hours: 9:30 AM - 4:00 PM ET
        market_open = dt_time(9, 30)  # 9:30 AM ET
        market_close = dt_time(16, 0)  # 4:00 PM ET

        # Market is closed if:
        # - Before 9:30 AM ET OR after 4:00 PM ET
        is_closed = current_et_time < market_open or current_et_time > market_close
        return is_closed

    except Exception as e:
        logger.error(f"❌ Error checking market hours: {str(e)}")
        return True


def update_stock_profile_bulk(engine, all_profile_data):
    """Bulk update stock_profile table for all companies"""
    try:
        with engine.connect() as conn:
            for i, profile_data in enumerate(all_profile_data, 1):
                symbol = profile_data.get("Symbol")
                if not symbol:
                    continue
                market_cap = profile_data.get("MarketCapitalization")
                pe_ratio = profile_data.get("PERatio")
                week_high = profile_data.get("52WeekHigh")
                week_low = profile_data.get("52WeekLow")

                conn.execute(
                    text("""
                    UPDATE stock_profile 
                    SET "MarketCapitalization" = :market_cap,
                        "PERatio" = :pe_ratio,
                        "52WeekHigh" = :week_high,
                        "52WeekLow" = :week_low
                    WHERE "Symbol" = :symbol
                """),
                    {
                        "symbol": symbol,
                        "market_cap": market_cap,
                        "pe_ratio": pe_ratio,
                        "week_high": week_high,
                        "week_low": week_low,
                    },
                )
            conn.commit()

    except Exception as e:
        logger.error(f"❌ Error bulk updating stock_profile: {str(e)}")
        raise e


def update_stock_performance_bulk(engine, all_performance_data):
    """Bulk update stock_performance table for all companies"""
    try:
        with engine.connect() as conn:
            for i, performance_data in enumerate(all_performance_data, 1):
                symbol = performance_data.get("symbol")
                if not symbol:
                    continue
                params = {
                    "symbol": symbol,
                    "change_1day": performance_data.get("1day_change_percent"),
                    "change_5day": performance_data.get("5day_change_percent"),
                    "change_1month": performance_data.get("month_change_percent"),
                    "change_1year": performance_data.get("year_change_percent"),
                    "change_5year": performance_data.get("5year_change_percent"),
                    "change_ytd": performance_data.get("ytd_change_percent"),
                }

                conn.execute(
                    text("""
                    UPDATE stock_performance 
                    SET "1day_change_percent" = :change_1day,
                        "5day_change_percent"  = :change_5day,
                        "month_change_percent"  = :change_1month,
                        "year_change_percent"  = :change_1year,
                        "5year_change_percent"  = :change_5year,
                        "ytd_change_percent"  = :change_ytd
                    WHERE "symbol" = :symbol
                """),
                    params,
                )

            conn.commit()

    except Exception as e:
        logger.error(f"❌ Error bulk updating stock_performance: {str(e)}")
        raise e


def add_daily_data_bulk(engine, all_daily_data):
    """Bulk add daily data for all companies"""
    try:
        with engine.connect() as conn:
            for i, daily_series in enumerate(all_daily_data, 1):
                symbol = (
                    daily_series.get("symbol")
                    if isinstance(daily_series, dict)
                    else daily_series.get("symbol", "UNKNOWN")
                )
                data_dict = (
                    daily_series.to_dict()
                    if hasattr(daily_series, "to_dict")
                    else daily_series
                )
                # Get the date for this record
                record_date = (
                    daily_series.name.date()
                    if hasattr(daily_series, "name")
                    else datetime.now().date()
                )
                # Check if data already exists for this symbol and date
                existing_data = conn.execute(
                    text("""
                        SELECT 1 FROM stock_daily_data 
                        WHERE symbol = :symbol AND date = :date
                        LIMIT 1
                    """),
                    {"symbol": symbol, "date": record_date},
                ).fetchone()

                if existing_data:
                    continue

                conn.execute(
                    text("""
                    INSERT INTO stock_daily_data 
                    (symbol, date, open, high, low, close, volume, sma_50, sma_200, rsi_14)
                    VALUES (:symbol, :date, :open, :high, :low, :close, :volume, :sma_50, :sma_200, :rsi_14)
                    ON CONFLICT (symbol, date) 
                    DO UPDATE SET 
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        sma_50 = EXCLUDED.sma_50,
                        sma_200 = EXCLUDED.sma_200,
                        rsi_14 = EXCLUDED.rsi_14
                    """),
                    {
                        "symbol": symbol,
                        "date": daily_series.name.date()
                        if hasattr(daily_series, "name")
                        else datetime.now().date(),
                        "open": data_dict.get("open"),
                        "high": data_dict.get("high"),
                        "low": data_dict.get("low"),
                        "close": data_dict.get("close"),
                        "volume": data_dict.get("volume"),
                        "sma_50": data_dict.get("sma_50"),
                        "sma_200": data_dict.get("sma_200"),
                        "rsi_14": data_dict.get("rsi_14"),
                    },
                )

            conn.commit()
    except Exception as e:
        logger.error(f"❌ Error bulk adding daily data: {str(e)}")
        raise e


def process_company_batch(companies, api_key, batch_name):
    """
    Process a batch of companies using one API key
    """
    batch_results = {
        "todays_data": [],
        "profile_data": [],
        "performance_data": [],
        "successful": [],
        "failed": [],
    }

    for i, symbol in enumerate(companies, 1):
        try:
            raw_daily_data, raw_profile_data = extract.extract_data(api_key, symbol)

            if raw_daily_data is None or raw_profile_data is None:
                batch_results["failed"].append(symbol)
                continue

            transformed_data, clean_profile, performance_snapshot = (
                transform_all.change_all(raw_daily_data, raw_profile_data, symbol)
            )

            if (
                transformed_data is not None
                and clean_profile is not None
                and performance_snapshot is not None
            ):
                # Extract today's data (most recent row)
                todays_data_series = transformed_data.iloc[0]
                batch_results["todays_data"].append(todays_data_series)
                batch_results["profile_data"].append(clean_profile)
                batch_results["performance_data"].append(performance_snapshot)
                batch_results["successful"].append(symbol)
            else:
                logger.error(
                    f"    ❌ {batch_name} Failed to transform data for {symbol}"
                )
                batch_results["failed"].append(symbol)

            # Rate limiting between companies for the same API key
            if i < len(companies):
                time_module.sleep(12)

        except Exception as e:
            logger.error(f"    ❌ {batch_name} Error processing {symbol}: {str(e)}")
            batch_results["failed"].append(symbol)
    return batch_results


def process_all_companies_parallel_6_keys():
    """
    Process all companies in parallel using 6 API keys simultaneously
    Each key processes 2 companies
    """
    COMPANIES = [
        "NVDA",
        "AAPL",
        "MSFT",
        "GOOG",
        "AMZN",
        "AVGO",
        "META",
        "TSLA",
        "BRK-B",
        "JPM",
        "WMT",
        "V",
    ]

    # Split companies into 6 batches (2 companies each)
    batches = []
    for i in range(len(API_KEYS)):
        batch = COMPANIES[i :: len(API_KEYS)]  # Round-robin distribution
        batches.append(batch)

    all_results = {
        "todays_data": [],
        "profile_data": [],
        "performance_data": [],
        "successful": [],
        "failed": [],
    }

    # Process all batches in parallel with 6 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
        # Submit all batches for parallel execution
        future_to_batch = {}
        for i, batch in enumerate(batches):
            if batch:  # Only submit if batch has companies
                future = executor.submit(
                    process_company_batch, batch, API_KEYS[i], f"Key-{i + 1}"
                )
                future_to_batch[future] = i

        # Collect results as they complete
        completed_batches = 0
        for future in concurrent.futures.as_completed(future_to_batch):
            batch_index = future_to_batch[future]
            completed_batches += 1
            try:
                batch_result = future.result()
                # Combine results
                all_results["todays_data"].extend(batch_result["todays_data"])
                all_results["profile_data"].extend(batch_result["profile_data"])
                all_results["performance_data"].extend(batch_result["performance_data"])
                all_results["successful"].extend(batch_result["successful"])
                all_results["failed"].extend(batch_result["failed"])
            except Exception as e:
                logger.error(f"❌ Batch {batch_index + 1} failed: {str(e)}")
                return e

    return all_results


def update_all_companies_bulk():
    """
    Main function that:
    1) Processes all companies using 6 API keys in parallel
    2) Combines the data
    3) Does bulk changes in the database
    """
    try:
        if not is_market_closed():
            return {"successful": [], "failed": [], "total_processed": 0}

        # 1) Process all companies in parallel with 6 keys
        processed_data = process_all_companies_parallel_6_keys()

        if not processed_data["todays_data"]:
            return processed_data
        # 2) Get database connection
        engine = get_db_connection()

        try:
            # 3) Bulk update all tables
            update_stock_profile_bulk(engine, processed_data["profile_data"])
            update_stock_performance_bulk(engine, processed_data["performance_data"])
            add_daily_data_bulk(engine, processed_data["todays_data"])
        finally:
            engine.dispose()

        final_result = {
            "successful": processed_data["successful"],
            "failed": processed_data["failed"],
            "total_processed": len(processed_data["successful"])
            + len(processed_data["failed"]),
        }
        return final_result

    except Exception as e:
        logger.error(f"❌ Bulk update failed: {str(e)}")
        return {"successful": [], "failed": [], "total_processed": 0, "error": str(e)}
