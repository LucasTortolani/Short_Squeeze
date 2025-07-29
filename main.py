import yfinance as yf
import pandas as pd
import numpy as np
from supabase import create_client
from datetime import datetime, timezone
from config import SUPABASE_URL, SUPABASE_KEY, RELEVANT_TICKERS
import time

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

tickers = RELEVANT_TICKERS

def calculate_volatility(df, window=14):
    """Calculate annualized volatility based on daily returns"""
    if len(df) < window:
        return None
    df['returns'] = df['Close'].pct_change()
    vol = df['returns'].rolling(window).std() * np.sqrt(252)
    return vol.iloc[-1] if not pd.isna(vol.iloc[-1]) else None

def safe_get_info_value(info, key, default=None):
    """Safely get value from stock info dict"""
    try:
        value = info.get(key, default)
        return value if value is not None and not pd.isna(value) else default
    except:
        return default

print(f"📊 Processing {len(tickers)} tickers...")

successful_inserts = 0
failed_inserts = 0

for i, ticker in enumerate(tickers, 1):
    print(f"Processing {ticker} ({i}/{len(tickers)})...")
    
    try:
        stock = yf.Ticker(ticker)

        # Get historical data
        hist = stock.history(period="30d", interval="1d")
        if hist.empty:
            print(f"⚠️ No historical data for {ticker}, skipping.")
            failed_inserts += 1
            continue

        # Basic price and volume data
        price = float(hist['Close'].iloc[-1])
        volume = int(hist['Volume'].iloc[-1])
        volatility = calculate_volatility(hist)

        # Get stock info with error handling
        try:
            info = stock.info
        except Exception as e:
            print(f"⚠️ Could not fetch info for {ticker}: {e}")
            info = {}

        # Extract info safely
        short_interest = safe_get_info_value(info, "shortPercentOfFloat")
        float_shares = safe_get_info_value(info, "floatShares")

        # Calculate days to cover
        days_to_cover = None
        if short_interest and float_shares and volume > 0:
            try:
                short_shares = short_interest * float_shares
                days_to_cover = short_shares / volume
            except (TypeError, ZeroDivisionError):
                days_to_cover = None

        # Calculate spikes
        prev_close = float(hist['Close'].iloc[-2]) if len(hist) > 1 else price
        price_spike = price > prev_close * 1.05

        avg_volume_10d = hist['Volume'].tail(10).mean()
        volume_spike = volume > avg_volume_10d * 2 if not pd.isna(avg_volume_10d) else False

        # Prepare data payload
        data = {
            "ticker": ticker,
            "price": price,
            "volume": volume,
            "short_interest": short_interest,
            "float_shares": int(float_shares) if float_shares else None,
            "days_to_cover": float(days_to_cover) if days_to_cover else None,
            "price_spike": int(price_spike),
            "volume_spike": int(volume_spike),
            "volatility_14d": float(volatility) if volatility else None,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Insert to database
        try:
            response = supabase.table("stock_metrics").insert(data).execute()
            if response.data:
                print(f"✅ Inserted data for {ticker}")
                successful_inserts += 1
            else:
                print(f"⚠️ No data returned for {ticker} insert")
                failed_inserts += 1
        except Exception as e:
            print(f"❌ Error inserting data for {ticker}: {e}")
            failed_inserts += 1

    except Exception as e:
        print(f"❌ Error processing {ticker}: {e}")
        failed_inserts += 1
    
    # Small delay to avoid rate limiting
    if i < len(tickers):
        time.sleep(0.1)

print(f"\n📈 Processing complete!")
print(f"✅ Successful: {successful_inserts}")
print(f"❌ Failed: {failed_inserts}")
print(f"📊 Total: {len(tickers)}")