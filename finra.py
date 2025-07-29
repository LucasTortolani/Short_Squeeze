import os
import pandas as pd
from supabase import create_client
from datetime import datetime, timedelta, timezone
from config import SUPABASE_URL, SUPABASE_KEY, RELEVANT_TICKERS, FILE_NAME

# --- CONFIGURE ---
home_dir = os.path.expanduser("~")

# Build the full path to the file in the Downloads folder
file_name = FILE_NAME  # add extension if needed, e.g., .csv or .txt
CSV_PATH = os.path.join(home_dir, "Downloads", file_name)

# --- INIT SUPABASE ---
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- LOAD CSV ---
try:
    df = pd.read_csv(CSV_PATH, delimiter='|')
    print(f"✅ Loaded {len(df)} records from CSV")
except FileNotFoundError:
    print(f"❌ File not found: {CSV_PATH}")
    exit(1)
except Exception as e:
    print(f"❌ Error loading CSV: {e}")
    exit(1)

# CLEAN & CONVERT TYPES
df = df.dropna(subset=["symbolCode", "currentShortPositionQuantity", "averageDailyVolumeQuantity", "settlementDate"])
df["currentShortPositionQuantity"] = pd.to_numeric(df["currentShortPositionQuantity"], errors='coerce')
df["previousShortPositionQuantity"] = pd.to_numeric(df["previousShortPositionQuantity"], errors='coerce')
df["averageDailyVolumeQuantity"] = pd.to_numeric(df["averageDailyVolumeQuantity"], errors='coerce')
df["daysToCoverQuantity"] = pd.to_numeric(df["daysToCoverQuantity"], errors='coerce')
df["changePercent"] = pd.to_numeric(df["changePercent"], errors='coerce')
df["settlementDate"] = pd.to_datetime(df["settlementDate"])

# FILTER TO WATCHLIST
filtered = df[df["symbolCode"].isin(RELEVANT_TICKERS)]

if filtered.empty:
    print("⚠️ No matching tickers found in CSV")
    exit(0)

print(f"📊 Processing {len(filtered)} tickers: {list(filtered['symbolCode'].unique())}")

today = datetime.now(timezone.utc).date()

for _, row in filtered.iterrows():
    symbol = row["symbolCode"]
    settlement_date = row["settlementDate"].date()

    # Start updating from settlement date + 1 day through today
    start_date = settlement_date + timedelta(days=1)

    current_date = start_date
    while current_date <= today:
        # Prepare update payload
        payload = {
            "finra_current_short": int(row["currentShortPositionQuantity"]),
            "finra_previous_short": int(row["previousShortPositionQuantity"]) if not pd.isna(row["previousShortPositionQuantity"]) else None,
            "finra_stock_split_flag": row["stockSplitFlag"] if pd.notna(row["stockSplitFlag"]) else None,
            "finra_avg_daily_volume": int(row["averageDailyVolumeQuantity"]),
            "finra_days_to_cover": float(row["daysToCoverQuantity"]),
            "finra_change_percent": float(row["changePercent"]),
            "finra_settlement_date": settlement_date.isoformat(),
        }

        try:
            # Update the row in Supabase where ticker and timestamp match
            response = supabase.table("stock_metrics") \
                .update(payload) \
                .eq("ticker", symbol) \
                .gte("timestamp", current_date.isoformat() + "T00:00:00Z") \
                .lt("timestamp", (current_date + timedelta(days=1)).isoformat() + "T00:00:00Z") \
                .execute()

            # Check if the update was successful
            if response.data:
                print(f"✅ Updated {symbol} for {current_date} ({len(response.data)} records)")
            else:
                print(f"⚠️ No records found to update for {symbol} on {current_date}")
                
        except Exception as e:
            print(f"❌ Failed to update {symbol} on {current_date}: {e}")

        current_date += timedelta(days=1)

print("✅ FINRA data processing completed.")