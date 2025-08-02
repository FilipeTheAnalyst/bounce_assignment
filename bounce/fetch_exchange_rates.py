import pandas as pd
import requests
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Load from .env or environment variable
API_KEY = os.getenv("OPENEXCHANGE_APP_ID")
BASE_URL = "https://openexchangerates.org/api/historical"

# Load events.csv
events_df = pd.read_csv("seeds/raw_events.csv")
events_df["event_date"] = pd.to_datetime(events_df["event_time"]).dt.date

# Get unique date/currency pairs
date_currency_df = events_df[["event_date", "currency"]].dropna().drop_duplicates()

# Fetch rates
records = []

for date, sub_df in date_currency_df.groupby("event_date"):
    date_str = str(date)
    symbols = ",".join(sub_df["currency"].unique())
    
    url = f"{BASE_URL}/{date_str}.json"
    params = {
        "app_id": API_KEY,
        "base": "USD",
        "symbols": symbols
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Failed to fetch data for {date_str}: {response.text}")
        continue

    data = response.json()
    for currency, rate in data.get("rates", {}).items():
        records.append({
            "date": date_str,
            "currency": currency,
            "rate_to_usd": 1 / rate if rate != 0 else None  # reverse conversion
        })

# Save to CSV for dbt seed
output_path = Path("seeds/exchange_rates.csv")
pd.DataFrame(records).to_csv(output_path, index=False)
print(f"Exchange rates written to {output_path}")