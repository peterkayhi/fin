import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────

tickers = [
    'VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO', 'VNQ',
    'PDBC', 'IAU', 'EDV', 'VGIT', 'VCLT', 'BNDX'
]

# Analysis period (trading days inside this range)
start_date = '2025-12-30'
end_date   = '2026-03-09'   # exclusive upper bound

# Fetch extra history so we can calculate 252 trading days back
fetch_start = '2024-11-01'

# Lookback periods in trading days
periods = [63, 126, 252]
period_names = ['63', '126', '252']

# ← change filename as needed
output_file = '/Users/peterkay/Downloads/dailyMomen20260309.csv'  

# ────────────────────────────────────────────────
# DOWNLOAD DATA
# ────────────────────────────────────────────────

print("Downloading data from yfinance (auto_adjust=True)...")
data = yf.download(
    tickers,
    start=fetch_start,
    end=end_date,
    auto_adjust=True,           # makes 'Close' fully adjusted
    progress=False
)

# ────────────────────────────────────────────────
# Extract adjusted closing prices
# ────────────────────────────────────────────────

print("\nColumns structure:", data.columns)
print("Data shape:", data.shape)

if isinstance(data.columns, pd.MultiIndex):
    if 'Close' in data.columns.levels[0]:
        adj_close = data['Close']
    elif 'Adj Close' in data.columns.levels[0]:
        adj_close = data['Adj Close']
    else:
        raise ValueError("Cannot find 'Close' or 'Adj Close'")
else:
    if 'Close' in data.columns:
        adj_close = data['Close']
    else:
        raise ValueError("No 'Close' column found")

# Clean column names to just tickers
if isinstance(adj_close.columns, pd.MultiIndex):
    adj_close.columns = adj_close.columns.get_level_values(1)
elif len(tickers) == 1:
    adj_close = adj_close.to_frame(name=tickers[0])

print("Adjusted close columns:", adj_close.columns.tolist())

# ────────────────────────────────────────────────
# Prepare trading calendar
# ────────────────────────────────────────────────

trading_dates = adj_close.index.sort_values()
trading_dates = trading_dates[trading_dates >= pd.to_datetime(start_date)]
trading_dates = trading_dates[trading_dates < pd.to_datetime(end_date)]

print(f"Found {len(trading_dates)} trading days in range")

# ────────────────────────────────────────────────
# Calculate momentum rows — now for EVERY trading day
# ────────────────────────────────────────────────

rows = []

for as_of_date in trading_dates:           # ← changed from monthly_ends
    for ticker in tickers:
        if ticker not in adj_close.columns:
            continue

        try:
            current_close = adj_close.loc[as_of_date, ticker]
            if pd.isna(current_close):
                continue

            idx = adj_close.index.get_loc(as_of_date)

            past_dates  = []
            past_closes = []
            gains       = []

            for days_back in periods:
                if idx < days_back:
                    continue   # not enough history — skip this period (or you could skip whole row)

                past_idx = idx - days_back
                past_date = adj_close.index[past_idx]
                past_close = adj_close.iloc[past_idx][ticker]

                if pd.isna(past_close):
                    continue

                gain_pct = (current_close - past_close) / past_close * 100

                past_dates.append(past_date)
                past_closes.append(past_close)
                gains.append(gain_pct)

            # Only create row if we have at least some periods
            if not gains:
                continue

            papa_avg = np.mean(gains)

            row = {
                'Symbol': ticker,
                'As of Date': as_of_date,
                'Close': round(float(current_close), 2),
                'Papa Avg': papa_avg,
            }

            for i, name in enumerate(period_names):
                # Only add columns for periods we actually calculated
                if i < len(past_dates):
                    row[name]                = past_dates[i].strftime('%m/%d/%Y')
                    row[f'{name} Day Close'] = round(float(past_closes[i]), 2)
                    row[f'{name} day gain']  = gains[i]

            rows.append(row)

        except (KeyError, ValueError) as e:
            continue

# ────────────────────────────────────────────────
# Build, sort, format and save
# ────────────────────────────────────────────────

if not rows:
    print("No valid rows generated — check data availability")
else:
    df = pd.DataFrame(rows)

    # Sort: newest date first, then highest Papa Avg within date
    df = df.sort_values(['As of Date', 'Papa Avg'], ascending=[False, False])

    # Format for readability
    df['As of Date'] = df['As of Date'].dt.strftime('%m/%d/%Y')
    for col in ['Papa Avg'] + [f'{p} day gain' for p in period_names]:
        # Only format columns that exist
        if col in df.columns:
            df[col] = df[col].round(2).astype(str) + '%'

    # Reorder columns (only include ones that actually exist)
    cols_order = ['Symbol', 'As of Date', 'Close', 'Papa Avg']
    for p in period_names:
        if p in df.columns:
            cols_order += [p, f'{p} Day Close', f'{p} day gain']

    df = df[cols_order]

    df.to_csv(output_file, index=False)
    print(f"\nSaved {len(df)} rows to {output_file}")

    # Show only the most recent snapshot for quick feedback
    latest_date = df['As of Date'].iloc[0]
    print(f"\nMost recent date ({latest_date}) — first 15 rows:")
    print(df[df['As of Date'] == latest_date].head(15))