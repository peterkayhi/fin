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

# Analysis period (last trading day of month must be inside this range)
start_date = '2025-01-01'
end_date   = '2026-03-02'   # exclusive upper bound

# Fetch extra history so we can calculate 252 trading days back
fetch_start = '2024-01-01'

# Lookback periods in trading days (easy to change)
periods = [63, 126, 252]
period_names = ['63', '126', '252']

# ────────────────────────────────────────────────
# DOWNLOAD DATA
# ────────────────────────────────────────────────

print("Downloading data from yfinance (auto_adjust=True)...")
data = yf.download(
    tickers,
    start=fetch_start,
    end=end_date,
    auto_adjust=True,           # ← makes 'Close' fully adjusted for dividends & splits
    progress=False
)

# ────────────────────────────────────────────────
# Extract adjusted closing prices safely
# ────────────────────────────────────────────────

# Show what we actually received (remove later if you want)
print("\nColumns structure:", data.columns)
print("Data shape:", data.shape)

if isinstance(data.columns, pd.MultiIndex):
    # Most common case with multiple tickers
    level_names = data.columns.levels[0]
    print("MultiIndex levels:", level_names)
    
    if 'Close' in level_names:
        adj_close = data['Close']
    elif 'Adj Close' in level_names:
        adj_close = data['Adj Close']  # fallback for older behavior
    else:
        raise ValueError("Cannot find 'Close' or 'Adj Close' in multi-index columns")
else:
    # Flat columns — usually happens with single ticker or certain settings
    if 'Close' in data.columns:
        adj_close = data['Close'].to_frame() if len(tickers) == 1 else data['Close']
    else:
        raise ValueError("No 'Close' column found in flat DataFrame")

# Make sure columns are just the ticker symbols (clean)
if isinstance(adj_close.columns, pd.MultiIndex):
    adj_close.columns = adj_close.columns.get_level_values(1)
elif len(tickers) == 1:
    adj_close.columns = [tickers[0]]

print("Adjusted close columns:", adj_close.columns.tolist())

# ────────────────────────────────────────────────
# Prepare trading calendar and monthly ends
# ────────────────────────────────────────────────

trading_dates = adj_close.index.sort_values()
trading_dates = trading_dates[trading_dates >= pd.to_datetime(start_date)]
trading_dates = trading_dates[trading_dates <  pd.to_datetime(end_date)]

# Last trading day of each calendar month in the range
monthly_ends = []
for dt in pd.date_range(start_date, end_date, freq='ME'):
    # Find the latest trading date ≤ month-end
    candidates = trading_dates[trading_dates <= dt]
    if not candidates.empty:
        monthly_ends.append(candidates[-1])

monthly_ends = sorted(set(monthly_ends), reverse=True)  # newest first

print(f"Found {len(monthly_ends)} monthly-end dates")

# ────────────────────────────────────────────────
# Calculate momentum rows
# ────────────────────────────────────────────────

rows = []

for as_of_date in monthly_ends:
    for ticker in tickers:
        if ticker not in adj_close.columns:
            continue
            
        try:
            current_close = adj_close.loc[as_of_date, ticker]
            if pd.isna(current_close):
                continue
                
            idx = adj_close.index.get_loc(as_of_date)
            
            past_dates   = []
            past_closes  = []
            gains        = []
            
            for days_back in periods:
                if idx < days_back:
                    raise ValueError("Not enough history")
                    
                past_idx = idx - days_back
                past_date = adj_close.index[past_idx]
                past_close = adj_close.iloc[past_idx][ticker]
                
                if pd.isna(past_close):
                    raise ValueError("Missing past price")
                
                gain_pct = (current_close - past_close) / past_close * 100
                
                past_dates.append(past_date)
                past_closes.append(past_close)
                gains.append(gain_pct)
            
            papa_avg = np.mean(gains)
            
            row = {
                'Symbol': ticker,
                'As of Date': as_of_date,
                'Close': round(float(current_close), 2),
                'Papa Avg': papa_avg,
            }
            
            for i, name in enumerate(period_names):
                row[name]                  = past_dates[i].strftime('%m/%d/%Y')
                row[f'{name} Day Close']   = round(float(past_closes[i]), 2)
                row[f'{name} day gain']    = gains[i]
            
            rows.append(row)
            
        except (KeyError, ValueError) as e:
            # Skip this ticker/date combo
            # print(f"Skipped {ticker} @ {as_of_date}: {e}")
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
    
    # Format dates and percentages to match your example
    df['As of Date'] = df['As of Date'].dt.strftime('%m/%d/%Y')
    for col in ['Papa Avg'] + [f'{p} day gain' for p in period_names]:
        df[col] = df[col].round(2).astype(str) + '%'
    
    # Reorder columns to match your CSV example
    cols_order = ['Symbol', 'As of Date', 'Close', 'Papa Avg']
    for p in period_names:
        cols_order += [p, f'{p} Day Close', f'{p} day gain']
    
    df = df[cols_order]
    
    # output_file = 'momentum_output.csv'
    output_file = '/Users/peterkay/Downloads/momentum.out.csv'
    df.to_csv(output_file, index=False)
    print(f"\nSaved {len(df)} rows to {output_file}")
    print("First few rows:\n", df.head())