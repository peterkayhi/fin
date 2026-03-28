"""
Papa Bear Daily Momentum Snapshot

forked from momentumDaily

pulls last 7 days with momentum calcs.

"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from yfcache import yfcache


# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────

tickers = [
    'VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO', 'VNQ',
    'PDBC', 'IAU', 'EDV', 'VGIT', 'VCLT', 'BNDX'
]

# Analysis period — get the last 7 days
start_date = (date.today() - timedelta(days=7)).isoformat()    # 7 days ago
end_date   = (date.today() - timedelta(days=0)).isoformat()   # today

# Fetch extra history so 252-day lookback works from day 1
fetch_start    = (date.today() - relativedelta(months=14)).isoformat()      # ~14 months before analysis_start — safe buffer

# Lookback periods in trading days
periods = [63, 126, 252]
period_names = ['63', '126', '252']

# ← change filename as needed
output_file = f'/Users/peterkay/Downloads/csv/papabear{end_date.replace("-", "")}.csv'

# ────────────────────────────────────────────────
# DOWNLOAD DATA
# ────────────────────────────────────────────────

print("Getting ticker adjusted close (auto_adjust=True)...")

cache = yfcache() # initialize caching
adj_close = cache.get (
    ticker_list=tickers,
    start_date=fetch_start,
    end_date=end_date,
    skip_cache=False
).final_df

# ────────────────────────────────────────────────
# Prepare trading calendar
# ────────────────────────────────────────────────

trading_dates = adj_close.index.sort_values()
trading_dates = trading_dates[trading_dates >= pd.to_datetime(start_date)]
trading_dates = trading_dates[trading_dates < pd.to_datetime(end_date)]

# print(f"Found {len(trading_dates)} trading days in range")

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

            papa_avg = np.mean(gains) # average of whatever periods we calculated

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

#    df.to_csv(output_file, index=False)
#    print(f"\nSaved {len(df)} rows to {output_file}")

    # Show only the most recent snapshot for quick feedback
    latest_date = df['As of Date'].iloc[0]
#    print(f"\nMost recent date ({latest_date}) — first 15 rows:")
#    print(df[df['As of Date'] == latest_date].head(6))

    print(
    df[df['As of Date'] == latest_date]
      [['Symbol', 'As of Date', 'Close', 'Papa Avg']]
      .head(len(tickers)) # show all tickers only for the most recent date
    )


    df[df['As of Date'] == latest_date][['Symbol', 'As of Date', 'Close', 'Papa Avg']].head(len(tickers)).to_csv(output_file, index=False) # save this snippet to csv 

print(f"\nSaved {len(tickers)} rows to {output_file}")
