# momentum_backtester.py
# Monthly momentum ETF rotation backtester
# Updated to fix backtest logic: per-rank bucket rotation, keep qty if same ticker in same rank, else reinvest current value of old into new

import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path

# ────────────────────────────────────────────────
#           CONFIGURATION
# ────────────────────────────────────────────────

TICKERS = [
    "VTV", "VUG", "VIOV", "VIOG", "VEA", "VWO", "VNQ",
    "PDBC", "IAU", "EDV", "VGIT", "VCLT", "BNDX"
]

# Analysis period — we want last trading days of month inside this window
analysis_start = "2025-01-01"
analysis_end   = "2026-03-10"   # exclusive upper bound

# Fetch extra history so 252-day lookback works from day 1
fetch_start    = "2023-12-01"   # ~14 months before analysis_start — safe buffer

INITIAL_VALUE = 100_000
VALUE_PER_BUCKET = INITIAL_VALUE // 3

# Momentum periods in **trading days**
PERIODS = [63, 126, 252]
PERIOD_NAMES = ["63", "126", "252"]

OUTPUT_DIR = Path("/Users/peterkay/Downloads/backtestFiles")
OUTPUT_DIR.mkdir(exist_ok=True)

# ────────────────────────────────────────────────
# 1. Download adjusted closes
# ────────────────────────────────────────────────

print("Downloading data (auto_adjust=True for dividend/split adjusted closes)...")
data = yf.download(
    tickers=TICKERS,
    start=fetch_start,
    end=analysis_end,
    auto_adjust=True,
    progress=True
)

# Extract adjusted Close — yfinance usually returns multi-index
if "Close" in data.columns.levels[0]:
    adj_close = data["Close"]
else:
    raise ValueError("Could not find 'Close' in downloaded data")

print(f"Downloaded shape: {adj_close.shape}")
print(f"Date range: {adj_close.index.min().date()} → {adj_close.index.max().date()}")
print("Tickers:", adj_close.columns.tolist())

# ────────────────────────────────────────────────
# 2. Find last trading day of each month in analysis period
# ────────────────────────────────────────────────

trading_dates = adj_close.index
monthly_ends = []

for dt in pd.date_range(analysis_start, analysis_end, freq="ME"):
    # Latest trading date ≤ calendar month-end
    candidates = trading_dates[trading_dates <= dt]
    if not candidates.empty:
        monthly_ends.append(candidates[-1])

monthly_ends = sorted(set(monthly_ends))  # ascending order

print(f"\nFound {len(monthly_ends)} analysis month-end trading dates:")
print([d.strftime("%Y-%m-%d") for d in monthly_ends])

# ────────────────────────────────────────────────
# 3. Build historyData rows
# ────────────────────────────────────────────────
# This is the core momentum ranking preparation step.
# For every valid month-end trading date in the analysis period:
#   - Loop through each ticker
#   - Check if we have enough clean history for ALL momentum periods (63, 126, 252 trading days)
#   - Calculate percentage gain for each lookback period
#   - Compute "Papa Avg" = simple arithmetic mean of the three momentum values
#   - Store a detailed row with current price, all past dates/closes/gains, and Papa Avg
#
# Important design choices / assumptions:
#   - We require **all three** momentum periods to have valid, non-zero, non-NaN data
#     → if any lookback is missing or invalid → skip the ticker for that month entirely
#   - Gains are calculated using adjusted closes (dividend & split adjusted)
#   - Momentum is expressed as percentage gain (not log return or total return index)
#   - Ranking will be done by Papa Avg descending (highest average momentum = best)
#   - We round displayed values to 2 decimals for readability in CSV
#   - Rows are collected in a list then turned into a DataFrame once at the end
#     (more memory-efficient than appending to df repeatedly)

rows = []

for as_of_date in monthly_ends:
    print(f"  Processing {as_of_date.date()} ... ", end="")

    # Get the integer position of this date in the adj_close index
    # (faster than .loc for repeated integer-based slicing later)
    idx = adj_close.index.get_loc(as_of_date)

    for ticker in TICKERS:
        if ticker not in adj_close.columns:
            continue

        # Current price on the rebalance date
        current_close = adj_close.loc[as_of_date, ticker]
        if pd.isna(current_close):
            continue

        past_dates  = []
        past_closes = []
        gains       = []

        valid = True
        for days_back in PERIODS:
            # Check if we even have enough rows of history before this date
            if idx < days_back:
                valid = False
                break

            # Go back exactly 'days_back' trading days (not calendar days)
            past_idx   = idx - days_back
            past_date  = adj_close.index[past_idx]
            past_close = adj_close.iloc[past_idx][ticker]

            # Make sure we have valid data — skip ticker if any lookback is bad
            if pd.isna(past_close) or past_close == 0:
                valid = False
                break

            # Standard momentum: percentage change
            gain_pct = (current_close / past_close - 1) * 100
            past_dates.append(past_date)
            past_closes.append(past_close)
            gains.append(gain_pct)

        if not valid:
            # Skip this ticker for this month — not enough reliable history
            continue

        # Our composite momentum score: plain average of the three periods
        papa_avg = np.mean(gains)

        # Build the output row with all details for auditing / debugging
        row = {
            "Ticker": ticker,
            "As of Date": as_of_date,
            "Close": round(float(current_close), 2),
            "Papa Avg": round(papa_avg, 2),
        }

        # Add the detailed per-period columns so we can verify calculations
        for i, name in enumerate(PERIOD_NAMES):
            row[name]                = past_dates[i].strftime("%m/%d/%Y")
            row[f"{name} Day Close"] = round(float(past_closes[i]), 2)
            row[f"{name} day gain"]  = round(gains[i], 2)

        rows.append(row)

    print("done")

# Safety check — if nothing survived the filters, something's wrong with data or lookbacks
if not rows:
    raise ValueError("No valid momentum rows generated — check data / lookbacks")

# Convert list of dicts → DataFrame once (much faster than incremental appends)
history_df = pd.DataFrame(rows)

# Critical sort: oldest to newest date, and within each date highest momentum first
# This ordering is what lets us do .head(3) later to get top-ranked tickers
history_df = history_df.sort_values(["As of Date", "Papa Avg"], ascending=[True, False])

# Make dates human-readable in the CSV (reviewers love this)
history_df["As of Date"] = history_df["As of Date"].dt.strftime("%m/%d/%Y")

history_df.to_csv(OUTPUT_DIR / "historyData.csv", index=False)
print(f"\nSaved historyData.csv  ({len(history_df)} rows)")

# ────────────────────────────────────────────────
# 4. portfolioList — top 3 per month
# ────────────────────────────────────────────────

portfolio_rows = []

for as_of_str in history_df["As of Date"].unique():
    month_df = history_df[history_df["As of Date"] == as_of_str]
    if len(month_df) < 3:
        print(f"Warning: only {len(month_df)} tickers for {as_of_str}")
        continue

    top3 = month_df.head(3)

    portfolio_rows.append({
        "As of Date": as_of_str,
        "Ticker 1": top3.iloc[0]["Ticker"],
        "Ticker 1 Close": top3.iloc[0]["Close"],
        "Ticker 2": top3.iloc[1]["Ticker"],
        "Ticker 2 Close": top3.iloc[1]["Close"],
        "Ticker 3": top3.iloc[2]["Ticker"],
        "Ticker 3 Close": top3.iloc[2]["Close"],
    })

portfolio_df = pd.DataFrame(portfolio_rows)
portfolio_df.to_csv(OUTPUT_DIR / "portfolioList.csv", index=False)
print(f"Saved portfolioList.csv  ({len(portfolio_df)} months)")

# ────────────────────────────────────────────────
# 5. backtestResults — independent bucket management
# ────────────────────────────────────────────────

backtest_rows = []

# Track per bucket (rank)
prev_tickers = [None, None, None]
prev_qtys = [0, 0, 0]

for _, row in portfolio_df.iterrows():
    as_of_str = row["As of Date"]
    as_of_date = pd.to_datetime(as_of_str)
    
    curr_tickers = [row["Ticker 1"], row["Ticker 2"], row["Ticker 3"]]
    curr_closes = [row["Ticker 1 Close"], row["Ticker 2 Close"], row["Ticker 3 Close"]]
    
    qtys = [0, 0, 0]
    values = [0, 0, 0]
    
    for i in range(3):
        curr_ticker = curr_tickers[i]
        curr_close = curr_closes[i]
        
        if prev_tickers[i] is None:
            # First row: initial allocation
            qtys[i] = VALUE_PER_BUCKET // curr_close
            print(f"Initial buy {as_of_str} B{i+1}: {curr_ticker} {qtys[i]} shares")
        elif curr_ticker == prev_tickers[i]:
            # Same ticker in this rank: keep qty
            qtys[i] = prev_qtys[i]
        else:
            # Rotation: sell old at current close, buy new with proceeds (floor qty)
            old_ticker = prev_tickers[i]
            old_close = adj_close.loc[as_of_date, old_ticker]
            current_value = prev_qtys[i] * old_close
            qtys[i] = int(current_value // curr_close)
            print(f"Rotate {as_of_str} B{i+1}: {old_ticker} → {curr_ticker} {qtys[i]} shares (proceeds ~{current_value:.0f})")
    
    # Compute values (round to integer dollars, as in sample)
    for i in range(3):
        values[i] = round(qtys[i] * curr_closes[i])
    
    total = sum(values)
    
    # Append row with formatting
    backtest_rows.append({
        "As of Date": as_of_str,
        "B1 Ticker": curr_tickers[0],
        "B1 Qty": qtys[0],
        "B1 Value": f"${values[0]:,}",
        "B2 Ticker": curr_tickers[1],
        "B2 Qty": qtys[1],
        "B2 Value": f"${values[1]:,}",
        "B3 Ticker": curr_tickers[2],
        "B3 Qty": qtys[2],
        "B3 Value": f"${values[2]:,}",
        "Total Value": f"${total:,}"
    })
    
    # Update prev for next iteration
    prev_tickers = curr_tickers
    prev_qtys = qtys

backtest_df = pd.DataFrame(backtest_rows)
backtest_df.to_csv(OUTPUT_DIR / "backtestResults.csv", index=False)
print(f"Saved backtestResults.csv  ({len(backtest_df)} rows)")

print("\nFinished! Check folder:", OUTPUT_DIR.resolve())