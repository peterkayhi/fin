# backtesterDaily.py
# Daily momentum ETF rotation backtester with portfolio-level threshold rebalancing
# Adapted from monthly version: now runs ranking + rotation + rebalance check every trading day
# Keeps integer shares, same bucket logic, same 3 output CSVs

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

analysis_start = "2007-10-31"   # inclusive
analysis_end   = "2008-01-30"   # exclusive

# Fetch extra history so 252-day lookback is valid from first analysis day
fetch_start    = "2006-08-01"   # ~13 months buffer before analysis_start

INITIAL_VALUE = 100_000
VALUE_PER_BUCKET = INITIAL_VALUE // 3

# Momentum periods in **trading days**
PERIODS = [63, 126, 252]
PERIOD_NAMES = ["63", "126", "252"]

REBALANCE_THRESHOLD = 0.20

FILE_SUFFIX = "2007-2008"

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

if "Close" in data.columns.levels[0]:
    adj_close = data["Close"]
else:
    raise ValueError("Could not find 'Close' in downloaded data")

print(f"Downloaded shape: {adj_close.shape}")
print(f"Date range: {adj_close.index.min().date()} → {adj_close.index.max().date()}")

# All trading dates in analysis window
trading_dates = adj_close.index[
    (adj_close.index >= pd.to_datetime(analysis_start)) &
    (adj_close.index < pd.to_datetime(analysis_end))
]

print(f"\nFound {len(trading_dates)} trading days in analysis period")

# ────────────────────────────────────────────────
# 2. Build historyData — momentum for every trading day
# ────────────────────────────────────────────────

rows = []

min_lookback = max(PERIODS)  # 252

for as_of_date in trading_dates:

    # Skip days without enough history
    idx = adj_close.index.get_loc(as_of_date)
    if idx < min_lookback:
        continue

    print(f"  Processing {as_of_date.date()} ... ", end="")

    for ticker in TICKERS:
        if ticker not in adj_close.columns:
            continue

        current_close = adj_close.loc[as_of_date, ticker]
        if pd.isna(current_close):
            continue

        past_dates  = []
        past_closes = []
        gains       = []

        valid = True
        for days_back in PERIODS:
            past_idx   = idx - days_back
            past_date  = adj_close.index[past_idx]
            past_close = adj_close.iloc[past_idx][ticker]

            if pd.isna(past_close) or past_close == 0:
                valid = False
                break

            gain_pct = (current_close / past_close - 1) * 100
            past_dates.append(past_date)
            past_closes.append(past_close)
            gains.append(gain_pct)

        if not valid:
            continue

        papa_avg = np.mean(gains)

        row = {
            "Ticker": ticker,
            "As of Date": as_of_date,
            "Close": round(float(current_close), 2),
            "Papa Avg": round(papa_avg, 2),
        }

        for i, name in enumerate(PERIOD_NAMES):
            row[name]                = past_dates[i].strftime("%m/%d/%Y")
            row[f"{name} Day Close"] = round(float(past_closes[i]), 2)
            row[f"{name} day gain"]  = round(gains[i], 2)

        rows.append(row)

    print("done")

if not rows:
    raise ValueError("No valid momentum rows generated — check data / lookbacks")

history_df = pd.DataFrame(rows)
history_df = history_df.sort_values(["As of Date", "Papa Avg"], ascending=[True, False])
history_df["As of Date"] = history_df["As of Date"].dt.strftime("%m/%d/%Y")

dataFileName = "historyData" + FILE_SUFFIX + ".csv"
history_df.to_csv(OUTPUT_DIR / dataFileName, index=False)
print(f"\nSaved historyData.csv  ({len(history_df)} rows)")

# ────────────────────────────────────────────────
# 3. portfolioList — top 3 per day
# ────────────────────────────────────────────────

portfolio_rows = []

for as_of_str in history_df["As of Date"].unique():
    day_df = history_df[history_df["As of Date"] == as_of_str]
    if len(day_df) < 3:
        print(f"Warning: only {len(day_df)} tickers for {as_of_str}")
        continue

    top3 = day_df.head(3)

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

dataFileName = "portfolioList" + FILE_SUFFIX + ".csv"
portfolio_df.to_csv(OUTPUT_DIR / dataFileName, index=False)
print(f"Saved portfolioList.csv  ({len(portfolio_df)} days)")

# ────────────────────────────────────────────────
# 4. backtestResults — same bucket logic + rebalancing
# ────────────────────────────────────────────────

backtest_rows = []

prev_tickers = [None, None, None]
prev_qtys = [0, 0, 0]

for _, row in portfolio_df.iterrows():
    as_of_str = row["As of Date"]
    as_of_date = pd.to_datetime(as_of_str, format="%m/%d/%Y")
    
    curr_tickers = [row["Ticker 1"], row["Ticker 2"], row["Ticker 3"]]
    curr_closes = [row["Ticker 1 Close"], row["Ticker 2 Close"], row["Ticker 3 Close"]]
    
    qtys = [0, 0, 0]
    values = [0, 0, 0]
    
    for i in range(3):
        curr_ticker = curr_tickers[i]
        curr_close = curr_closes[i]
        
        if prev_tickers[i] is None:
            qtys[i] = VALUE_PER_BUCKET // curr_close
            print(f"Initial buy {as_of_str} B{i+1}: {curr_ticker} {qtys[i]} shares")
        elif curr_ticker == prev_tickers[i]:
            qtys[i] = prev_qtys[i]
        else:
            old_ticker = prev_tickers[i]
            old_close = adj_close.loc[as_of_date, old_ticker]
            current_value = prev_qtys[i] * old_close
            qtys[i] = int(current_value // curr_close)
            print(f"Rotate {as_of_str} B{i+1}: {old_ticker} → {curr_ticker} {qtys[i]} shares (proceeds ~{current_value:.0f})")
    
    # Current values after rotation/keep
    for i in range(3):
        values[i] = round(qtys[i] * curr_closes[i])
    
    total = sum(values)

    # Portfolio-level rebalancing check
    target = total / 3
    needs_rebalance = False
    for i in range(3):
        diff_pct = abs(values[i] - target) / target
        if diff_pct > REBALANCE_THRESHOLD:
            needs_rebalance = True
            break

    if needs_rebalance:
        print(f"REBALANCE triggered {as_of_str} | drift > {REBALANCE_THRESHOLD:.0%}")
        
        for i in range(3):
            qtys[i] = int(target // curr_closes[i])
        
        for i in range(3):
            values[i] = round(qtys[i] * curr_closes[i])
        
        total = sum(values)
        print(f"  After rebalance → values: {values}  total: {total:,.0f}")

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
    
    prev_tickers = curr_tickers
    prev_qtys = qtys

backtest_df = pd.DataFrame(backtest_rows)
dataFileName = "backtestResults" + FILE_SUFFIX + ".csv"
backtest_df.to_csv(OUTPUT_DIR / dataFileName, index=False)
print(f"Saved backtestResults.csv  ({len(backtest_df)} rows)")

print("\nFinished! Check folder:", OUTPUT_DIR.resolve())