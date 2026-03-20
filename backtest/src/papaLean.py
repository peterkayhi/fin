""" 

Papa Lean Backtest 
This script implements the Papa Bear momentum strategy as described on muscularportfolios.com. It downloads historical price data for the specified ETFs, calculates the momentum, constructs the portfolio based on the top 3 momentum scores each month, and then evaluates the performance against SPY.

Leaner version of momentum.py with more comments and explanations for educational purposes. This is meant to be a clear and straightforward implementation of the strategy, without any optimizations or shortcuts, so that you can see exactly how each step works.

Based on papaGraph.py but focused on exporting csvs and giving a summary like momentum.py

next steps:
- figure out why month end dates don't match w/ momentum me dates
- implement balancing


"""

import pandas as pd
import yfinance as yf
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# =============================================================================
# CONFIGURATION - change these as needed
# =============================================================================
# Official Papa Bear 13 ETFs (straight from muscularportfolios.com)
tickers = [
    'VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO', 'VNQ',
    'PDBC', 'IAU', 'EDV', 'VGIT', 'VCLT', 'BNDX'
]

start_date = "2016-01-01"      # <-- your desired backtest start
end_date   = datetime.today().strftime("%Y-%m-%d")

benchmark = "SPY"

# =============================================================================
# DOWNLOAD WITH MOMENTUM LOOKBACK BUFFER
# =============================================================================
# Fix for the bug you spotted: pull extra history so 12-month momentum works from day 1
print("Downloading adjusted price data with 15-month lookback buffer...")
download_start = (pd.to_datetime(start_date) - pd.DateOffset(months=15)).strftime("%Y-%m-%d")

all_tickers = tickers + [benchmark]
data = yf.download(
    all_tickers,
    start=download_start,
    end=end_date,
    auto_adjust=True,      # fully adjusted OHLC (Close column is perfect)
    progress=False
)["Close"]

# data.to_csv("/Users/peterkay/Downloads/backtestFiles/papa_bear_raw_prices.csv") # save raw data for debugging

# =============================================================================
# Resample to month-end last trading day
# .resample("ME") groups by month-end, .last() takes the last price of that month.
# .dropna(how="all") removes rows where all tickers are NaN (e.g., if month-end was a holiday).
# .resample is an intermediate step to ensure we have clean month-end data for momentum calculations and rebalancing - you have to add an aggregation method like .last() to get a single price per month, and dropna to handle any missing data at month-ends. how="all" means it will only drop rows where all columns are NaN, which is what we want in case some tickers have missing data but others don't.
monthly_prices = data.resample("ME").last().dropna(how="all")

# monthly_prices.to_csv("/Users/peterkay/Downloads/backtestFiles/papa_bear_monthly_prices.csv") # save monthly data for debugging


# =============================================================================
# PAPA BEAR MOMENTUM: average of 3/6/12-month returns
# .pct_change(periods=3) gives 3-month return, etc. Returns are in decimal (e.g., 0.05 for 5%).
# .pct_change() looks back by N rows as defined by periods, so it calculates the return from that point to the current row.
# =============================================================================
mom_3  = monthly_prices.pct_change(periods=3)
mom_6  = monthly_prices.pct_change(periods=6)
mom_12 = monthly_prices.pct_change(periods=12)

avg_momentum = (mom_3 + mom_6 + mom_12) / 3

# =============================================================================
# PORTFOLIO: top 3 equal-weighted each month (rebalance on month-end)
# We create a weights DataFrame initialized to 0.0, then loop through each month starting from the second row (since the first month has no prior momentum). For each month, we look at the previous month's momentum, find the top 3 tickers, and assign them a weight of 1/3 in the current month. This way, we are always using the most recent momentum data to determine our holdings for the next month.
# the .iloc[i-1] gets the previous month's momentum, .nlargest(3) finds the top 3 tickers, and we set those tickers' weights to 1/3 for the current month in the weights DataFrame.
# =============================================================================
weights = pd.DataFrame(0.0, index=avg_momentum.index, columns=avg_momentum.columns)

for i in range(1, len(avg_momentum)):
    # For each month starting from the second row (i=1), we look back at the previous month's momentum to determine the top 3 tickers. We then assign those tickers a weight of 1/3 for the current month and create a table of weights that we will later apply to the returns. 
    #
    # the len() function in panda returns the number of rows in avg_momentum, so the loop iterates through each month starting from the second one (since the first month has no prior momentum data). Inside the loop, we use .iloc[i-1] to access the previous month's momentum data, find the top 3 tickers with .nlargest(3), and then set their weights to 1/3 for the current month in the weights DataFrame.
    #
    # how to list tail 3 of those with weight > 0
    # weights.tail(3).loc[:, lambda df: df.sum() > 0]
    # weights.tail(3).loc[:, lambda ddf: ddf.sum() > 0] # ddf names the frame inside the lambda function, you can name it anything you want, it's just a placeholder for the DataFrame being processed. The key part is that it allows you to filter columns based on their sum, which is what we want to do to find the tickers that have a weight greater than 0 in the last 3 rows.

    prev_mom = avg_momentum.iloc[i-1] # Get the previous month's momentum
    top3 = prev_mom.nlargest(3).index # Find the top 3 tickers and use .index to get their names. e.g. returns something like Index(['BNDX', 'EDV', 'IAU'], dtype='str', name='Ticker')
    weights.loc[avg_momentum.index[i], top3] = 1/3 # Assign equal weight to the top 3 for the current month. the [] after weights.loc[...] selects the columns corresponding to the top 3 tickers and sets their weight to 1/3 for that month.

# Monthly asset returns
# list tail of those with return > 0
# monthly_returns.tail(3).loc[:, lambda df: df.sum() > 0]

monthly_returns = monthly_prices.pct_change()

# Strategy returns (weights from previous month applied to current month)
# The weights are shifted by 1 month to apply the previous month's weights to the current month's returns, which simulates the rebalancing process. The .sum(axis=1) then sums across all tickers to get the total portfolio return for each month.
# .shift function shifts the weights down by one row, so the weights for month t are applied to the returns of month t. This simulates the real-world scenario where you determine your portfolio allocation at the end of month t-1 and then experience the returns in month t based on that allocation.
# .sum(axis=1) sums the weighted returns across all tickers for each month, giving you the total portfolio return for that month.
portfolio_returns = (weights.shift(1) * monthly_returns).sum(axis=1)
bench_returns = monthly_returns[benchmark] # Extract the benchmark returns (SPY) for the same period to compare against our strategy. This will allow us to calculate performance metrics and plot the equity curve for both the portfolio and the benchmark.

# =============================================================================
# SLICE TO USER'S ORIGINAL start_date (bug fix in action)
# =============================================================================
original_start = pd.to_datetime(start_date)
portfolio_returns = portfolio_returns[portfolio_returns.index >= original_start]
bench_returns      = bench_returns[bench_returns.index >= original_start]

# =============================================================================
# PERFORMANCE METRICS
# =============================================================================
def cagr(returns):
    if len(returns) == 0:
        return np.nan
    cum = (1 + returns).cumprod()
    n_months = len(returns)
    return cum.iloc[-1] ** (12 / n_months) - 1

def max_drawdown(returns):
    cum = (1 + returns).cumprod()
    peak = cum.cummax()
    dd = (cum - peak) / peak
    return dd.min()

def sharpe(returns, rf=0.0):
    excess = returns - rf / 12
    return excess.mean() / excess.std() * np.sqrt(12) if excess.std() != 0 else np.nan

print("\n=== PAPA BEAR BACKTEST RESULTS ===")
print(f"Period:           {portfolio_returns.index[0]:%b %Y} – {portfolio_returns.index[-1]:%b %Y}")
print(f"Portfolio CAGR:   {cagr(portfolio_returns):.1%}")
print(f"SPY CAGR:         {cagr(bench_returns):.1%}")
print(f"Portfolio Max DD: {max_drawdown(portfolio_returns):.1%}")
print(f"SPY Max DD:       {max_drawdown(bench_returns):.1%}")
print(f"Portfolio Sharpe: {sharpe(portfolio_returns):.2f}")
print(f"SPY Sharpe:       {sharpe(bench_returns):.2f}")

# =============================================================================
# EQUITY CURVE PLOT
# =============================================================================
# .cumprod() calculates the cumulative product of (1 + returns) to get the equity curve. This shows how $1 invested in the strategy and the benchmark would have grown over time, allowing us to visually compare their performance.
port_cum = (1 + portfolio_returns).cumprod()
bench_cum = (1 + bench_returns).cumprod()


# Save results
results = pd.DataFrame({
    "Portfolio": port_cum,
    "SPY": bench_cum,
    "Portfolio Monthly Return": portfolio_returns
})
results.to_csv("/Users/peterkay/Downloads/backtestFiles/papa_bear_backtest_fixed.csv")
print("\nResults saved to papa_bear_backtest_fixed.csv")