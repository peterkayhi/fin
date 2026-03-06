# Import the libraries—yfinance for data fetching, pandas for data handling
import yfinance as yf  # 'yf' is just a shortcut alias
import pandas as pd    # 'pd' is common shorthand
from datetime import datetime, timedelta  # Built-in for handling dates

# Step 1: Define your basket of 13 tickers (replace with yours)
tickers = tickers = "VTV VUG VIOV VIOG VEA VWO VNQ PDBC IAU EDV VGIT VCLT BNDX".split()

# Step 2: Fetch historical closing prices
# We'll download data for the past 2 years to ensure we have enough history
# IMPORTANT: Use auto_adjust=True (now the default in recent yfinance versions)
# This makes 'Close' already adjusted for splits/dividends — no separate 'Adj Close' needed
end_date = datetime.now()  # Today's date
start_date = end_date - timedelta(days=365*2 + 30)  # ~2 years + buffer for 252+ days
data = yf.download(tickers, start=start_date, end=end_date, auto_adjust=True)['Close']

# 'data' is now a pandas DataFrame: rows are dates, columns are tickers, values are ADJUSTED closing prices
# If you ever need the raw/unadjusted prices + separate Adj Close, switch to auto_adjust=False

# Step 3: Simulate rebalancing at a specific date (e.g., last trading day)
rebalance_date = data.index[-1]  # Last row's date (pandas uses dates as index)

# Now, for each ticker, calculate the gains
momentum_scores = {}  # A dictionary to store ticker: score

for ticker in tickers:
    prices = data[ticker].dropna()  # Get adjusted prices for this ticker, drop any missing values
    
    if len(prices) < 252:  # Skip if not enough data
        continue
    
    # Current (adjusted) price
    current_price = prices.loc[rebalance_date]
    
    # Prices from 63, 126, 252 trading days ago — .shift() moves back by rows (trading days)
    price_63 = prices.shift(63).loc[rebalance_date]   # ~3 months back
    price_126 = prices.shift(126).loc[rebalance_date] # ~6 months
    price_252 = prices.shift(252).loc[rebalance_date] # ~12 months
    
    # Calculate percentage gains (handle if any are NaN/missing)
    if pd.notna(price_63) and pd.notna(price_126) and pd.notna(price_252):
        gain_63 = (current_price / price_63 - 1) * 100
        gain_126 = (current_price / price_126 - 1) * 100
        gain_252 = (current_price / price_252 - 1) * 100
        
        # Average the three momentum periods
        avg_gain = (gain_63 + gain_126 + gain_252) / 3
        momentum_scores[ticker] = avg_gain

# Step 4: Rank by momentum score (descending) and select top 3
if momentum_scores:
    sorted_tickers = sorted(momentum_scores, key=momentum_scores.get, reverse=True)
    top_3 = sorted_tickers[:3]
    
    print(f"At {rebalance_date.date()}, top 3 tickers by momentum: {top_3}")
    print("Momentum scores (avg of 3/6/12 month gains):")
    for t in top_3:
        print(f"{t}: {momentum_scores[t]:.2f}%")
    
    # Step 5: Rebalance simulation—equal weight to top 3
    allocation = {t: round(1/3, 4) for t in top_3}  # nicer display
    print("Suggested equal allocation:", allocation)
else:
    print("Not enough data for calculations.")