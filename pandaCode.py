"""
PandCode.py
Collection of examples for using Pandas in df manipulation and analysis.
"""

# Import the necessary libraries.
# Pandas is for data manipulation and analysis, like working with spreadsheets in code.
# We use 'pd' as a shortcut alias—common convention in Python to save typing.
import pandas as pd

# NumPy is for numerical computations; Pandas relies on it, but we might use it directly for math ops.
# We use 'np' as the alias.
import numpy as np

# Define the file path as a variable so it's easy to change for future CSVs.
# Replace 'historical_stock_prices.csv' with your actual file name.
# This assumes the CSV is in the same directory as this script; otherwise, use a full path like '/Users/peterkay/Downloads/historical_stock_prices.csv'.
file_path = '/Users/peterkay/Downloads/csv/aapl2016.csv'

# Load the CSV into a Pandas DataFrame.
# A DataFrame is like a table: rows and columns with labels.
# pd.read_csv() reads the file, assuming the first row is headers (column names).
# It handles large files efficiently by loading into memory as a structured array.
# If your CSV uses a different delimiter (e.g., semicolons), add sep=';' as an argument.
# For dates, we'll parse them later if needed.
df = pd.read_csv(file_path)

# Print the first few rows to inspect the data.
# df.head(5) shows the top 5 rows (default is 5 if no number is given).
# Useful for quickly checking if the import worked and what the data looks like.
# In stock data, this might show recent dates with prices.
print("First 5 rows of the data:")
print(df.head(5))

# Print the last few rows.
# df.tail(5) shows the bottom 5 rows.
# Great for seeing the oldest data in historical stock prices.
print("\nLast 5 rows of the data:")
print(df.tail(5))

# Get a summary of the DataFrame.
# df.info() shows column names, data types (e.g., float for prices, object for dates), non-null counts, and memory usage.
# Helps spot issues like missing values or wrong types (e.g., dates as strings).
print("\nDataFrame info:")
print(df.info())

# Descriptive statistics.
# df.describe() computes mean, std dev, min, max, quartiles for numerical columns.
# For stocks, this gives quick insights like average close price or volatility (std dev).
print("\nDescriptive statistics:")
print(df.describe())

# Assuming 'Date' is a column, convert it to datetime type.
# Dates often come as strings in CSVs; pd.to_datetime() parses them into proper dates.
# This enables time-based operations like resampling.
# errors='coerce' turns invalid dates into NaT (Not a Time).
# If your column isn't named 'Date', replace it here.
df['Date'] = pd.to_datetime(df['date'], errors='coerce')

# Set 'Date' as the index.
# df.set_index() makes 'Date' the row labels instead of 0,1,2...
# Useful for time series data like stocks, so you can slice by date easily.
# inplace=True modifies the DataFrame directly without creating a copy.
df.set_index('Date', inplace=True)

# Sort by index (dates) if not already sorted.
# df.sort_index() ensures chronological order, ascending=True by default.
# Important for historical data that might be imported out of order.
df.sort_index(inplace=True)

# Print the DataFrame after these changes.
print("\nDataFrame after date conversion and indexing:")
print(df.head())

# Select specific columns.
# df[['open', 'close']] creates a new DataFrame with just those columns.
# Like selecting columns in a SQL query or spreadsheet.
# Useful for focusing on price changes without volume or highs/lows.
prices = df[['open', 'close']]
print("\nSelected 'open' and 'close' columns:")
print(prices.head())

# Filter rows based on a condition.
# df[df['close'] > 100] returns rows where close price > 100.
# This uses boolean indexing: creates a mask of True/False, then subsets.
# Great for finding days when a stock crossed a threshold.
high_close_days = df[df['close'] > 100]  # Adjust 100 to a relevant value for your data.
print("\nDays where close > 100:")
print(high_close_days.head())

# Calculate a new column: daily return.
# df['Return'] = (close - open) / open, as percentage.
# Vectorized operation: applies to whole column at once, fast even for large data.
# NumPy handles the math under the hood.
df['Daily Return'] = (df['close'] - df['open']) / df['open'] * 100
print("\nDataFrame with new 'Daily Return' column:")
print(df[['open', 'close', 'Daily Return']].head())

# Handle missing values.
# df.fillna(0) replaces NaNs with 0; here for 'volume' as example.
# For stocks, missing prices might mean holidays—could forward-fill instead: df.fillna(method='ffill').
# df.dropna() would remove rows with any NaNs.
df['volume'] = df['volume'].fillna(0)  # Assuming 'volume' might have misses.
print("\nAfter filling missing volumes with 0:")
print(df['volume'].head())

# Group by and aggregate.
# Assuming monthly data, but for daily: resample to monthly.
# df.resample('M') groups by month-end, .mean() averages.
# Requires datetime index. Great for monthly average close prices.
monthly_avg = df.resample('ME').mean(numeric_only=True)
print("\nMonthly average prices:")
print(monthly_avg['close'].head())

# Rolling window calculation: 5-day moving average.
# df['close'].rolling(5).mean() computes average over last 5 rows.
# Shifts window down the data. Useful for smoothing stock trends.
df['5-Day MA'] = df['close'].rolling(window=5).mean(numeric_only=True)
print("\nDataFrame with 5-day moving average:")
print(df[['close', '5-Day MA']].head(10))  # Show 10 to see the window fill.

# Pivot table example (if you had multiple stocks, but simulating here).
# For single stock, not super useful, but imagine adding a 'Symbol' column.
# pd.pivot_table() like Excel pivots: aggregates data.
# Here, average close by year (extract year from index).
df['Year'] = df.index.year
pivot = pd.pivot_table(df, values='close', index='Year', aggfunc='mean')
print("\nAverage close by Year (pivot table):")
print(pivot)

# Export back to CSV.
# df.to_csv() saves the modified DataFrame.
# index=True includes the Date index as a column.
# Useful for saving processed data for later analysis.
output_path = 'processed_stock_prices.csv'
df.to_csv(output_path, index=True)
print(f"\nExported processed data to {output_path}")

# For larger files: read in chunks.
# If future CSVs are huge (e.g., years of tick data), use chunksize.
# This reads 1000 rows at a time, processes, then concatenates.
# Prevents memory overload on your Mac Mini.
chunks = pd.read_csv(file_path, chunksize=1000)
df_large = pd.concat(chunks)  # Concatenate all chunks into one DataFrame.
print("\nExample of loading in chunks for large files (size of concatenated DF):")
print(df_large.shape)  # Shows (rows, columns)

# ======= new stuff

# ───────────────────────────────────────────────
#  Download real adjusted price data (most common pattern)
# ───────────────────────────────────────────────
tickers = ["SPY", "QQQ", "TLT"]
df = yf.download(tickers, start="2023-01-01", end="2024-01-01", auto_adjust=True, progress=False)

# Keep only adjusted Close prices
df = df["Close"].copy()           # now columns = tickers

print("Shape:", df.shape)
print(df.tail(3))

# ───────────────────────────────────────────────
# Most frequently used pandas operations
# ───────────────────────────────────────────────

# 1. Select columns
prices = df[["SPY", "QQQ"]]

# 2. Filter rows (date range)
recent = prices.loc["2025-01-01":]

# 3. Basic calculations
returns = prices.pct_change()                    # daily returns
log_returns = prices.pct_change().add(1).log()   # ≈ log(1+r)

# 4. Rolling / moving calculations (very common in finance)
sma_20 = prices.rolling(20).mean()
ewm_26 = prices.ewm(span=26, adjust=False).mean()

# 5. Cumulative calculations
cum_ret = (1 + returns).cumprod() - 1            # total return curve
drawdown = cum_ret - cum_ret.cummax()            # running drawdown

# 6. Summary statistics
stats = prices.describe()                        # count, mean, std, min, 25%, 50%, 75%, max
annual_vol = returns.std() * (252 ** 0.5)        # annualized volatility

# 7. Missing data handling (very frequent)
prices = prices.fillna(method="ffill")           # forward fill (common for prices)
# or: prices = prices.dropna()                   # drop rows with any NaN

# 8. Resampling / aggregation (daily → monthly, etc.)
monthly = prices.resample("ME").last()           # month-end prices
monthly_ret = monthly.pct_change()

# 9. Ranking / correlation (portfolio analysis)
corr_matrix = returns.corr()
rank_today = returns.iloc[-1].rank(ascending=False)

# 10. Apply custom function (vectorized when possible)
def sharpe(rets): return rets.mean() / rets.std() * (252 ** 0.5)
sharpe_ratios = returns.apply(sharpe).round(3)

# ───────────────────────────────────────────────
# Quick view of results
# ───────────────────────────────────────────────
print("\nLast 5 daily returns:")
print(returns.tail())

print("\nSharpe ratios (annualized):")
print(sharpe_ratios)

print("\nCorrelation matrix:")
print(corr_matrix.round(3))

# -- more quick references --
### Quick reference – most-used functions in practice:

# Task                        # One-liner example                              # Frequency #
#----------------------------------------------------------------------------
# Download + Adj Close         `yf.download(…, auto_adjust=True)["Close"]`  
# Select columns               `df[["SPY","QQQ"]]`                            
# Date slicing                 `df.loc["2024-01":]`                           
# Daily returns                `df.pct_change()`                              
# Rolling average              `df.rolling(20).mean()`                        
# Cumulative product           `(1 + ret).cumprod()`                         
# Fill / drop NaNs             `ffill()` / `dropna()`                        
# Resample (OHLC)              `df.resample("W").last()`                     
# Correlation                  `df.corr()`                                    
# Apply custom func            `df.apply(your_func)`                          
# Groupby (advanced)           `df.groupby(pd.Grouper(freq="Q")).mean()`      

# ------- return rows based on condition --------
# Single row as Series (most common)
row = df.iloc[n]          # n = 5 → 6th row

# As DataFrame (keeps column names as header)
row_df = df.iloc[[n]]     # note the double brackets
# double brackets are crucial: df.iloc[n] returns a Series (1D), while df.iloc[[n]] returns a DataFrame (2D) with one row. The latter keeps the column names as headers, which is often more convenient for further processing or exporting.
#
# the reason why double brackets return a DataFrame is that the inner brackets [n] select the row index, while the outer brackets [[n]] indicate that we want to keep the result as a DataFrame. This is a common pattern in Pandas when you want to maintain the tabular structure even for a single row or column.

# By date index (if datetimeindex)
row = df.loc["2025-06-30"]               # exact date
row = df.loc["2025-06-01":"2025-06-30"]  # range

# To get the 3 largest closing prices (and their tickers) from a pandas DataFrame with many tickers as columns and dates as rows (using adjusted close via yfinance):
import yfinance as yf
import pandas as pd

# Assuming df is your DataFrame: index=dates, columns=tickers, values=adjusted Close
# Example: df = yf.download(tickers, period="1mo", auto_adjust=True)['Close']

# For the most recent row (latest month-end or last available date)
latest_row = df.iloc[-1]                    # Series: ticker -> adjusted close

top3 = latest_row.nlargest(3)               # returns Series with top 3

print(top3)
# Output example:
# AAPL    245.67
# MSFT    312.45
# NVDA    198.23
# Name: Close, dtype: float64

# how to drop or delete a row 
import pandas as pd

# Drop row by index label (e.g., specific date)
df = df.drop('2025-01-15')          # if index is Date

# Drop multiple rows
df = df.drop(['2025-01-15', '2025-01-16'])

# Drop row by integer position (safer when index has duplicates)
df = df.drop(df.index[0])           # drop first row
df = df.drop(df.index[[0, 5, 10]])  # drop several by position

# Drop in-place (modifies df directly, no reassignment needed)
df.drop('2025-01-15', inplace=True)

# ------- return columns based on condition --------
# Single column as Series (99% of finance use-cases)
col = df["SPY"]           # ticker as column name
col = df.Y                # only if column name is valid identifier (no spaces, etc.)

# As DataFrame (keeps it 2D)
col_df = df[["SPY"]]      # double brackets – very important

# Multiple columns
subset = df[["SPY", "QQQ", "TLT"]]

#--------Most frequent pattern you'll write in finance scripts:
latest_prices = df.iloc[-1]           # Series with tickers as index
spy_latest = df["SPY"].iloc[-1]       # scalar
spy_series = df["SPY"]                # full history of SPY
#
#-------- selecting columns on condition
# Select columns where sum == 0
zero_sum_cols = df.columns[df.sum() == 0]

# As list (most common)
zero_sum_tickers = df.columns[df.sum() == 0].tolist()

# Select those columns only
df_zero = df[zero_sum_cols]

# One-liner – columns where total return is zero
zero_cols = df.columns[(df.iloc[-1] / df.iloc[0] - 1) == 0]

# Columns with any NaN
nan_cols = df.columns[df.isna().any()]

# Columns where all values > 0
positive_cols = df.columns[df.gt(0).all()]

# Columns with max value above threshold
high_max_cols = df.columns[df.max() > 600]

# Columns whose absolute daily change never exceeds 5%
low_vol_cols = df.columns[(df.pct_change().abs() <= 0.05).all()]

# Most frequent finance pattern: columns (tickers) with positive cumulative return
positive_cumret_cols = df.columns[(df.pct_change().add(1).cumprod().iloc[-1] > 1)]

# list tail of df and only show columns where cumulative return > 0
df.tail(3).loc[:, lambda df: df.sum() > 0]
# : selects all rows, and the lambda function is applied to the columns of the tail DataFrame. The lambda function computes the sum of each column over the last 3 rows and returns a boolean Series indicating which columns have a positive sum. The .loc then selects only those columns from the tail DataFrame, effectively showing you the last 3 rows but only for the tickers that had a positive cumulative return in those rows.
#lambda df:               # receives the 3-row DataFrame
#    df.sum()             # Series: sum of each column over the 3 rows
#    > 0                  # boolean Series: True where column sum is positive
#

   # how to list tail 3 of those with weight > 0
    # weights.tail(3).loc[:, lambda df: df.sum() > 0]
    # weights.tail(3).loc[:, lambda ddf: ddf.sum() > 0]
    # weights.loc['2025-10-31':'2026-03-31'].loc[:, lambda ddf: ddf.sum() > 0]
    # weights.loc['2026-01-30'].loc[:, lambda ddf: ddf.sum() > 0]


    #  # ddf names the frame inside the lambda function, you can name it anything you want, it's just a placeholder for the DataFrame being processed. The key part is that it allows you to filter columns based on their sum, which is what we want to do to find the tickers that have a weight greater than 0 in the last 3 rows.

#===== sum across columns 

# Assuming you already have this (dates as index, tickers as columns)
# df = yf.download(tickers, start="2024-01-01", auto_adjust=True)["Close"]

# One-liner - most readable & efficient
df_sums = pd.DataFrame({
    'Total': df.sum(axis=1)
}, index=df.index)

# Alternative (exactly the same result, sometimes clearer to new pandas users)
# df_sums = df.sum(axis=1).to_frame(name='Total')

# date select operations
import pandas as pd

# 1. Strict before (most common)
before_date = df[df.index < pd.Timestamp("2025-06-01")]

# 2. Up to but not including the given date
before_or_on = df[df.index <= pd.Timestamp("2025-06-01")]

# 3. Only dates strictly before (same as #1 but more explicit)
older = df[df.index < given_date]

# 4. If you prefer .loc (same result, sometimes clearer)
older = df.loc[df.index < given_date]

# 5. If you want everything before + including a cutoff date
up_to = df.loc[:given_date]          # ← very idiomatic when index is sorted

# 1. Oldest & cleanest — using slicing on sorted DatetimeIndex
portfolio_value = df.loc[:given_date].sum(axis=1)

# 2. Using boolean mask (same result, more explicit)
portfolio_value = df[df.index < given_date].sum(axis=1)

# 3. If you want everything strictly before + chained in one line
portfolio_value = df[df.index < given_date].sum(axis=1)

#-------- selecting rows on condition
# Single condition - most common pattern
df[df["SPY"] > 500]                     # rows where SPY Close > 500

# Multiple conditions (use & | ~ and parentheses)
df[(df["SPY"] > 500) & (df["QQQ"] < df["SPY"])]  

# Any ticker > 0 (all columns)
df[df > 0]                              # keeps shape, NaN where ≤ 0

# At least one ticker > some value
df[df.gt(600).any(axis=1)]              # any ticker > 600

# All tickers positive
df[df.gt(0).all(axis=1)]

# Latest row where SPY crossed above 20-day SMA
sma20 = df["SPY"].rolling(20).mean()
df[(df["SPY"] > sma20) & (df["SPY"].shift(1) <= sma20.shift(1))]

# transpose
import pandas as pd

# Whatever your DataFrame is called
df = pd.read_csv(...)   # or however you got it

# Transpose it
df_transposed = df.T

# or do it in-place if you really want to replace the original
df = df.T
# =================
# check for any changes
#
# Option 1 – Most common & clearest
changed = df.ne(df.shift(1))          # True where value differs from previous row
any_changes_per_month = changed.any(axis=1)     # True if any ticker changed that month

# To see which months had at least one change
print(any_changes_per_month[any_changes_per_month])  

# To see how many tickers changed each month
changed_count = changed.sum(axis=1)

# how to combine the df .nlargest method with a condition. I have 2 series, one with stock prices and another with a true/false mask for each of those stock prices. I want the top 3 prices that are also true in the mask
# Assuming:
# prices = pd.Series(...)   # indexed by tickers or dates
# mask   = pd.Series(..., dtype=bool)   # same index as prices

# ────────────────────────────────────────
# Option 1 – Recommended & clearest
top3 = prices[mask].nlargest(3)

# ────────────────────────────────────────
# Option 2 – one-liner (very common)
top3 = prices.where(mask).nlargest(3)

# ────────────────────────────────────────
# Option 3 – explicit (good for readability / debugging)
filtered = prices[mask]          # keeps index
top3 = filtered.nlargest(3)

# ────────────────────────────────────────
# If you also want the mask values or original boolean series alongside:
result = prices[mask].nlargest(3).to_frame('price')
result['in_portfolio'] = mask[result.index]   # usually True, but keeps structure

"""
generate a minimal script implementing a 200 day moving average
"""
import yfinance as yf
import pandas as pd

# Pick your ticker
ticker = "SPY"

# Download adjusted prices
df = yf.download(ticker, period="2y", auto_adjust=True, progress=False)

# Calculate 200-day SMA on adjusted close
df['SMA200'] = df['Close'].rolling(window=200).mean()

# Show last few rows with price + SMA
print(df[['Close', 'SMA200']].tail(8))

# Optional: simple status
latest = df.iloc[-1]
print(f"\n{ticker} today:")
print(f"Close:    {latest['Close']:.2f}")
print(f"SMA200:   {latest['SMA200']:.2f}")
print(f"Position: {'Above' if latest['Close'] > latest['SMA200'] else 'Below'} 200-day SMA")
#============================

"""
how to select rows older than given_date
"""

# Assuming df has DatetimeIndex
older_than_given = df[df.index < given_date]

import pandas as pd

# 1. Strict before (most common)
before_date = df[df.index < pd.Timestamp("2025-06-01")]

# 2. Up to but not including the given date
before_or_on = df[df.index <= pd.Timestamp("2025-06-01")]

# 3. Only dates strictly before (same as #1 but more explicit)
older = df[df.index < given_date]

# 4. If you prefer .loc (same result, sometimes clearer)
older = df.loc[df.index < given_date]

# 5. If you want everything before + including a cutoff date
up_to = df.loc[:given_date]          # ← very idiomatic when index is sorted

import yfinance as yf

df = yf.download("SPY", start="2020-01-01", auto_adjust=True)
# now df.index is already DatetimeIndex

given_date = pd.Timestamp("2024-01-01")
old_data = df[df.index < given_date]         # everything before 2024

"""
how can I chain that to something like value.sum(axis=1)
"""

# 1. Oldest & cleanest — using slicing on sorted DatetimeIndex
portfolio_value = df.loc[:given_date].sum(axis=1)

# 2. Using boolean mask (same result, more explicit)
portfolio_value = df[df.index < given_date].sum(axis=1)

# 3. If you want everything strictly before + chained in one line
portfolio_value = df[df.index < given_date].sum(axis=1)

import yfinance as yf
import pandas as pd

tickers = ["SPY", "QQQ", "TLT"]
df = yf.download(tickers, start="2020-01-01", auto_adjust=True)["Close"]

given_date = pd.Timestamp("2024-01-01")

# Portfolio value history before given date
old_portfolio_values = df[df.index < given_date].sum(axis=1)
# or
# old_portfolio_values = df.loc[:given_date].sum(axis=1)   # ← includes given_date
#================
"""
start with a df of dates and columns of stock prices and create a df of dates and the sum of all the columns
"""
import pandas as pd

# Assuming you already have this (dates as index, tickers as columns)
# df = yf.download(tickers, start="2024-01-01", auto_adjust=True)["Close"]

# One-liner - most readable & efficient
df_sums = pd.DataFrame({
    'Total': df.sum(axis=1)
}, index=df.index)

# Alternative (exactly the same result, sometimes clearer to new pandas users)
# df_sums = df.sum(axis=1).to_frame(name='Total')

#===============================
"""
i have a df of many ticker prices for one month per row. what's the best way to check if any prices have changed from the previous month.
"""
# Option 1 – Most common & clearest
changed = df.ne(df.shift(1))          # True where value differs from previous row
any_changes_per_month = changed.any(axis=1)     # True if any ticker changed that month

# To see which months had at least one change
print(any_changes_per_month[any_changes_per_month])  

# To see how many tickers changed each month
changed_count = changed.sum(axis=1)

# Option 2 – If you want the mask of changed cells
price_changes_mask = df != df.shift(1)           # same as .ne()

# Option 3 – Get the actual differences (useful for debugging / logging)
deltas = df.diff()                     # = df - df.shift(1)
changed_mask = deltas.ne(0) & deltas.notna()

# Option 4 – Very compact: months where something changed
months_with_changes = df.index[df.ne(df.shift(1)).any(axis=1)]

"""
Hey Peter,

`.any(axis=1)` is a pandas method that checks whether **at least one value is True** along each row.

Given a DataFrame of booleans (like the one we get from `df != df.shift(1)` or `df.ne(df.shift(1))`):

```python
# Example shape: rows = months, columns = tickers
       AAPL   MSFT   TSLA   SPY
2025-01  True  False   True  False
2025-02 False   True  False   True
2025-03  True   True  False  False
```

When you run:

```python
changed.any(axis=1)
```

It returns a Series (one value per row/month):

```
2025-01     True    ← because AAPL or TSLA changed
2025-02     True    ← because MSFT or SPY changed
2025-03     True    ← because AAPL or MSFT changed
dtype: bool
```

Key points:

- `axis=1` → look **across columns** (left to right) for each row
- `.any()` → "is there at least one True in this row?"
- Result: boolean Series telling you which months had **any ticker price change**

Contrast with:

- `.any(axis=0)` → would check down each column (per ticker)
- `.all(axis=1)` → True only if **every** value in the row is True

Most common pattern we use:

```python
months_with_changes = df.index[df.ne(df.shift(1)).any(axis=1)]
```

That gives you the actual dates/months where something moved.

Clear? Let me know if you want the opposite check (.all()) explained too.
"""
"""
method to remove column or delete column - just give code examples only
"""
# 1. Most common & recommended
df = df.drop('Close', axis=1)

# 2. In-place (modifies original df)
df.drop('Close', axis=1, inplace=True)

# 3. Drop multiple columns
df = df.drop(['Open', 'High', 'Low'], axis=1)

# 4. Using del (only for single column)
del df['Volume']

# 5. Keep only wanted columns (alternative approach)
df = df[['AAPL', 'MSFT', 'Close']]          # keeps only these

# 6. Drop column by index position (if you don't know name)
df = df.drop(df.columns[3], axis=1)         # drops 4th column

#======= rename a column
# 
#===============

# Rename the first column (position 0) to a new name
data = data.rename(columns={data.columns[0]: 'new_column_name'})

data.rename(columns={data.columns[0]: 'new_column_name'}, inplace=True)

# If you know the current name
data = data.rename(columns={'old_name': 'new_column_name'})

# If you want to rename by position using a list
data.columns = ['new_column_name'] + list(data.columns[1:])




"""
code only how to append n copies of a given ticker to the df

"""
# Assuming df has Date index and columns of tickers
# and you want to append n identical copies of 'AAPL' columns

ticker = 'AAPL'
n = 3

data = yf.download(ticker, auto_adjust=True)['Close'].rename(ticker)

new_cols = [f"{ticker}_{i+1}" for i in range(n)]

df[new_cols] = data

"""
code only how to append n copies of a given ticker to the df

append a column to existing df
"""


ticker = 'AAPL'
n = 3

data = yf.download(ticker, auto_adjust=True)['Close']
data.name = ticker

new_cols = [f"{ticker}_{i+1}" for i in range(n)]

df = df.assign(**{col: data for col in new_cols})
# Detailed breakdown of this line (works cleanly with your pandas 3.0.1):

df = df.assign(**{col: data for col in new_cols})

""" # Step-by-step:

 1. new_cols is a list like: ['AAPL_1', 'AAPL_2', 'AAPL_3']

2. The dict comprehension builds:
   {
       'AAPL_1': data,   # data is the Series (same values for every column)
       'AAPL_2': data,
       'AAPL_3': data
   }

3. ** unpacks that dictionary into keyword arguments:
   df.assign(AAPL_1=data, AAPL_2=data, AAPL_3=data)

4. pandas.assign() adds each key as a new column,
   and because we pass the SAME Series object to every column,
   pandas automatically aligns on the index and broadcasts the values.

5. It returns a NEW DataFrame (does not modify df in place),
   so we re-assign it back to df.

# Why this is reliable:
# - No shape issues (pandas handles the broadcasting internally)
# - Works even if df has a different index (it aligns automatically)
# - Clean and readable for multiple identical columns
"""
# Equivalent longer version:
new_data = {col: data for col in new_cols}
df = df.assign(**new_data)

#================
# caching with parquet
import yfinance as yf
import pandas as pd
from pathlib import Path

cache_dir = Path.home() / ".cache" / "finance_data"
cache_dir.mkdir(parents=True, exist_ok=True)

ticker = "AAPL"
parquet_path = cache_dir / f"{ticker}.parquet"

if not parquet_path.exists():
    df = yf.download(ticker, auto_adjust=True, progress=False)
    df.to_parquet(parquet_path)
    print("Downloaded and cached")
else:
    print("Using cache")

df = pd.read_parquet(parquet_path)
print(df.tail())


# To check if all elements in a pandas DataFrame are True (more pythonic than the double .any() you mentioned for "any"):
all_true = df.all().all()
all_true = (df == True).all().all()

#function to test if 2 frames have same values

def frames_equal(df1, df2):
    """Return True if two DataFrames have exactly the same values (ignores index/columns names)."""
    return df1.equals(df2) or (df1.values == df2.values).all()

#Compare DataFrames ignoring index
def frames_equal_ignore_index(df1, df2):
    return (df1.values == df2.values).all()

# show rows of differences

diff_mask = np.any(df1.values != df2.values, axis=1)
result = df1[diff_mask]

# if df structure is identical:
# Assuming df1 and df2 have the same shape and index/columns
diff_mask = (df1 != df2).any(axis=1)
result = df1[diff_mask]

# I have a frame with 3 cols - date, ticker, price - how to sort by date, ticker
import pandas as pd

df = df.sort_values(by=['date', 'ticker'])

# how to designate asc or desc
df = df.sort_values(by=['date', 'ticker'], ascending=[True, True])

# using datetime library generate a string for filenames that includes year-month-day-hour-min
from datetime import datetime

# Current timestamp in year-month-day-hour-minute format
timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")

print(timestamp)  # Example: 2026-03-28-07-38

# how to show the rows of df1 where df1.values != df2.values
# Assuming df1 and df2 have the same shape and index/columns
diff_mask = (df1 != df2).any(axis=1)
result = df1[diff_mask]


# if you are strictly comparing values
import numpy as np

diff_mask = np.any(df1.values != df2.values, axis=1)
result = df1[diff_mask]

# If you want to see the actual differences side-by-side, use:
diff = df1[(df1 != df2).any(axis=1)].compare(df2)

# how to get index info 
import pandas as pd

# Basic index information
print(df.index)                    # shows the index object
print(type(df.index))              # what kind of index is it?
print(df.index.names)              # list of level names (None if single index)

# Quick check if it's a MultiIndex
print(isinstance(df.index, pd.MultiIndex))   # True/False - this is what you usually want

# More detailed MultiIndex info
if isinstance(df.index, pd.MultiIndex):
    print("MultiIndex detected")
    print(f"Number of levels: {df.index.nlevels}")
    print(f"Level names: {df.index.names}")
    print(f"Level values example:\n{df.index[:5]}")  # first few tuples
else:
    print("Single level index")
    print(f"Name: {df.index.name}")

# inspect dataframe indexes or index

import pandas as pd

# Basic index information
print(df.index)                    # shows the index object
print(type(df.index))              # what kind of index is it?
print(df.index.names)              # list of level names (None if single index)

# Quick check if it's a MultiIndex
print(isinstance(df.index, pd.MultiIndex))   # True/False - this is what you usually want

# More detailed MultiIndex info
if isinstance(df.index, pd.MultiIndex):
    print("MultiIndex detected")
    print(f"Number of levels: {df.index.nlevels}")
    print(f"Level names: {df.index.names}")
    print(f"Level values example:\n{df.index[:5]}")  # first few tuples
else:
    print("Single level index")
    print(f"Name: {df.index.name}")

# one-lines to inspect index
# Quick checks (recommended)
df.index.nlevels          # 1 = regular index, >1 = MultiIndex
isinstance(df.index, pd.MultiIndex)
df.index.names            # None or list of names

# Full info in one go
df.index.to_frame(index=False)   # shows index as columns (great for MultiIndex)

# check for multi-indexing
print("cf columns:")
print(cf.columns)
print("cf.columns.names:", cf.columns.names)
print("\ndf y columns:")
print(dfy.columns)
print("dfy.columns.names:", dfy.columns.names)


# flatten multiindex
# Option 1: flatten the MultiIndex (recommended)
dfy.columns = dfy.columns.get_level_values(1)   # or .get_level_values('Ticker')

# Option 2: drop the level name
dfy.columns.names = [None] * len(dfy.columns.names)

# Option 3: reset to simple columns if it's from yfinance or similar
dfy = dfy.droplevel(0, axis=1)   # if first level is useless
#
# test for values = NaN
# o test for NaN values in a pandas DataFrame (indexed by date, with ticker columns like 'AAPL', 'MSFT', etc.) and show the dates where any ticker has NaN:

import pandas as pd
import numpy as np

# Assuming df is your DataFrame from yf.download(..., auto_adjust=True)
# df has Date index and ticker columns with 'Close' prices

# 1. Find dates where ANY ticker has NaN
nan_dates_any = df[df.isna().any(axis=1)].index

print("Dates with any NaN:")
print(nan_dates_any)

# 2. Show the actual rows with NaNs (recommended for inspection)
print("\nRows with NaNs:")
print(df[df.isna().any(axis=1)])

# To detect rows with NO NaN values (i.e., all tickers have valid prices):
# df is your pandas DataFrame (Date index, ticker columns, auto_adjust=True)

# Rows where NONE of the values are NaN
complete_rows = df[df.notna().all(axis=1)]

# Or equivalently:
complete_rows = df.dropna()   # simplest and most common

print(complete_rows)

# One-liner to get just the dates:
complete_dates = df[df.notna().all(axis=1)].index
# or
complete_dates = df.dropna().index

df.dropna() # when you want to remove incomplete rows.

# offset days by X e.g. yesterday
yesterday = (date.today() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

# return a date N months ago
import pandas as pd

def months_ago(n: int) -> pd.Timestamp:
    return pd.Timestamp.now() - pd.DateOffset(months=n)

"""
how to create a new dataframe of NaN values given a date range and the columns from an existing dataframe. the date range should produce an index containing every day from start date to and including end date. all values are NaN
"""
import pandas as pd
import numpy as np
from datetime import datetime

# Assuming you have an existing df with the desired columns
# and you want a new df with the same columns but NaN values

def create_nan_df(existing_df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    # Create date range with daily frequency (includes both start and end)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create new DataFrame with NaN values, same columns, date index
    nan_df = pd.DataFrame(
        index=date_range,
        columns=existing_df.columns,
        dtype=float  # or 'object' if mixed types
    )
    
    return nan_df

# how to make a df column (e.g. date) the index

import pandas as pd

# Assuming df has a 'date' column
df = df.set_index('date')

# If the column is not already datetime (recommended)
df.index = pd.to_datetime(df.index)

df.set_index('date', inplace=True) # in place

df.index.name = None # if you want to remove the index name.

# how to add a second index
import pandas as pd

# Add a second level to the existing index
df = df.set_index('second_column', append=True)

# Or set both at once (if you still have the original 'date' column)
df = df.set_index(['date', 'second_column'])

#============================
# how to display .values as %
import pandas as pd
import yfinance as yf

# Example: get data (using your installed versions)
df = yf.download("SPY", auto_adjust=True)

# 1. Convert to percentage (multiply by 100) and format as string with %
df['pct'] = (df['Close'].pct_change() * 100).round(2).astype(str) + '%'

# 2. Or keep as numeric but display with % in output
print((df['Close'].pct_change() * 100).round(2).map("{:.2f}%".format))

# For a whole DataFrame display
pd.options.display.float_format = '{:.2%}'.format
print(df['Close'].pct_change())   # now shows as 0.12% etc.

"""
how to combine the values from 2 series "today_list" and "today_prices" with identical index and Name into one dataframe

"""

import pandas as pd

# Assuming today_list and today_prices are your Series with same index
df = pd.DataFrame({
    today_list.name: today_list,
    today_prices.name: today_prices
})

# more concise way (if you want to keep the original Series names as column headers):

df = pd.concat([today_list, today_prices], axis=1)

# create and assign column names
df = pd.DataFrame({
    'list_column': today_list,      # your custom name for today_list
    'price_column': today_prices    # your custom name for today_prices
})

"""print a df and specify formatting for a given column e.g. % for "Avg Momentum" or currency for "Adj Close" columns
"""

# Fix for "The '.style' accessor requires jinja2" error (no extra installs needed)

import yfinance as yf
import pandas as pd

df = yf.download("SPY", start="2025-01-01", auto_adjust=True, progress=False)
df["Avg Momentum"] = df["Close"].pct_change(20).rolling(10).mean() * 100

# Simple formatting without .style (uses pandas 3.0.1 built-ins)
print(df[["Close", "Avg Momentum"]].tail(10).to_string(
    float_format=lambda x: f"${x:,.2f}" if x > 10 else f"{x:.2f}%"
))

# Better per-column control (still no jinja2)
formatted = df[["Close", "Avg Momentum"]].tail(10).copy()

# Apply formatting manually for print
print("\nFormatted output:")
for idx, row in formatted.iterrows():
    close_str = f"${row['Close']:,.2f}"
    mom_str = f"{row['Avg Momentum']:.2f}%"
    print(f"{idx.date()}   Close: {close_str:>12}   Avg Momentum: {mom_str:>10}")