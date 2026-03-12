"""
pandaCsv.py

Loads a CSV file into a Pandas DataFrame and performs basic data manipulation.
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
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# Set 'Date' as the index.
# df.set_index() makes 'Date' the row labels instead of 0,1,2...
# Useful for time series data like stocks, so you can slice by date easily.
# inplace=True modifies the DataFrame directly without creating a copy.
df.set_index('date', inplace=True)

# Sort by index (dates) if not already sorted.
# df.sort_index() ensures chronological order, ascending=True by default.
# Important for historical data that might be imported out of order.
df.sort_index(inplace=True)

# Print the DataFrame after these changes.
print("\nDataFrame after date conversion and indexing:")
print(df.head())

# Select specific columns.
# df[['Open', 'Close']] creates a new DataFrame with just those columns.
# Like selecting columns in a SQL query or spreadsheet.
# Useful for focusing on price changes without volume or highs/lows.
prices = df[['Open', 'Close']]
print("\nSelected 'Open' and 'Close' columns:")
print(prices.head())

# Filter rows based on a condition.
# df[df['Close'] > 100] returns rows where Close price > 100.
# This uses boolean indexing: creates a mask of True/False, then subsets.
# Great for finding days when a stock crossed a threshold.
high_close_days = df[df['Close'] > 100]  # Adjust 100 to a relevant value for your data.
print("\nDays where Close > 100:")
print(high_close_days.head())

# Calculate a new column: daily return.
# df['Return'] = (close - open) / open, as percentage.
# Vectorized operation: applies to whole column at once, fast even for large data.
# NumPy handles the math under the hood.
df['Daily Return'] = (df['Close'] - df['Open']) / df['Open'] * 100
print("\nDataFrame with new 'Daily Return' column:")
print(df[['Open', 'Close', 'Daily Return']].head())

# Handle missing values.
# df.fillna(0) replaces NaNs with 0; here for 'Volume' as example.
# For stocks, missing prices might mean holidays—could forward-fill instead: df.fillna(method='ffill').
# df.dropna() would remove rows with any NaNs.
df['Volume'].fillna(0, inplace=True)  # Assuming 'Volume' might have misses.
print("\nAfter filling missing volumes with 0:")
print(df['Volume'].head())

# Group by and aggregate.
# Assuming monthly data, but for daily: resample to monthly.
# df.resample('M') groups by month-end, .mean() averages.
# Requires datetime index. Great for monthly average close prices.
monthly_avg = df.resample('M').mean()
print("\nMonthly average prices:")
print(monthly_avg['Close'].head())

# Rolling window calculation: 5-day moving average.
# df['Close'].rolling(5).mean() computes average over last 5 rows.
# Shifts window down the data. Useful for smoothing stock trends.
df['5-Day MA'] = df['Close'].rolling(window=5).mean()
print("\nDataFrame with 5-day moving average:")
print(df[['Close', '5-Day MA']].head(10))  # Show 10 to see the window fill.

# Pivot table example (if you had multiple stocks, but simulating here).
# For single stock, not super useful, but imagine adding a 'Symbol' column.
# pd.pivot_table() like Excel pivots: aggregates data.
# Here, average Close by year (extract year from index).
df['Year'] = df.index.year
pivot = pd.pivot_table(df, values='Close', index='Year', aggfunc='mean')
print("\nAverage Close by Year (pivot table):")
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

print("\nFinished processing CSV with Pandas.")
