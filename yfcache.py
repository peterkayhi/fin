import yfinance as yf
import pandas as pd
import duckdb
from datetime import date
import os
import logging
from typing import NamedTuple, List

class YfCacheResult(NamedTuple):
    final_df: pd.DataFrame
    missed_tickers: List[str]
    needed_starts: List[pd.Timestamp]

class yfcache:
    def __init__(self):
        # Ensure the cache directory exists
        db_path = os.path.expanduser("~/.cache/finance_data/yfcache.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Connect to DuckDB (creates file if it doesn't exist)
        self.con = duckdb.connect(db_path)
        # Store individual price points to allow flexible subset retrieval and range slicing
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                date DATE,
                ticker VARCHAR,
                price DOUBLE,
                PRIMARY KEY (date, ticker)
            )
        """)

        # Setup a dedicated logger for yfcache to avoid configuration conflicts
        self.logger = logging.getLogger("yfcache")
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            self.logger.propagate = False  # Keep cache logs isolated from the root logger
            formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            
            for handler in [logging.FileHandler("/Users/peterkay/Downloads/backtestFiles/yfcache.app.log", mode='a', encoding='utf-8'), logging.StreamHandler()]:
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
        self.logger.info(f"yfcache initialized with database at {db_path}")

    def _key(self, ticker_list, start_date, end_date):
        # 1. Standardize ticker_list to list and create sorted key string
        if isinstance(ticker_list, str):
            ticker_list = [ticker_list]
        tickers_key = ",".join(sorted(ticker_list))
        
        # 2. Standardize dates to ISO strings for consistency
        if isinstance(start_date, date):
            start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, date):
            end_date = end_date.strftime("%Y-%m-%d")
            
        # Ensure chronological order to prevent SQL BETWEEN and date_range failures
        s, e = sorted([start_date, end_date])

        # Convert to Timestamps to utilize pandas business day offsets
        s_ts = pd.to_datetime(s)
        e_ts = pd.to_datetime(e)

        # If end_date is today or in the future, cap it at yesterday to ensure we only request completed days
        if e_ts.date() >= pd.Timestamp.now().date():
            e_ts = e_ts - pd.Timedelta(days=1)

        # Ensure both dates are business days 
        # start date rolls forward
        s = pd.offsets.BusinessDay().rollforward(s_ts).strftime("%Y-%m-%d")
        # end date rolls back 
        e = pd.offsets.BusinessDay().rollback(e_ts).strftime("%Y-%m-%d")
        return ticker_list, tickers_key, s, e
    
    def get(self, ticker_list, start_date, end_date, skip_cache = False):
        # Centralized input cleaning and standardization via _key
        ticker_list, _, start_date, end_date = self._key(ticker_list, start_date, end_date)
        self.logger.info(f"Requesting data for tickers: {ticker_list} from {start_date} to {end_date}")

        # 1. Identify which tickers need a cache update
        missed_tickers = []
        needed_starts = []
        requested_start_dt = pd.to_datetime(start_date)

        for ticker in ticker_list:
            # Check if we have data for this ticker covering the requested range
            res = self.con.execute("""
                SELECT min(date)::VARCHAR, max(date)::VARCHAR FROM prices WHERE ticker = ?
            """, [ticker]).fetchone()
            
            # Download if ticker is totally missing, or cached range is insufficient
            # or we're being forced to ignore (skip) cache
            if not res or res[0] is None or res[0] > start_date or res[1] < end_date or skip_cache:
                missed_tickers.append(ticker)
                
                # Determine the most efficient start date for this specific ticker
                if not res or res[0] is None or res[0] > start_date or skip_cache:
                    needed_starts.append(requested_start_dt)
                else:
                    # Per request: start one day after the latest day in cache
                    needed_starts.append(pd.to_datetime(res[1]) + pd.Timedelta(days=1))

        # 2. Fetch and merge missing data or if we're explicitly told to skip caching
        if missed_tickers:
            self.logger.info(f"Tickers missing or needing update: {missed_tickers} with needed starts: {needed_starts}")
            # Use the earliest start date required by any ticker in the missing batch
            download_start = min(needed_starts).strftime("%Y-%m-%d")
            today = date.today().strftime("%Y-%m-%d") # Yahoo Finance gets exclusive end date, so we can use today to get up to yesterday's data
            new_data = yf.download(missed_tickers, start=download_start, end=today, auto_adjust=True, progress=False)
            
            if not new_data.empty:
                # Extract Close prices (auto_adjust moves Adj Close here)
                new_data = new_data["Close"]
                
                # Standardize result to DataFrame even for single tickers
                if isinstance(new_data, pd.Series):
                    new_data = new_data.to_frame(name=missed_tickers[0])
                
                new_data.index.name = 'date'
                # Transform to "Long" format: columns [date, ticker, price]
                df_long = new_data.reset_index().melt(id_vars='date', var_name='ticker', value_name='price')
                df_long = df_long.dropna(subset=['price']) # Drop non-trading days/NaNs

                # Upsert into DuckDB
                self.con.execute("INSERT OR REPLACE INTO prices SELECT * FROM df_long")

        else: 
            self.logger.info("Cache hit")

        # 3. Retrieve the final requested subset from the database
        placeholders = ', '.join(['?'] * len(ticker_list))
        query = f"""
            SELECT date, ticker, price FROM prices 
            WHERE ticker IN ({placeholders}) AND date >= ? AND date <= ?
        """
        params = ticker_list + [start_date, end_date]
        # df_table = df_table.dropna(subset=['price']) # Drop non-trading days/NaNs
        results_df = self.con.execute(query, params).df().dropna(subset=['price']) # Drop non-trading days/NaNs

        if results_df.empty:
            # Create the full calendar range requested by the user
            all_dates = pd.date_range(start=start_date, end=end_date)
            # Return an empty frame with requested tickers and dates (filled with NaN)
            final_df = pd.DataFrame(index=all_dates, columns=ticker_list)
            return YfCacheResult(final_df, missed_tickers, needed_starts)

        # Pivot the database results back to "Wide" format (Date index, Ticker columns)
        final_df = results_df.pivot(index='date', columns='ticker', values='price')
        
        # Ensure all calendar days and all requested tickers are present
        final_df.index = pd.to_datetime(final_df.index)
        return YfCacheResult(final_df, missed_tickers, needed_starts)
