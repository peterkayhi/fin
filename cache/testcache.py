
import sys
from pathlib import Path
import yfinance as yf
import pandas as pd
from yfcache import yfcache
from datetime import date

# Example script using yfcache class
cache = yfcache()

tickers=['AAPL', 'VIOV']
startd="2019-09-12"
endd="2019-12-31"
skip_cache = True

# first pull the cached data
cache_result = cache.get(tickers, startd, endd) # now the cached version
df_cached = cache_result.final_df

# then skip cache i.e. download again from YF
skip_result = cache.get(tickers, startd, endd, skip_cache)
df_skip_cache = skip_result.final_df

# then directly download from YF
dfy = yf.download(tickers, start=startd, end=endd, auto_adjust=True, progress=False)["Close"] 

# report the results
print(f"Cached Tickers missed: {cache_result.missed_tickers}. Needed starts: {cache_result.needed_starts}")
print(f"Skip cache Tickers missed: {skip_result.missed_tickers}. Needed starts: {skip_result.needed_starts}")


print(f"cached matches skip_cache?{(df_skip_cache.values == df_cached.values).all()}")
print(f"cached matches Yahoo?{(df_skip_cache.values == dfy.values).all()}")
print(f"skip_cache matches Yahoo?{(df_cached.values == dfy.values).all()}")

print(f"Diff skip_cache:dfy {(abs(df_skip_cache - dfy) / df_skip_cache).sum()}")
print(f"Diff cached:dfy {(abs(df_cached - dfy) / df_cached).sum()}")
print(f"Diff cached:skip_cache {(abs(df_cached - df_skip_cache) / df_cached).sum()}")

#


pass