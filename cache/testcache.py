"""
testcache.py
works the yfcache to analyze performance and caching results

next bug: yf downloads are off by a hair, and skip_cache results don't seem to reflect latest yahoo donlaods
e.g. if skip cache essentially does a refresh, then it should match yahoo which happens only a few seconds later. 
find out why skip cache doesn't match yahoo
"""
import sys
from pathlib import Path
import yfinance as yf
import pandas as pd
from yfcache import yfcache
from datetime import datetime

# Example script using yfcache class
cache = yfcache()

tickers = [
    'VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO', 'VNQ',
    'PDBC', 'IAU', 'EDV', 'VGIT', 'VCLT', 'BNDX'
]

startd="2026-03-28" # test weekend logic
endd=datetime.today().strftime("%Y-%m-%d")
skip_cache = True
timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
csvfile = f"/Users/peterkay/Downloads/csv/yfdl{timestamp}.csv"

def same_result (dfa, dfb):
    return (dfa.final_df.values == dfb.final_df.values).all()

# first pull the cached data
cache_result = cache.get(tickers, startd, endd) # now the cached version

# then skip cache i.e. download again from YF and refresh cache
skip_result = cache.get(tickers, startd, endd, skip_cache)

after_cache = cache.get(tickers, startd, endd) # now the recently refreshed cached version

# then directly download from YF
dfy = yf.download(tickers, start=startd, end=endd, auto_adjust=True, progress=False)["Close"] 
dfy.to_csv(csvfile)

# report the results
print("*** Cache hits ***")
print(f"Cached Tickers missed: {cache_result.missed_tickers}. Needed starts: {cache_result.needed_starts}")
print(f"Skip cache Tickers missed: {skip_result.missed_tickers}. Needed starts: {skip_result.needed_starts}")
print(f"TEST:0 After cache Tickers missed: {after_cache.missed_tickers}. Needed starts: {after_cache.needed_starts}")
print("\n\n")

print("*** Comparisons ***")
print(f"cached matches skip_cache?{same_result(skip_result, cache_result)}")
# generates error below - different size frames. need to figure out what do to here - have it so that cache.get results essentially identical yahoo frame, or include all the NaN values.  perhaps do the NaN conversions internally to keep the caching consistent and high performing but remove all Nan values when returning back to Yahoo, like in the original code. 
print(f"skip_cache matches Yahoo?{(skip_result.final_df.values == dfy.values).all()}")
print(f"cache matches Yahoo?{(cache_result.final_df.values == dfy.values).all()}")
print(f"TEST: TRUE after cache matches skip_cache?{same_result(skip_result, after_cache)}")
print("\n\n")

print("*** Differences ***")
print(f"Diff skip_cache:dfy {(abs(skip_result.final_df - dfy) / skip_result.final_df).sum().sum()}")
print(f"Diff cached:dfy {(abs(cache_result.final_df - dfy) / cache_result.final_df).sum().sum()}")
print(f"Diff cached:skip_cache {(abs(cache_result.final_df.values - skip_result.final_df.values) / cache_result.final_df.values).sum().sum()}")
print(f"after cache:skip_cache {(abs(after_cache.final_df.values - skip_result.final_df.values) / after_cache.final_df.values).sum().sum()}")
print("\n\n")

pass