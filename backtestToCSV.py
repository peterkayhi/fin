"""
backtestToCSV.py

exports adjusted closes from yfinance data to csv
forked from backtesterWorking.py 



"""


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
analysis_start = "2015-01-01"   # inclusive lower bound
analysis_end   = "2016-02-27"   # exclusive upper bound

INITIAL_VALUE = 100_000
VALUE_PER_BUCKET = INITIAL_VALUE // 3

# Momentum periods in **trading days**
PERIODS = [63, 126, 252]
PERIOD_NAMES = ["63", "126", "252"]

# Rebalancing threshold (as decimal) — 20% drift triggers full portfolio rebalance
REBALANCE_THRESHOLD = 0.20

# suffix gets appended to each csv file
FILE_SUFFIX = "2007-2008adjustedCloses"

OUTPUT_DIR = Path("/Users/peterkay/Downloads/csv")
OUTPUT_DIR.mkdir(exist_ok=True)

# ────────────────────────────────────────────────
# 1. Download adjusted closes
# ────────────────────────────────────────────────

print("Downloading data (auto_adjust=True for dividend/split adjusted closes)...")
data = yf.download(
    tickers=TICKERS,
    start=analysis_start,
    end=analysis_end,
    auto_adjust=True,
    progress=True
)

# Extract adjusted Close — yfinance usually returns multi-index
if "Close" in data.columns.levels[0]:
    adj_close = data["Close"]
else:
    raise ValueError("Could not find 'Close' in downloaded data")


yfdata = pd.DataFrame(adj_close)
dataFileName = "yfdataexport" + FILE_SUFFIX + ".csv"
yfdata.to_csv(OUTPUT_DIR / dataFileName, index=True)
print(f"Saved yfdataexport.csv  ({len(yfdata)} rows)")
print(f"filename: {OUTPUT_DIR}/yfdataexport{FILE_SUFFIX}.csv")

print("\nFinished! Check folder:", OUTPUT_DIR.resolve())