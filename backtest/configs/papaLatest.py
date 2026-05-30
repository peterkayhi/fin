""" 
papaLatest.py
Papa bear original 13 for yesterday - minimal history is pulled
Designed to be used to give latest investment data
"""
import pandas as pd
from datetime import date
from backtest.src.momda import run_momda

today = date.today().strftime("%Y-%m-%d")

CONFIG = {
    "tickers_param": [
        'VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO', 'VNQ',
        'PDBC', 'IAU', 'EDV', 'VGIT', 'VCLT', 'BNDX'
    ],
    "start_date": (date.today() - pd.DateOffset(months=2)).strftime("%Y-%m-%d"), # 2 months ago
    # usually defaults to last business momth end 
    "end_date": today, # today
    "skip_cache": True, # we want live data so make sure we're getting latest
    "verbose": False,
    "file_prefix": f"papaLatest{today.replace('-', '')}"
}


if __name__ == "__main__":
    run_momda(**CONFIG)
