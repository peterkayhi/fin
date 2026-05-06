""" 
papaLatest.py
Papa bear original 13 for yesterday - minimal history is pulled
Designed to be used to give latest investment data
"""
import pandas as pd
from datetime import date, timedelta
from backtest.src.momda import run_momda

today = date.today().strftime("%Y-%m-%d")

CONFIG = {
    "tickers_param": [
        'VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO', 'VNQ',
        'PDBC', 'IAU', 'EDV', 'VGIT', 'VCLT', 'BNDX'
    ],
    "start_date": (date.today() - pd.DateOffset(months=2)).strftime("%Y-%m-%d"), # 6 months ago
    # usually defaults to last business momth end 
    "end_date": today, # today
    "verbose": False,
    "file_prefix": f"papaLatest{today.replace('-', '')}"
}

pass

if __name__ == "__main__":
    today_dt = date.today()
    # today_dt = pd.to_datetime('2026-04-24').date() #temp test
    # weekday() returns 4 for Friday. 
    # It's the last Friday if adding 7 days changes the month.
    if today_dt.weekday() == 4 and (today_dt + timedelta(days=7)).month != today_dt.month:
        run_momda(**CONFIG)
    else:
        print(f"Today in history: {today}")
