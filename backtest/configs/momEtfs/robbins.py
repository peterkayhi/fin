""" 
robbins.py

copy and paste from AI on Robbins all season portfolio. Includes weights which we don't do yet. 

"""
import sys
from pathlib import Path
from datetime import date
from backtest.src.momda import run_momda

all_seasons_portfolio = [
    {"ticker": "VTI", "weight": 0.30, "name": "Vanguard Total Stock Market ETF", "class": "Stocks"},
    {"ticker": "TLT", "weight": 0.40, "name": "iShares 20+ Year Treasury Bond ETF", "class": "Long-Term Treasuries"},
    {"ticker": "IEF", "weight": 0.15, "name": "iShares 7-10 Year Treasury Bond ETF", "class": "Intermediate Treasuries"},
    {"ticker": "GLD", "weight": 0.075, "name": "SPDR Gold Shares", "class": "Gold"},
    {"ticker": "DBC", "weight": 0.075, "name": "Invesco DB Commodity Index Tracking Fund", "class": "Commodities"}
]

# Simple dictionary for quick calculations
weights = {
    "VTI": 0.30,
    "TLT": 0.40,
    "IEF": 0.15,
    "GLD": 0.075,
    "DBC": 0.075
}

all_seasons_etfs = ['VTI', 'TLT', 'IEF', 'GLD', 'DBC']

CONFIG = {
    "tickers_param": all_seasons_etfs,
    # "mda_param": 200,
    # "top_assets": 3,
   #"rebalance_trigger": 1.0,
    "file_prefix": "Robbins"
}


if __name__ == "__main__":
    run_momda(**CONFIG)
