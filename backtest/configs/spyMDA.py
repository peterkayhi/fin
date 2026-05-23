""" 
spy.py
SPY only portfolio
"""
import sys
from pathlib import Path
from datetime import date


from backtest.src.momda import run_momda

CONFIG = {
    # avantis "tickers_param": ["AVUS", "AVLV", "AVSC", "AVUV"],
    "tickers_param": ['SPY'],
    "mda_param": 200,
    "file_prefix": "spyMda"
}

if __name__ == "__main__":
    run_momda(**CONFIG)
