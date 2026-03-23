""" 
spy.py
SPY only portfolio
"""
import sys
from pathlib import Path
from datetime import date
# Make backtest/ the root for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # parents[1] = backtest/
from src.papaSrc import run_papa

CONFIG = {
    # avantis "tickers_param": ["AVUS", "AVLV", "AVSC", "AVUV"],
    "tickers_param": ["SPY", "SPY", "SPY"],
    "file_suffix_param": "spyL"
}

if __name__ == "__main__":
    run_papa(**CONFIG)
