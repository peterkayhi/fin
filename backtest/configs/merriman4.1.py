""" 
merriman4.py
Merriman's 4 fund portfolio, with monthly rebalancing and independent bucket management 
"""
import sys
from pathlib import Path
from datetime import date
# Make backtest/ the root for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # parents[1] = backtest/
from src.papaSrc import run_papa

CONFIG = {
    "tickers_param": ["VOO", "VONV", "VIOV", "VIOO"],
    "file_suffix_param": "merriman4L"
}

if __name__ == "__main__":
    run_papa(**CONFIG)
