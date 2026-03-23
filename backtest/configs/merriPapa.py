""" 
merriman4.py
Papa bear 13 + Merriman's 4 fund portfolio, with monthly rebalancing and independent bucket management 
"""
import sys
from pathlib import Path
from datetime import date
# Make backtest/ the root for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # parents[1] = backtest/
from src.papaSrc import run_papa

CONFIG = {
    # avantis "tickers_param": ["AVUS", "AVLV", "AVSC", "AVUV"],
    "tickers_param": [
        "VOO", "VONV", "VIOV", "VIOO", 'VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO', 'VNQ', 'PDBC', 'IAU', 'EDV', 'VGIT', 'VCLT', 'BNDX'
    ],
    "file_suffix_param": "merripapaL"
}

if __name__ == "__main__":
    run_papa(**CONFIG)
