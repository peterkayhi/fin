""" 
pappapaRamseyaOrig.py
Papa bear original 13 plus Dave Ramsey's 8 picks, 
"""
import sys
from pathlib import Path
from datetime import date
# Make backtest/ the root for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # parents[1] = backtest/
from src.papaSrc import run_papa

CONFIG = {
    "tickers_param": [
        'VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO', 'VNQ',
        'PDBC', 'IAU', 'EDV', 'VGIT', 'VCLT', 'BNDX', "QQQ", "VIG", "SCHD", "MGK", "IWY", "VXUS"
    ],
    "file_suffix_param": "PapaRamseyL"
}


if __name__ == "__main__":
    run_papa(**CONFIG)
