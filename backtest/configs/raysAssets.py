""" 
raysAssets.py
based on the below grok output:
https://grok.com/share/bGVnYWN5_9d190ea1-b9d9-4226-8a67-ad53170590ac

"""
import sys
from pathlib import Path
from datetime import date
# Make backtest/ the root for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # parents[1] = backtest/
from src.papaSrc import run_papa

CONFIG = {
    "tickers_param": ["VOO", "VT", "TLT", "BND", "VNQ", "TIP", "VTIP", "GLD"],
    "file_suffix_param": "raysAssetsL"
}

if __name__ == "__main__":
    run_papa(**CONFIG)
