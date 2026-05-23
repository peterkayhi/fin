""" 
raysAssets.py
based on the below grok output:
https://grok.com/share/bGVnYWN5_9d190ea1-b9d9-4226-8a67-ad53170590ac

"""
import sys
from pathlib import Path
from datetime import date

from backtest.src.momda import run_momda

CONFIG = {
    "tickers_param": ["VOO", "VT", "TLT", "BND", "VNQ", "TIP", "VTIP", "GLD"],
    "file_prefix": "raysAssets"
}

if __name__ == "__main__":
    run_momda(**CONFIG)
