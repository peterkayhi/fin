""" 
gemini20.py
NotebookLM's recommendation to expand the original 13 picks to 20,


"""
import sys
from pathlib import Path
from datetime import date
# Make backtest/ the root for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # parents[1] = backtest/
from src.papaSrc import run_papa

CONFIG = {
    "tickers_param": [
        # US Equities
        'VV',    # US Large Cap Blend
        'MTUM',  # US Large Cap Momentum
        'IJH',   # US Mid Cap
        'VIOO',  # US Small Cap Blend
        'IWC',   # US Micro-Cap
        
        # International & Emerging Equities
        'VEA',   # International Developed Blend
        'VWO',   # Emerging Markets
        'VSS',   # International Small Cap Blend
        'EWX',   # Emerging Markets Small Cap
        
        # Real Assets & Alternatives
        'VNQ',   # US REITs
        'VNQI',  # International REITs
        'DBC',   # Broad Commodities
        'IAU',   # Gold
        'TREE',  # Timber
        
        # Fixed Income
        'VGIT',  # Intermediate-Term US Treasuries
        'TLT',   # Long-Term US Treasuries
        'VTIP',  # Treasury Inflation-Protected Securities (TIPS)
        'LQD',   # US Corporate Bonds (Investment Grade)
        'HYG',   # US High Yield Credit (Junk Bonds)
        'BNDX',  # Non-US Developed Bonds
        'VWOB'   # Emerging Market Bonds
    ],
    "file_suffix_param": "Gemini20L"
}


if __name__ == "__main__":
    run_papa(**CONFIG)
