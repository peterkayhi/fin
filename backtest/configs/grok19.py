""" 
grok19.py
Grok's recommendation to expand the original 13 picks to 19,
https://grok.com/share/bGVnYWN5_83844bc9-d263-48e2-a614-05e4659f090a


"""
import sys
from pathlib import Path
from datetime import date
# Make backtest/ the root for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # parents[1] = backtest/
from src.papaSrc import run_papa

CONFIG = {
    "tickers_param": [
        # US Equities (now full size spectrum + blend)
        "VTI",   # US Total Market
        "VTV",   # Large Value
        "VUG",   # Large Growth
        "VO",    # Mid Cap Blend
        "VIOV",  # Small Value
        "VIOG",  # Small Growth
        
        # International Equities (added small-cap exposure)
        "VEA",   # Developed Markets
        "VWO",   # Emerging Markets
        "VSS",   # International Small Cap
        
        # Real Estate (now global)
        "VNQ",   # US REITs
        "VNQI",  # Global ex-US REITs
        
        # Commodities & Precious Metals
        "PDBC",  # Broad Commodities
        "IAU",   # Gold
        
        # Fixed Income (added safety + inflation + credit)
        "BIL",   # Short-Term Treasuries (cash proxy)
        "VGIT",  # Intermediate Treasuries
        "EDV",   # Long Treasuries
        "VTIP",  # TIPS (inflation-protected)
        "VCLT",  # Long Corporate Bonds
        "JNK",   # High Yield Bonds
        "BNDX"   # International Bonds
    ],
    "file_suffix_param": "Grok19L"
}


if __name__ == "__main__":
    run_papa(**CONFIG)
