""" 
rayBroad.py

based on digital ray's recommendation on a broad portfoli. 

"""
import sys
from pathlib import Path
from datetime import date

from backtest.src.momda import run_momda

CONFIG = {
    "tickers_param": [
        # U.S. & global large‑cap equity indices
        'SPY',     # SPDR S&P 500 ETF Trust
        'IVV',     # iShares Core S&P 500 ETF

        # International developed‑market equities
        'IEFA',    # iShares Core MSCI EAFE ETF
        'VEA',     # Vanguard FTSE Developed Markets ETF

        # Emerging‑market equities
        'VWO',     # Vanguard FTSE Emerging Markets ETF
        'IEMG',    # iShares Core MSCI Emerging Markets ETF

        # Dividend‑focused equity funds or high‑dividend ETFs
        'VYM',     # Vanguard High Dividend Yield ETF
        'SCHD',    # Schwab U.S. Dividend Equity ETF

        # Broad industrials & materials sectors (commodities‑linked equities)
        'XLI',     # Industrial Select Sector SPDR Fund
        'XLB',     # Materials Select Sector SPDR Fund

        # Short‑duration Treasury ETFs
        'SHY',     # iShares 1-3 Year Treasury Bond ETF
        'BIL',     # SPDR Bloomberg 1-3 Month T-Bill ETF

        # Long‑duration Treasury ETFs
        'TLT',     # iShares 20+ Year Treasury Bond ETF
        'EDV',     # Vanguard Extended Duration Treasury ETF

        # Investment‑grade corporate bond indices
        'LQD',     # iShares iBoxx $ Investment Grade Corporate Bond ETF
        'VCIT',    # Vanguard Intermediate-Term Corporate Bond ETF

        # High‑yield (junk) bond indices
        'HYG',     # iShares iBoxx $ High Yield Corporate Bond ETF
        'JNK',     # SPDR Bloomberg High Yield Bond ETF

        # Broad commodity index
        'DBC',     # Invesco DB Commodity Index Tracking Fund
        'GSG',     # iShares S&P GSCI Commodity-Indexed Trust

        # Real‑estate investment trusts (REITs)
        'VNQ',     # Vanguard Real Estate ETF
        'IYR',     # iShares U.S. Real Estate ETF

        # Gold (physical or ETF)
        'GLD',     # SPDR Gold Shares
        'IAU',     # iShares Gold Trust
    ],
    "file_prefix": "rayBroad"
}

if __name__ == "__main__":
    run_momda(**CONFIG)
# inconsequential comment here. 