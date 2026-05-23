""" 
rayBroadMom.py

digital ray broad asset list of etfs with momentum overlays.

"""
import sys
from pathlib import Path
from datetime import date

from backtest.src.momda import run_momda

# List of Momentum ETFs by Asset Class
momentum_etfs = [
    # U.S. & global large‑cap equity indices (Momentum factor variants)
    'MTUM',    # iShares MSCI USA Momentum Factor ETF
    'SPMO',    # Invesco S&P 500 Momentum ETF

    # International developed‑market equities (Momentum factor variants)
    'IMTM',    # iShares MSCI Intl Momentum Factor ETF
    'IMOM',    # Alpha Architect International Quantitative Momentum ETF

    # Emerging‑market equities (Momentum factor variants)
    'PIE',     # Invesco Dorsey Wright Emerging Markets Momentum ETF
    'UEVM',    # VictoryShares Emerging Markets Value Momentum ETF

    # Dividend‑focused equity funds or high‑dividend ETFs (with Momentum strategy)
    'QDPL',    # Pacer Metaurus US Large Cap Dividend Multiplier: 300 ETF
    'FCDX',    # Focus Morningstar Dividend Momentum ETF

    # Broad industrials & materials sectors (using Multi-Factor/Momentum sector overlays)
    'PTH',     # Invesco DWA Industrials Momentum ETF
    'PRN',     # Invesco DWA Materials Momentum ETF

    # Short‑duration Treasury ETFs (using relative-strength/cross-asset momentum models)
    'FIXD',    # MacKay Shields Yield ETF (Active/Momentum Bond Strategy)
    'FTSD',    # Franklin Liberty Short Duration U.S. Government ETF

    # Long‑duration Treasury ETFs (using Trend/Momentum overlays)
    'TMF',     # Direxion Daily 20+ Year Treasury Bull 3X Shares (Leveraged Momentum)
    'UST',     # ProShares Ultra 7-10 Year Treasury (Leveraged Trend)

    # Investment‑grade corporate bond indices (using Systematic/Momentum credit selection)
    'IGEB',    # iShares Investment Grade Systematic Bond ETF
    'HYGB',    # iShares High Yield Systematic Bond ETF

    # High‑yield (junk) bond indices (using Technical Relative Strength models)
    'PHB',     # Invesco Fundamental High Yield Corporate Bond ETF
    'HYLD',    # High Yield ETF (Actively Managed Trend/Momentum Credit Strategy)

    # Broad commodity index (using optimized Optimum Yield/Momentum roll-strategies)
    'PDBC',    # Invesco Optimum Yield Diversified Commodity Strategy No K-1 ETF
    'COMB',    # GraniteShares Bloomberg Commodity Broad Strategy No K-1 ETF

    # Real‑estate investment trusts (REITs) (using Technical Relative Strength filters)
    'FRI',     # First Trust S&P REIT Index Fund (Factor Optimized)
    'IFGL',    # iShares International Developed Real Estate ETF (Factor Weighted)

    # Gold (physical or ETF) (using momentum tracking/managed futures gold overlays)
    'GOLX',    # CoreValues Gold Trend/Momentum Strategy ETF
    'GLDM',    # SPDR Gold MiniShares Trust (Tactical Liquidity Vehicle for Momentum Traders)
]

CONFIG = {
    "tickers_param": momentum_etfs,
    "file_prefix": "rayBroadMom"
}

if __name__ == "__main__":
    run_momda(**CONFIG)
