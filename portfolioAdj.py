import pandas as pd
import yfinance as yf
from datetime import datetime

def generate_etf_adjusted_closes_csv(
    start_date: str,          # '2007-01-01'
    end_date: str = None,     # '2007-12-31' or None for latest
    tickers: list[str] = None,
    output_file: str = "etf_adjusted_closes.csv"
) -> pd.DataFrame:
    """
    Fetches adjusted closing prices for the given tickers and date range,
    formats them exactly like your sample CSV:
      - Date in M/D/YYYY format (no leading zeros)
      - Columns = tickers in the order provided
      - Values = raw adjusted close prices, rounded to 2 decimals
    
    No normalization — just the real prices from Yahoo Finance.
    """
    if not tickers:
        raise ValueError("Please provide a list of tickers")
    
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date) if end_date else None
    
    print(f"Downloading adjusted close prices for {tickers} from {start_date} to {end_date or 'latest'}...")
    
    # Download all at once — very efficient
    df = yf.download(
        tickers,
        start=start,
        end=end,
        progress=True,
        auto_adjust=True,      # This gives us the adjusted close directly
        actions=False
    )
    
    # Extract just the Adjusted Close (it's renamed to 'Close' when auto_adjust=True)
    if len(tickers) == 1:
        prices = df['Close'].to_frame(name=tickers[0])
    else:
        prices = df['Close']
    
    # Keep only the columns in the exact order you passed
    prices = prices[tickers]
    
    # Drop fully empty rows (edge cases)
    prices = prices.dropna(how='all')
    
    if prices.empty:
        raise ValueError("No price data returned — check dates and tickers?")
    
    # Round to 2 decimal places (matches your sample)
    prices = prices.round(2)
    
    # Format date index like your sample: 1/3/2007 instead of 2007-01-03
    prices.index = prices.index.strftime('%-m/%-d/%Y')
    prices.index.name = 'Date'
    
    # Save and return
    prices.to_csv(output_file)
    print(f"CSV saved to: {output_file}")
    
    return prices


# ────────────────────────────────────────────────
# Example usage — should closely match your sample period
# ────────────────────────────────────────────────

if __name__ == "__main__":
    tickers = ["VTV", "VUG", "VIOV", "VIOG", "VEA", "VWO", "VNQ", "PDBC", "IAU", "EDV", "VGIT", "VCLT", "BNDX"]
    
    result = generate_etf_adjusted_closes_csv(
        start_date='2025-01-01',
        end_date='2026-03-03',           # a bit extra to catch the dates in your sample
        tickers=tickers,
        output_file = "/Users/peterkay/Downloads/etfmar-2026.csv"
    )
    
    print("\nFirst few rows of the result:")
    print(result.head(10))