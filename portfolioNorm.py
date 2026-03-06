import pandas as pd
import yfinance as yf
from datetime import datetime

def generate_portfolio_nominal_gains(
    start_date: str,          # '2020-01-01'
    end_date: str,            # '2020-12-31' or None for latest available
    tickers: list[str],       # ['VTV', 'VUG', 'VIOV', 'VEA', 'VWO']
    initial_value: float = 1000.0,
    output_file: str = "portfolios_sample.csv"
) -> pd.DataFrame:
    """
    Creates a CSV very similar to your sample:
    - Index: dates
    - Columns: the ticker symbols
    - Values: nominal portfolio value starting from $1000 for each ETF independently
    
    Uses adjusted close prices (handles dividends & splits).
    """
    # Convert string dates to datetime for cleaner handling
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date) if end_date else None
    
    print(f"Fetching data for {tickers} from {start_date} to {end_date or 'latest'}...")
    
    # Download adjusted close prices for all tickers at once
    df = yf.download(
        tickers,
        start=start,
        end=end,
        progress=True,
        auto_adjust=True,       # gets adjusted close directly
        actions=False
    )
    
    # We only want the Adjusted Close
    if len(tickers) == 1:
        prices = df['Close'].to_frame(name=tickers[0])
    else:
        prices = df['Close']
    
    # Make sure we have the same column order as input tickers
    prices = prices[tickers]
    
    # Drop any rows that are completely NaN (rare, but can happen at edges)
    prices = prices.dropna(how='all')
    
    if prices.empty:
        raise ValueError("No price data returned for the selected period and tickers.")
    
    # Normalize each ticker to start at initial_value on its first available date
    normalized = pd.DataFrame(index=prices.index, columns=tickers, dtype=float)
    
    for ticker in tickers:
        col = prices[ticker]
        
        # Find first valid price (skip leading NaNs)
        first_valid_idx = col.first_valid_index()
        if first_valid_idx is None:
            print(f"Warning: No valid data for {ticker}")
            normalized[ticker] = float('nan')
            continue
            
        first_price = col.loc[first_valid_idx]
        
        # Normalize: value = initial_value × (price / first_price)
        normalized[ticker] = initial_value * (col / first_price)
    
    # Round to 2 decimal places (matches your sample style)
    normalized = normalized.round(2)
    
    # Make date index look like your sample (M/D/YYYY)
    normalized.index = normalized.index.strftime('%-m/%-d/%Y')
    
    # Rename index to match your sample
    normalized.index.name = 'Date'
    
    # Save to CSV
    normalized.to_csv(output_file)
    print(f"Saved to {output_file}")
    
    return normalized


# ────────────────────────────────────────────────
# Example usage
# ────────────────────────────────────────────────

if __name__ == "__main__":
    tickers = ['VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO']
    
    # Same period as your sample
    result = generate_portfolio_nominal_gains(
        start_date='2007-01-01',
        end_date='2007-01-10',           # just a few days to match sample
        tickers=tickers,
        initial_value=1000.0
    )
    
    # Show first few rows
    print("\nFirst few rows of result:")
    print(result.head(8))