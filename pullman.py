
# gain_calculator.py
# Simple script to calculate nominal total gain for a symbol (stock/ETF) 
# between two dates using yfinance + pandas

import yfinance as yf
import pandas as pd
from datetime import datetime

def calculate_nominal_total_gain(symbol: str, start_date: str, end_date: str) -> dict:
    """
    Calculate the nominal total gain for a symbol between start_date and end_date.
    
    Returns a dictionary with:
    - start_price
    - end_price
    - total_gain_percentage
    - total_gain_multiplier
    - number_of_trading_days
    - actual_start_date (the first available trading day)
    - actual_end_date   (the last available trading day)
    """
    try:
        # Download historical data - we only really need Adjusted Close
        # auto_adjust=True is the modern default → gives us adjusted prices in 'Close'
        df = yf.download(
            symbol,
            start=start_date,
            end=end_date,
            progress=False,           # cleaner output
            auto_adjust=True,         # important: gives adjusted prices
            actions=False             # we don't need dividends/splits separately here
        )
        
        if df.empty:
            return {"error": f"No data returned for {symbol} between {start_date} and {end_date}"}
        
        # Sort just in case, though yfinance usually returns sorted
        df = df.sort_index()
        
        # Get first and last available trading day in the range
        start_row = df.iloc[0]
        end_row   = df.iloc[-1]
        
#        start_price = start_row['Close']   # this is adjusted when auto_adjust=True
#        end_price   = end_row['Close']

        start_price = start_row['Close'].item()
        end_price   = end_row['Close'].item()
        
        # Calculate gain
        gain_multiplier = end_price / start_price
        gain_pct = (gain_multiplier - 1) * 100
        
        result = {
            "symbol": symbol.upper(),
            "start_date_requested": start_date,
            "end_date_requested": end_date,
            "actual_start_date": df.index[0].strftime("%Y-%m-%d"),
            "actual_end_date": df.index[-1].strftime("%Y-%m-%d"),
            "start_price_adj": round(start_price, 4),
            "end_price_adj": round(end_price, 4),
            "total_gain_percentage": round(gain_pct, 2),
            "total_gain_multiplier": round(gain_multiplier, 4),
            "trading_days": len(df),
            "note": "Using adjusted close prices (accounts for splits)"
        }
        
        return result
    
    except Exception as e:
        return {"error": str(e)}


# ────────────────────────────────────────────────
# Example usage
# ────────────────────────────────────────────────

if __name__ == "__main__":
    
    # Feel free to change these
    symbol    = "SPY"                  # or VTI, QQQ, AAPL, etc.
    startdate     = "2023-01-03"
    enddate       = "2023-02-25"
    
    df = yf.download(
        symbol,
        start=startdate,
        end=enddate,
        progress=False,           # cleaner output
        auto_adjust=True,         # important: gives adjusted prices
        actions=False             # we don't need dividends/splits separately here
    )
        
