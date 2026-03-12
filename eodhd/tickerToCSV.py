"""
tickerToCSV.py

exports historical data for a ticker to a CSV file
"""

import config as cfg
from eodhd import APIClient
import pandas as pd

FROM_DATE = "2016-01-01"
TO_DATE = "2016-01-15"
SYMBOL = 'AAPL.US'
PERIOD = "d"
ORDER = "a"

DATAFILE = "aapl2016.csv"
FOLDER = "/Users/peterkay/Downloads/csv/"

if __name__ == "__main__":
    api = APIClient(cfg.API_KEY)

    # --- EOD historical stock market data ---
    data = api.get_eod_historical_stock_market_data(
        symbol=SYMBOL,
        period=PERIOD,
        from_date=FROM_DATE,
        to_date=TO_DATE,
        order=ORDER,
    )

    df = pd.DataFrame(data)
    df.to_csv(f"{FOLDER}{DATAFILE}", index=False)

print(f"Saved {len(df)} rows to {FOLDER}{DATAFILE}")


