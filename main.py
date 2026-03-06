import yfinance as yf
# import pandas as pd


print("version", yf.__version__)  # Should print something like 1.2.0 or whatever current is
spy = yf.Ticker("SPY")
print(spy.history(period="1d"))  # Quick check it pulls data
