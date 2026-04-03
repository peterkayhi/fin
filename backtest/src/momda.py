""" 
momda - forked papaSrc and added moving day average logic

big difference from Papa bear:

adds "money market" ticker for each bucket (e.g. 3 money markets) to whatever the ticker list is
in momentum computation pick the top three that are also above their MDA
cash accounts are always considered above their mda so worst case (i.e. all tickers are below their mda) all money will go to cash for that month

"""

import pandas as pd
import yfinance as yf
import numpy as np
import logging
from datetime import datetime
from pathlib import Path
from yfcache import yfcache

# =============================================================================
# CONFIGURATION - change these as needed
# =============================================================================
# Official Papa Bear 13 ETFs (straight from muscularportfolios.com)

def run_momda (
    tickers_param: list[str] = [
        'VTV', 'VUG', 'VIOV', 'VIOG', 'VEA', 'VWO', 'VNQ',
        'PDBC', 'IAU', 'EDV', 'VGIT', 'VCLT', 'BNDX'
    ],
    mom_days: list[int] = [63, 126, 252], # trading days to average over
    start_date: str  = (datetime.today() - pd.Timedelta(weeks=532)).strftime("%Y-%m-%d"),# approx 10 yrs and 3 months go
    end_date: str    = (pd.Timestamp.today() - pd.offsets.BMonthEnd(1)).strftime("%Y-%m-%d"), # previous month business end date
    top_assets: int = 3, # how many top assets to balance?
    value_start: float = 100_000, # starting porfolio value
    rebalance_trigger: float  = .2, # what's the biggest bucket delta we'll take before we rebalance
    mda_param: int = 0, # non-zero values kick in the days to compute moving average
    cash_etf: str = 'BIL',
    file_prefix: str  = "momda",  # suffix gets appended to each csv file
    verbose: bool = False, # outputs lots of csv files along the way
    skip_cache: bool = False, # use the cache to minimize Yahoo api calls
    output_dir_param: str  = "/Users/peterkay/Downloads/backtestFiles" # directory holding csvs
) -> None:

    # set/verify directory
    output_dir = Path(output_dir_param)
    output_dir.mkdir(exist_ok=True)

    # Setup logging
    logger = logging.getLogger(file_prefix)
    logdir = Path("/Users/peterkay/Downloads/logFiles")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        for handler in [logging.FileHandler(f"{logdir}/{file_prefix}.app.log", mode='a', encoding='utf-8'), logging.StreamHandler()]:
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    logger.info(f"Starting MOMDA backtest with tickers: {tickers_param}, MDA lookback: {mda_param} days, top assets: {top_assets}, from {start_date} to {end_date}")

    # simple output to csv function
    def save_csv(
        df: pd.DataFrame, # holds what we'll output
        file_name: str = "csvFile",
        dir: str = output_dir, # pathname
        pref: str = file_prefix # prefix - typicaly the project name
        ) -> None:
        df.to_csv(f"{dir}/{pref}{file_name}.csv")


    # fix top_assets in case we have less tickers than buckets to hold
    top_assets = min(top_assets, len(tickers_param))
    # =============================================================================
    # DOWNLOAD WITH MOMENTUM LOOKBACK BUFFER
    # =============================================================================
    # Fix for the bug you spotted: pull extra history so 12-month momentum works from day 1
    download_start = (pd.to_datetime(start_date) - pd.DateOffset(months=15)).strftime("%Y-%m-%d")
    cache = yfcache() # get our Yahoo data via caching - reduces api call guilt
    dataget = cache.get( tickers_param, download_start, end_date, skip_cache)
    data = dataget.final_df
    logger.info(f"DL tickers missed: {dataget.missed_tickers}, Needed starts: {dataget.needed_starts}")

    csvFileName = "yfdata"
    if verbose: save_csv(data,"yfdata")

    if mda_param > 0:  # if we're doing mda we'll need cash accounts
        if skip_cache:
            cash_df = yf.download(
                cash_etf,
                start=download_start,
                end=end_date,
                auto_adjust=True,      # fully adjusted OHLC (Close column is perfect)
                progress=False
            )["Close"]
        else:
            cashget = cache.get( [cash_etf], download_start, end_date)
            cash_df = cashget.final_df
            logger.info(f"Cash Missed tickers: {cashget.missed_tickers}, Needed starts: {cashget.needed_starts}")

        cash_cols = [f"{cash_etf}{i}" for i in range(1, top_assets + 1)] # labels for cash df
        data = data.assign(**{col: cash_df for col in cash_cols}) # add a cash account ticker for each bucket (top_assets) to the portfolio - this way under the worst month cash will show up as the best momentum performer.
        mda = data.rolling(mda_param).mean() # store moving day average in another dataframt
        above_mda = data > mda # and yet another mask to hold true if closing price is above mda
        above_mda[cash_cols] = True #force the cash accounts to be above closing price so that we know that at least those accounts will get selected.
    else: # we're not doing mda
        above_mda = data != None # set the mask to all true so nothing gets affected
    above_mda = above_mda.resample("BME").last().dropna(how="all") # we'll only need the month-end mask
    pass
    

    # =============================================================================
    # Resample to month-end last trading day
    # .resample("ME") groups by month-end, .last() takes the last price of that month.
    # .dropna(how="all") removes rows where all tickers are NaN (e.g., if month-end was a holiday).
    # .resample is an intermediate step to ensure we have clean month-end data for momentum calculations and rebalancing - you have to add an aggregation method like .last() to get a single price per month, and dropna to handle any missing data at month-ends. how="all" means it will only drop rows where all columns are NaN, which is what we want in case some tickers have missing data but others don't.
    # use BME instead of ME to get the last business day of the month, which is more accurate for trading purposes since the actual month-end might be a weekend or holiday. BME stands for Business Month End.
    monthly_prices = data.resample("BME").last().dropna(how="all")

    if verbose: save_csv(monthly_prices,"monthPrices")

    # monthly_prices.to_csv("/Users/peterkay/Downloads/backtestFiles/papa_bear_monthly_prices.csv") # save monthly data for debugging

    # =============================================================================
    # PAPA BEAR MOMENTUM: average of 3/6/12-month returns
    # .pct_change(periods=3) gives 3-month return, etc. Returns are in decimal (e.g., 0.05 for 5%).
    # .pct_change() looks back by N rows as defined by periods, so it calculates the return from that point to the current row.
    # we need to lookback 63 days vs. 3 months so we'll look through the data df and then resample to business month end after calculating the pct change. This way we ensure that the momentum is calculated based on the actual daily data, and then we can align it with the month-end dates for our strategy.

    # to calculate 62 days back we need data.pct_change(periods=62) 
    # =============================================================================    
    # Calculate momentum for each period and average them
    mom_list = [data.pct_change(periods=d).resample("BME").last() for d in mom_days]
    avg_momentum = sum(mom_list) / len(mom_days)

    # =============================================================================
    # PORTFOLIO: top 3 equal-weighted each month (rebalance on month-end)
    # We create a weights DataFrame initialized to 0.0, then loop through each month starting from the second row (since the first month has no prior momentum). For each month, we look at the previous month's momentum, find the top 3 tickers that are also above their mda (if that feature is turned on and store tickers and values for later use 
    # the .iloc[i-1] gets the previous month's momentum, .nlargest(3) finds the top 3 tickers, 
    # =============================================================================


    ticker_cols = [f"t{i}" for i in range(1, top_assets + 1)] # build list of ticker cols e.g. t1, t2, t3 etc
    price_cols = [f"p{i}" for i in range(1, top_assets + 1)]  # list of price cols e.g. p1, p2, p3, etc

    # create empty dataframes to handle top tickers and their prices

    top_ticks = pd.DataFrame(index=avg_momentum.index, columns=ticker_cols)
    top_close = pd.DataFrame(index=avg_momentum.index, columns=price_cols)


    for i in range(1, len(avg_momentum) + 1 ):  # doing +1 so we populate the last row
        # For each month starting from the second row (i=1), we look back at the previous month's momentum to determine the top tickers. 
        #
        # the len() function in panda returns the number of rows in avg_momentum, so the loop iterates through each month starting from the second one (since the first month has no prior momentum data). Inside the loop, we use .iloc[i-1] to access the previous month's momentum data, find the top 3 tickers with .nlargest(3), and then store the values for later use 

        prev_mom = avg_momentum.iloc[i-1] # Get the previous month's momentum
        mda_mask = above_mda.loc[prev_mom.name] # and that month's mda mask
        ticks = prev_mom[mda_mask].nlargest(top_assets) # get our top tickers that are above the mda in a series that includes the date
        top_ticks.loc[ticks.name, ticker_cols] = ticks.index # save those tickers
        prices = monthly_prices.loc[ticks.name][ticks.index] # then lookup the prices for the date [ticks.name] for the tickers [ticks.index]
        top_close.loc[ticks.name, price_cols] = prices.values # and store those prices in their corresponding puka 
        pass # end of loop

    # trim out dates before the actual start date
    top_close = top_close[top_close.index > pd.to_datetime(start_date)]
    # save 'em
    if verbose: save_csv(top_ticks,"topTics")
    if verbose: save_csv(top_close,"topClose")


    #===========
    # calculate holdings

    # create value and shares tables
    val_cols = [f"v{i}" for i in range(1, top_assets + 1)]  # value of shares e.g. v1, v2, v3
    share_cols = [f"s{i}" for i in range(1, top_assets + 1)]  # shares held  e.g. s1, s2, s3

    value = pd.DataFrame(index=avg_momentum.index, columns=val_cols)
    shares = pd.DataFrame(index=avg_momentum.index, columns=share_cols)

    # build df of any month where tickers have changed 
    # 

    any_changes = top_ticks.ne(top_ticks.shift(1)).any(axis=1)

    if verbose: save_csv(any_changes,"anyChanges")

    for month, adj_close in top_close.iterrows(): # iterate through top_close df, month holds the index (the date and row holds the series (row) of top_close
        if top_close.index.get_loc(month) == 0 : # are we on 1st row
            value.loc[month] = value_start / top_assets # divide up value by number of buckets
        else:  # all but the 1st row 
            # this monthy's value is last months shares * this month's price of last month's ticker adj close of those shares
            value.loc[month] = shares.shift(1).loc[month].values * monthly_prices.loc[month][top_ticks.shift(1).loc[month]].values  # top_ticks.shift1 gets last month's tickers which then looks up this month's prices in monthly_prices  
        if any_changes[month] | (top_close.index.get_loc(month) == 0) : 
            # any changed tickers this month or are we on the 1st month?
            # yes, sell what we have and buy the new ones on this month's ticker's closing price
            shares.loc[month, share_cols] = value.loc[month].values / adj_close.values
            logger.info(f"Changed tickers on {month.strftime('%Y-%m-%d')}") # log it
        else: 
            shares.loc[month] = shares.shift(1).loc[month] # no changes - just carry forward the shares we have
        pass
        if (value.loc[month].max() - value.loc[month].min()) / value.loc[month].min() >= rebalance_trigger:  # we hit the rebalance trigger
            logger.info(f"Rebalancing on {month.strftime('%Y-%m-%d')}") # log that we rebalanced this month
            value.loc[month] = value.loc[month].sum() / top_assets # evenly distribute value
            shares.loc[month, share_cols] = value.loc[month].values / adj_close.values # and buy the shares back

    if verbose: save_csv(value,"value")
    if verbose: save_csv(shares,"shares")

    #==========================
    # create portfolio list for the last day 

    day_list = [data.pct_change(periods=d).resample("B").last() for d in mom_days] # get daily momentum buckets
    avg_daily_momentum = sum(day_list) / len(mom_days) # and average them
    last_day = pd.to_datetime(avg_daily_momentum.iloc[-1].name) # save the last date for later use
    today_momentum = (avg_daily_momentum.loc[last_day] * 100).round(2) # extract last day's momentum and convert to pcnt
    today_prices = data.loc[pd.to_datetime(today_momentum.name)][today_momentum.index] # get today's closing prices
    today_report = pd.DataFrame({ # combine the two into 1 report dataframe
        'Avg Momentum': today_momentum,      
        'Adj Close': today_prices    
    })
    today_report = today_report.sort_values('Avg Momentum', ascending=False) # sort by momentum
    # format the columns
    today_report["Adj Close"] = today_report["Adj Close"].map("${:,.2f}".format)
    today_report["Avg Momentum"] = today_report["Avg Momentum"].map("{:.2f}%".format)
    # twist it around to get more of a report look 

    pass
    
    # today_report = avg_daily_momentum.reset_index().melt(id_vars='Date', var_name='Ticker', value_name='Avg').set_index('Date')
    # today_report = today_report.loc[pd.to_datetime(data.iloc[-1].name)].sort_values('Avg', ascending=False)  # last day only
    #===========
    # finalize data for copy/paste friendly format

    copy_paste = value.sum(axis=1) # we just care about the monthly total portfolio value
    copy_paste = value[value.index > pd.to_datetime(start_date)].sum(axis=1) # only total portfolio value from the original start date specified

    #copy_paste = copy_paste.rename(columns={copy_paste.columns[0]: f"{file_prefix}"})
    copy_paste.rename(f"{file_prefix}", inplace=True)
    csvFileName = "CopyPaste"
    logger.info(f"Exporting Values to {file_prefix}{csvFileName}.csv")
    save_csv(copy_paste,"CopyPaste")

    logger.info(f"Asset Mix on {last_day.strftime('%m/%d/%Y')} Closing:")
    logger.info(today_report)
    
    logger.info("Done!")