from collections import defaultdict
from itertools import count
import matplotlib.pyplot as plt

import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
from statsmodels.nonparametric.kernel_regression import KernelReg
from utils import util_functions as util
from utils import broker_api_functions as baf
import re
import talib
from scipy.stats import linregress


def get_ti_for_divergence(prices,cl):
    prices['RSI'] = talib.RSI(cl, timeperiod=14)
    return prices


def find_extrema(s, inputParams, bw='cv_ls'):
    """
    Input:
        s: prices as pd.series
        bw: bandwith as str or array like
    Returns:
        prices: with 0-based index as pd.series
        extrema: extrema of prices as pd.series
        smoothed_prices: smoothed prices using kernel regression as pd.series
        smoothed_extrema: extrema of smoothed_prices as pd.series
    """
    # Copy series so we can replace index and perform non-parametric
    # kernel regression.
    # prices = s.copy()
    prices = s[['date', 'close']].copy()
    prices = prices.reset_index()
    # prices = prices.reset_index(drop=True)
    # prices.set_index('date')
    # prices.set_index(["date"], inplace = True, append = True, drop = True)    
    # prices.columns = ['date','open','high', 'low','close','volume']
    prices.drop('index', inplace=True, axis=1)
    prices.set_index('date', inplace=False)
    prices.columns = ['date','close']
    
    

    # prices.columns = ['date','open','high', 'low','close','volume']
    # prices.columns = ['date','close']
    

    prices = prices['close']

    kr = KernelReg(
        [prices.values],
        [prices.index.to_numpy()],
        var_type='c', bw=[float(inputParams['revBWValue'])]
    )
    f = kr.fit([prices.index])

    # Use smoothed prices to determine local minima and maxima
    smooth_prices = pd.Series(data=f[0], index=prices.index)
    smooth_local_max = argrelextrema(smooth_prices.values, np.greater)[0]
    smooth_local_min = argrelextrema(smooth_prices.values, np.less)[0]
    local_max_min = np.sort(
        np.concatenate([smooth_local_max, smooth_local_min]))
    smooth_extrema = smooth_prices.loc[local_max_min]

    # Iterate over extrema arrays returning datetime of passed
    # prices array. Uses idxmax and idxmin to window for local extrema.
    price_local_max_dt = []
    for i in smooth_local_max:
        if (i > 1) and (i < len(prices)-1):
            price_local_max_dt.append(prices.iloc[i-2:i+2].idxmax())

    price_local_min_dt = []
    for i in smooth_local_min:
        if (i > 1) and (i < len(prices)-1):
            price_local_min_dt.append(prices.iloc[i-2:i+2].idxmin())

    maxima = pd.Series(prices.loc[price_local_max_dt])
    # print("maxima :" + str(maxima))
    minima = pd.Series(prices.loc[price_local_min_dt])
    # print("minima :" + str(minima))

    extrema = pd.concat([maxima, minima]).sort_index()

    # Return series for each with bar as index
    return extrema, prices, smooth_extrema, smooth_prices


def find_patterns(extrema, prices, max_bars=100):
    currentPrice = prices.tail(1).values[0]
    lastIndex = prices.tail(1).index[-1]

    """
    Input:
        extrema: extrema as pd.series with bar number as index
        max_bars: max bars for pattern to play out
    Returns:
        patterns: patterns as a defaultdict list of tuples
        containing the start and end bar of the pattern
    """
    patterns = defaultdict(list)
    
    # Need to start at five extrema for pattern generation

    window = extrema.tail(5)
    # print(window)
    # # A pattern must play out within max_bars (default 35)
    # if ((window.index[4] - window.index[1]) > (max_bars * 1.2)):
    #     continue
    
    # if (window.index[-1] - window.index[0]) > max_bars:
    #     continue

    # Using the notation from the paper to avoid mistakes
    e1 = window.iloc[0]
    e2 = window.iloc[1]
    e3 = window.iloc[2]
    e4 = window.iloc[3]
    e5 = window.iloc[4]
    
    

    rtop_g1 = np.mean([e1, e3, e5])
    rtop_g2 = np.mean([e2, e4])
    
    x1 = np.array([[window.index[1], window.index[3]]])
    y1 = np.array([[window.iloc[1], window.iloc[3]]])
    slope1, intercept1, r_value1, p_value1, std_err1 = linregress(x1, y1)
    
    trend_line_price = slope1 * lastIndex + intercept1 
    # a1 = np.array([[window.index[1], window.index[3], lastIndex ]])
    # b1 = np.array([[window.iloc[1], window.iloc[3], trend_line_price]])
    # patterns['trend_line_price'] = trend_line_price 
    # print(x1) 
    # print(y1)
    # print(trend_line_price)
    # print(currentPrice)
    # print(lastIndex)
    # Buy Failed Swing
    stopLossPct = round(((abs((e5 / currentPrice) -1) * 100) * -1), 2)
    if (e5 < e4):
        patterns['Buy'].append((window.index[1], window.index[4]))

    elif (e5 > e4):
        patterns['Sell'].append((window.index[1], window.index[4]))   


    if (e5 < e4) and (e5 > e3) and (e3 <= (e2*0.95)) and (currentPrice > (trend_line_price * 1.01))  :
        patterns['Buy Failed Swing'].append((window.index[1], window.index[4]))
    
    # Buy Double Bottom
    if (e5 < e4) and (e5 >= (e3*0.98)) and (e5 <= e3) and (e3 <= (e2*0.90)) and (currentPrice > (trend_line_price * 1.01)) :
        patterns['Buy Double Bottom'].append((window.index[1], window.index[4]))

    # Buy Non-Failure Swing
    if (e5 < e4) and (e4 > e2) and (e3 < e1) and (e5 > e1) and (currentPrice > (trend_line_price*1.01)) :
        patterns['Buy Non-Failure Swing'].append((window.index[0], window.index[4]))

    
    # Buy Rising Channel
    if (e5 > e3) and (e3 > e1) and (e4 > e2) and (currentPrice > (trend_line_price *1.01)) :
        patterns['Buy Rising Channel'].append((window.index[0], window.index[4]))

    # Buy Horizondal Channel
    if (e5 > e3) and (e3 > e1) and (e4 > e2) and (currentPrice > (trend_line_price *1.01)):
        patterns['Buy Horizondal Channel'].append((window.index[0], window.index[4]))

    # Head and Shoulders
    if (e1 > e2) and (e3 > e1) and (e3 > e5) and \
            (abs(e1 - e5) <= 0.1*np.mean([e1, e5])) and \
            (abs(e2 - e4) <= 0.1*np.mean([e1, e5])):
        patterns['HS'].append((window.index[0], window.index[4]))

    # Inverse Head and Shoulders
    if (e1 < e2) and (e3 < e1) and (e3 < e5) and \
            (abs(e1 - e5) <= 0.01*np.mean([e1, e5])) and \
            (abs(e2 - e4) <= 0.01*np.mean([e1, e5])):
        patterns['IHS'].append((window.index[0], window.index[4]))

    # Broadening Top
    if (e1 > e2) and (e1 < e3) and (e3 < e5) and (e2 > e4):
        patterns['BTOP'].append((window.index[0], window.index[4]))

    # Broadening Bottom
    if (e1 < e2) and (e1 > e3) and (e3 > e5) and (e2 < e4):
        patterns['BBOT'].append((window.index[0], window.index[4]))

    # Triangle Top
    if (e1 > e2) and (e1 > e3) and (e3 > e5) and (e2 < e4):
        patterns['TTOP'].append((window.index[0], window.index[4]))

    # Triangle Bottom
    if (e1 < e2) and (e1 < e3) and (e3 < e5) and (e2 > e4):
        patterns['TBOT'].append((window.index[0], window.index[4]))

    # Rectangle Top
    if (e1 > e2) and \
            (abs(e1-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e3-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e5-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e2-rtop_g2)/rtop_g2 < 0.0075) and \
            (abs(e4-rtop_g2)/rtop_g2 < 0.0075) and \
            (min(e1, e3, e5) > max(e2, e4)):

        patterns['RTOP'].append((window.index[0], window.index[4]))

    # Rectangle Bottom
    if (e1 < e2) and \
            (abs(e1-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e3-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e5-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e2-rtop_g2)/rtop_g2 < 0.0075) and \
            (abs(e4-rtop_g2)/rtop_g2 < 0.0075) and \
            (max(e1, e3, e5) > min(e2, e4)):

        patterns['RBOT'].append((window.index[0], window.index[4]))
        
                # Buy Cup and Handle
    if (e5 < e4) and (e5 > e3) and (e1 > e5) and (e1 > e3) and (e1 >= e4):
        patterns['Buy Cup and Handle'].append((window.index[0], window.index[4]))

    return patterns, trend_line_price, stopLossPct
