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


def find_extrema(s, bw='cv_ls'):
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
        var_type='c', bw=[2.5]
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


def find_patterns(extrema, max_bars=100):
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
    

    # Using the notation from the paper to avoid mistakes
    e1 = window.iloc[0]
    e2 = window.iloc[1]
    e3 = window.iloc[2]
    e4 = window.iloc[3]
    e5 = window.iloc[4]
    



    rtop_g1 = np.mean([e1, e3, e5])
    rtop_g2 = np.mean([e2, e4])
    

    
    # Buy Failed Swing
    if (e5 < e4) and (e5 > e3) and (e3 <= (e2*0.95)):
        patterns['Buy Failed Swing'].append((window.index[1], window.index[4]))
    
    # Buy Double Bottom
    if (e5 < e4) and (e5 >= (e3*0.98)) and (e5 <= e3) and (e3 <= (e2*0.90)):
        patterns['Buy Double Bottom'].append((window.index[1], window.index[4]))

    # Buy Non-Failure Swing
    if (e5 < e4) and (e4 > e2) and (e3 < e1) and (e5 > e1):
        patterns['Buy Non-Failure Swing'].append((window.index[0], window.index[4]))
    
    # Buy Rising Channel
    if (e5 > e3) and (e3 > e1) and (e4 > e2):
        patterns['Buy Rising Channel'].append((window.index[0], window.index[4]))

    # Buy Horizondal Channel
    if (e5 > e3) and (e3 > e1) and (e4 > e2):
        patterns['Buy Rising Channel'].append((window.index[0], window.index[4]))

    # Head and Shoulders
    if (e1 > e2) and (e3 > e1) and (e3 > e5) and \
            (abs(e1 - e5) <= 0.1*np.mean([e1, e5])) and \
            (abs(e2 - e4) <= 0.1*np.mean([e1, e5])):
        patterns['HS'].append((window.index[0], window.index[4]))

    # Inverse Head and Shoulders
    elif (e1 < e2) and (e3 < e1) and (e3 < e5) and \
            (abs(e1 - e5) <= 0.01*np.mean([e1, e5])) and \
            (abs(e2 - e4) <= 0.01*np.mean([e1, e5])):
        patterns['IHS'].append((window.index[0], window.index[4]))

    # Broadening Top
    elif (e1 > e2) and (e1 < e3) and (e3 < e5) and (e2 > e4):
        patterns['BTOP'].append((window.index[0], window.index[4]))

    # Broadening Bottom
    elif (e1 < e2) and (e1 > e3) and (e3 > e5) and (e2 < e4):
        patterns['BBOT'].append((window.index[0], window.index[4]))

    # Triangle Top
    elif (e1 > e2) and (e1 > e3) and (e3 > e5) and (e2 < e4):
        patterns['TTOP'].append((window.index[0], window.index[4]))

    # Triangle Bottom
    elif (e1 < e2) and (e1 < e3) and (e3 < e5) and (e2 > e4):
        patterns['TBOT'].append((window.index[0], window.index[4]))

    # Rectangle Top
    elif (e1 > e2) and \
            (abs(e1-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e3-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e5-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e2-rtop_g2)/rtop_g2 < 0.0075) and \
            (abs(e4-rtop_g2)/rtop_g2 < 0.0075) and \
            (min(e1, e3, e5) > max(e2, e4)):

        patterns['RTOP'].append((window.index[0], window.index[4]))

    # Rectangle Bottom
    elif (e1 < e2) and \
            (abs(e1-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e3-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e5-rtop_g1)/rtop_g1 < 0.0075) and \
            (abs(e2-rtop_g2)/rtop_g2 < 0.0075) and \
            (abs(e4-rtop_g2)/rtop_g2 < 0.0075) and \
            (max(e1, e3, e5) > min(e2, e4)):

        patterns['RBOT'].append((window.index[0], window.index[4]))
                # Buy Cup and Handle
    elif (e5 < e4) and (e5 > e3) and (e1 > e5) and (e1 > e3) and (e1 >= e4):
        patterns['Buy Cup and Handle'].append((window.index[0], window.index[4]))

    return patterns


def plot_window(prices, extrema, ax=None):
    # def plot_window(prices, extrema, smooth_prices, smooth_extrema, df, ax=None):
    """
    Input: data from find_extrema
    Output: plots window for actual and smoothed prices and extrema
    """
    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111)
    
    window = extrema.tail(5)

    # # A pattern must play out within max_bars (default 35)
    # if ((window.index[4] - window.index[1]) > (max_bars * 1.2)):
    #     continue
    
    # if (window.index[-1] - window.index[0]) > max_bars:
    #     continue

    # Using the notation from the paper to avoid mistakes
    
    x1 = np.array([[window.index[1], window.index[3]]])
    y1 = np.array([[window.iloc[1], window.iloc[3]]])
    slope1, intercept1, r_value1, p_value1, std_err1 = linregress(x1, y1)
    
    lastIndex = prices.tail(1).index[-1]

    trend_line_price = slope1 * lastIndex + intercept1 


    x2 = np.array([window.index[1], window.index[3], lastIndex ])
    y2 = np.array([window.iloc[1], window.iloc[3], trend_line_price])


    x3 = np.array([[window.index[2], window.index[4]]])
    y3 = np.array([[window.iloc[2], window.iloc[4]]])
    slope2, intercept2, r_value2, p_value2, std_err2 = linregress(x3, y3)

    trend_line_price2 = slope2 * lastIndex + intercept2 


    x4 = np.array([window.index[2], window.index[4], lastIndex ])
    y4 = np.array([window.iloc[2], window.iloc[4], trend_line_price2])
    # plt.plot(a1, b1, 'r', label='price line')
    # # plt.plot(a1, intercept1 + slope1*a1, 'r', label='price line')


    # x2 = np.array([240,252, 271])
    # y2 = np.array([1106.5, 1012.8, 864.44166667])

    prices.plot(ax=ax, color="green")
    # prices.plot(ax, b1, figsize='r', label='price line')
    plt.plot(x2, y2, 'r', label='price line')
    plt.plot(x4, y4, 'r', label='price line')
    ax.scatter(extrema.index, extrema.values, color="red")
    plt.title(stockName + "\n " + "Net Reversal Score : ")

    # ax.text(
    #     0,
    #     1100,
    #     "Preceding trend score : "
    #     + str(precedingtrendscore)
    #     + "\n"
    #     + "Trend Level Score : "
    #     + str(trendLevelScore)
    #     + "\n"
    #     + "Over Extension Score: "
    #     + str(overExtensionScore)
    #     + "\n"
    #     + "Retracement Level Score : "
    #     + str(retracementLevelScore)
    #     + "\n"
    #     + "Volume Open Interest Score : "
    #     + str(volumeOpenInterstScore)
    #     + "\n"
    #     + "Selling Climax Score : "
    #     + str(sellingClimaxScore)
    #     + "\n"
    #     + "cdlMaxScore : "
    #     + str(cdlMaxScore)
    #     + "\n"
    #     + "cdlTotalScore : "
    #     + str(cdlTotalScore),
    #     horizontalalignment="left",
    #     verticalalignment="top",
    #     fontsize=10,
    #         )

    # smooth_prices.plot(ax=ax, color='pink')
    # ax.scatter(smooth_extrema.index, smooth_extrema.values, color='orange')
    # plt.figure(figsize=(30, 20))
    # plt.get_current_fig_manager().window.state("zoomed")
   

    plt.show()

def count(list1, l, r):
	c = 0
	# traverse in the list1
	for x in list1:
		# condition check
		if x > float(l) and x < float(r):
			c+= 1	
	return c

# Main function is called by default, and the first function to be executed
if __name__ == "__main__":    
    
    # Read the system configration file that contains logs informations, and telegram ids
    configFileH = open('conf/config.ini')
    configList = configFileH.readlines()
    configFileH.close()
    configDict = {}
    
    # Store the configuraiton files in a dictionary for reusablity. 
    for configItem in configList:
        configItem = configItem.strip('\n').strip('\r').split('=')
        if(len(configItem) > 1):            
            configDict[str(configItem[0])] = configItem[1]    
    
    logEnableFlag = True if configDict['ENABLE_LOG_FLAG'] == 'True' else False 
    testingFlag = True if configDict['TESTING_FLAG'] == 'True' else False
    
    programName = configDict['LIVE_TRADES_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['LIVE_TRADES_PGM_NAME']) + '.log')

    programExitFlag = 'N'    

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)
    
    adminTradeAccount = configDict['ADMIN_TRADE_ACCOUNT']
    
    currDate = util.get_date_time_formatted("%Y-%m-%d")

    # Connect to Kite ST
    kite, isKiteConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = configDict['ADMIN_TRADE_BROKER'])    

    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if (isKiteConnected):           
       
        alertMsg = 'The program (' + programName.replace('_','\_') + ')  started at ' + str(util.get_date_time_formatted("%d-%m-%Y %H:%M:%S"))
        
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)
        
        try:
            instList = util.get_test_inst_list(mySQLCursor)
   
            for instRow in instList:
                # stockName = "BANK NIFTY"
                # tradeSymbol = "BANK NIFTY"
                instrumentToken = instRow[0]
                tradeSymbol = instRow[1]
                stockName = instRow[2]
                print("-----------------------")
                print(tradeSymbol)
                print("-----------------------")                
                            
                fromDate = util.get_lookup_date(400)
                toDate = util.get_lookup_date(0)
                instrumentToken = instrumentToken
                
            
                # Verify whether the connection to MySQL database is open
                histRecords = baf.get_historical_data(kite, instrumentToken, fromDate, toDate, 'day')
    
                df = pd.DataFrame(histRecords)
                extrema, prices, smooth_extrema, smooth_prices=find_extrema(df, bw='cv_ls')
                
                df1 = extrema.to_frame()
                df2 = prices.to_frame()
                df2.set_index('close', inplace=False)
                lastIndex = df2.index[-1]
                # df2['index'].tail(1).values[0]
            
                # df1 = df1.reset_index()
                # df1.drop('index', inplace=True, axis=1)
                # df1.set_index('close', inplace=False)
            
                # df2 = pd.DataFrame({'close': [extrema]})
                df1['price_level'] = df1['close']
                df1['lower_band']  =  df1['close'] * 0.98
                df1['upper_band']  =  df1['close'] * 1.02
                df1["rank"] = df1["price_level"].rank()
                df1['RSI'] = talib.RSI(df2['close'], timeperiod=14)
                
                df1 = df1.reset_index()

                for index, row in df1.iterrows():               
                    df1.loc[index, 'frequency'] = count(df1['price_level'], row['lower_band'],  row['upper_band'])-1
                    df1.loc[index, 'recent_price_level'] = df1['price_level'][df1.index[-1]]
                    df1.loc[index, 'current_rank'] = df1['rank'][df1.index[-1]]
                    df1.loc[index, 'last_price'] = df2['close'][df2.index[-1]]
                    
                
                df1["distance"]= (df1['price_level']/ df1['price_level'][df1.index[-1]]-1)*100
                df1['avg_distance_res'] = (df1[(df1['rank']> df1['current_rank']) & (df1['rank'] <= (df1['current_rank']+5))])['distance'].mean()
                df1['max_distance_res'] = (df1[(df1['rank']> df1['current_rank'])])['distance'].max()
                df1['avg_distance_sub'] = (df1[(df1['rank']< df1['current_rank']) & (df1['rank'] >= (df1['current_rank']-5))])['distance'].mean()
                df1['max_distance_sub'] = (df1[(df1['rank']< df1['current_rank'])])['distance'].min()
                df1['linear_slope_price'] = talib.LINEARREG_ANGLE(df1['price_level'], timeperiod=3)
                df1['linear_slope_rsi'] = talib.LINEARREG_ANGLE(df1['RSI'], timeperiod=3)
                print(df1['linear_slope_price'].tail(2))
                print(df1['linear_slope_rsi'].tail(2))
                lastRec = df1.tail(1)            
                last2Rec = df1.tail(3).head(1)


                x1 = np.array([[last2Rec['index'].values[0], lastRec['index'].values[0]]])
                y1 = np.array([[last2Rec['price_level'].values[0], lastRec['price_level'].values[0]]])
                y2 = np.array([[last2Rec['RSI'].values[0], lastRec['RSI'].values[0]]])

                slope1, intercept1, r_value1, p_value1, std_err1 = linregress(x1, y1)
                df1.loc[df1.index[-1], 'price_slope'] = slope1

                df1.loc[df1.index[-1], 'trend_line_price'] = slope1 * lastIndex + intercept1

                slope1, intercept1, r_value1, p_value1, std_err1 = linregress(x1, y2)
                df1.loc[df1.index[-1], 'rsi_slope'] = slope1
                
                # Divergence = ""
                # if (df1['linear_slope_price']  < 0 and df1['linear_slope_rsi'] > 0) :
                #     Divergence = "Strong Bullish Divergence 1"             
                # elif (df1['linear_slope_price']  < 50 and df1['linear_slope_rsi'] > 70) :
                #     Divergence = "Weak Bullish Divergence 2" 
                # elif (df1['linear_slope_price']  < - 70 and df1['linear_slope_rsi'] > -50) :
                #     Divergence = "Weak Bullish Divergence 3" 
                
                # print(Divergence)

                # print("slope1: %f, intercept1: %f" % (slope1, intercept1))
                # print("R-squared1: %f" % r_value1**2)


                df1.to_csv("test.csv")            

                patterns=find_patterns(extrema, max_bars=100)
                print(patterns)
                for name, pattern_periods in patterns.items():
                    print(name)
                    print("-----------------------")
                    for pattern in pattern_periods:
                        print(str(df.loc[pattern[0], 'date']) + ", " + str(df.loc[pattern[1], 'date']))
                    print("-----------------------")                    
                        
                plot_window(prices, extrema, ax=None)

                # plot_window(prices, extrema, smooth_prices, smooth_extrema, df, ax=None)
            



            
        except Exception as e:
            alertMsg = 'Live trade service failed (main block): '+ str(e)
            print(alertMsg)

    util.logger(logEnableFlag, 'info', "Program ended") 

