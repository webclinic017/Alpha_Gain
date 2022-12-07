from collections import defaultdict
from importlib.resources import path
import matplotlib.pyplot as plt
import csv
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
from statsmodels.nonparametric.kernel_regression import KernelReg
from utils import util_functions as util
from utils import trade_scoring_copy as tsc
import quantstats as qs
from utils import broker_api_functions as baf
from utils import chart_patterns as cp
import datetime
import os
from scipy.stats import linregress

pathadd = "D:\\alphagain_development\\core\\"

# 1. Set up multiple variables to store the titles, text within the report
page_title_text = "Daily Stock Chart Analysis Report"
title_text = "Daily Chart With Reversals"
js = """      
<script type = "text/JavaScript">
         <!--
            function AutoRefresh( t ) {
               setTimeout("location.reload(true);", t);
            }
         //-->
</script>            
"""
# 2. Combine them together using a long f-string
html = f"""
        <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@200&display=swap" rel="stylesheet">
        <head>
            <title>{page_title_text}</title>
        {js}
        </head>
        <body style="background: linear-gradient(to right, #373b44, #4286f4);" onload = "JavaScript:AutoRefresh(5000);">
            <h1 style="color:white; font-family: 'Poppins', sans-serif; text-align:center;">{title_text}</h1> \n 
                    
"""
os.remove("html_report.html")
file_object = open("html_report.html", "a")
file_object.write(html)
file_object.close()


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
    plt.title(stockName + "\n " + "Net Reversal Score : " + str(netScore))

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
    plt.savefig("img/" + tradeSymbol + ".png")

    plt.show()


# Main function is called by default, and the first function to be executed
if __name__ == "__main__":

    # Read the system configration file that contains logs informations, and telegram ids
    configFileH = open("conf/config.ini")
    configList = configFileH.readlines()
    configFileH.close()
    configDict = {}

    # Store the configuraiton files in a dictionary for reusablity.
    for configItem in configList:
        configItem = configItem.strip("\n").strip("\r").split("=")
        if len(configItem) > 1:
            configDict[str(configItem[0])] = configItem[1]

    logEnableFlag = True if configDict["ENABLE_LOG_FLAG"] == "True" else False
    testingFlag = True if configDict["TESTING_FLAG"] == "True" else False

    programName = configDict["LIVE_TRADES_PGM_NAME"]

    # Initialized the log files
    util.initialize_logs(str(configDict["LIVE_TRADES_PGM_NAME"]) + ".log")

    programExitFlag = "N"

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)

    adminTradeAccount = configDict["ADMIN_TRADE_ACCOUNT"]

    currDate = util.get_date_time_formatted("%Y-%m-%d")

    # Connect to Kite ST
    kite, isKiteConnected = baf.connect_broker_api(
        cnx, mySQLCursor, adminTradeAccount, broker=configDict["ADMIN_TRADE_BROKER"]
    )

    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if isKiteConnected:

        alertMsg = (
            "The program ("
            + programName.replace("_", "\_")
            + ")  started at "
            + str(util.get_date_time_formatted("%d-%m-%Y %H:%M:%S"))
        )

        util.send_alerts(
            logEnableFlag,
            cnx,
            mySQLCursor,
            configDict["TELG_ADMIN_ID"],
            "INFO",
            alertMsg,
            telgUpdateFlag="Y",
            programName=programName,
        )

        try:

            # instList = util.get_test_inst_list(mySQLCursor)
   
            # for instRow in instList:
                stockName = "BANK NIFTY"
                tradeSymbol = "BANK NIFTY"
                # instrumentToken = instRow[0]
                # tradeSymbol = instRow[1]
                # stockName = instRow[2]
                print("-----------------------")
                print(tradeSymbol)
                print("-----------------------")

                fromDate = util.get_lookup_date(30)
                toDate = util.get_lookup_date(0)
                instrumentToken = "260105"
                # instrumentToken = instrumentToken
                # Verify whether the connection to MySQL database is open
                histRecords = baf.get_historical_data(
                    kite, instrumentToken, fromDate, toDate, "15minute"
                )

                df = pd.DataFrame(histRecords)
                extrema, prices, smooth_extrema, smooth_prices = cp.find_extrema(
                    df, bw="cv_ls"
                )
                patterns, patternsTrendLine  = cp.find_patterns(extrema, prices, max_bars=100)
                

                currentPrice = prices.tail(1).values[0]
                commentsHtml = ""
                # tsc.get_bullish_reversal_score(histRecords)
                (
                    netScore,
                    precedingtrendscore,
                    trendLevelScore,
                    overExtensionScore,
                    retracementLevelScore,
                    volumeOpenInterstScore,
                    sellingClimaxScore,
                    cdlMaxScore,
                    cdlTotalScore,
                    commentsHtml
                ) = tsc.get_bullish_reversal_score(histRecords)

                # print("precedingtrendscore : " + str(precedingtrendscore))
                # print("trendLevelScore : " + str(trendLevelScore))
                # print("overExtensionScore : " + str(overExtensionScore))
                # print("retracementLevelScore : " + str(retracementLevelScore))
                # print("volumeOpenInterstScore : " + str(volumeOpenInterstScore))
                # print("sellingClimaxScore : " + str(sellingClimaxScore))
                # print("cdlMaxScore : " + str(cdlMaxScore))
                # print("cdlTotalScore : " + str(cdlTotalScore))
                file_object = open("html_report.html", "a")
                file_object.write(
                    f'<a href="reports/{tradeSymbol}.html"><img src="img/{tradeSymbol}.png" width="400"></a> \n'
                )
                file_object.close()
                if os.path.exists(f"reports/{tradeSymbol}.html"):
                    os.remove(f"reports/{tradeSymbol}.html")
                else:
                    print("The file does not exist")
                # print(commentsHtml)
                file_object = open(f"reports/{tradeSymbol}.html", "a")
                file_object.write(
                    f"""
                <div class="grid-container">
                    <link rel="stylesheet" href="style.css">
                    <link href="https://fonts.googleapis.com/css2?family=Source+Sans+Pro&display=swap" rel="stylesheet">
                    <div class="grid-item"><img src="{pathadd}img/{tradeSymbol}.png" width="800"></div>
                    <a href="{pathadd}html_report.html"><img src="https://media.istockphoto.com/vectors/back-icon-vector-illustration-back-button-icon-vector-back-arrow-icon-vector-id953455676?k=20&m=953455676&s=170667a&w=0&h=IZAa6C_ceMI0gVFVqsytgpGmyV8DANEvB0Xf6Mx1wrg=" width="50"></a>
                    <div class="grid-item" style = "font-family: 'Source Sans Pro', sans-serif;">
                    <table>
                    <tr>
                        <td>Preceding Trend Score</td>
                        <td>{str(precedingtrendscore)}</td>
                    </tr>
                    <tr>
                        <td>Trend Level Score</td>
                        <td>{str(trendLevelScore)}</td>
                    </tr>
                    <tr>
                        <td>Over Extension Score</td>
                        <td>{str(overExtensionScore)}</td>
                    </tr>
                    <tr>
                        <td>Retracement Level Score</td>
                        <td>{str(retracementLevelScore)}</td>
                    </tr>
                    <tr>
                        <td>Volume Open Interest Score</td>
                        <td>{str(volumeOpenInterstScore)}</td>
                    </tr>
                    <tr>
                        <td>Selling Climax Score</td>
                        <td>{str(sellingClimaxScore)}</td>
                    </tr>
                    <tr>
                        <td>Cdl Max Score</td>
                        <td>{str(cdlMaxScore)}</td>
                    </tr>
                    <tr>
                        <td>Cdl Total Score</td>
                        <td>{str(cdlTotalScore)}</td>
                    
                    </tr>
                    </table>
                </div>
                <div class = "grid-item2" style = "font-family: 'Source Sans Pro', sans-serif;">
                    <table>
                    {str(commentsHtml)}
                    </table>

                <div class = "grid-item2" style = "font-family: 'Source Sans Pro', sans-serif;">
                <table>               

                """
                )
                file_object.close()
               

                plot_window(prices, extrema, ax=None)

                file_object = open(f"reports/{tradeSymbol}.html", "a")
                file_object.write(""" 
                                    <tr>                                        
                                        <th>Name</th>
                                        <th>End Date</th>
                                        <th>Trend Line Price</th>
                                        <th>Current Market Price</th>
                                    </tr>
                            """)

                for name, pattern_periods in patterns.items():
                    for pattern in pattern_periods:
                        endDate = datetime.datetime.strptime(
                            str((df.loc[pattern[1], "date"]).to_pydatetime()).split()[
                                0
                            ],
                            "%Y-%m-%d",
                        )
                        lookUpDate = datetime.datetime.strptime(
                            util.get_lookup_date(15), "%Y-%m-%d"
                        )
                        if endDate >= lookUpDate:
                            # file_object = open(f"reports/{tradeSymbol}.html", "a")
                            file_object.write(f"""
                                    <tr>                                        
                                        <td>{name}</td>
                                        <td>{str(endDate)}</td>
                                        <td>{patternsTrendLine[str(name)]}</td>
                                        <td>{str(currentPrice)}</td>
                                    </tr>
                            """)
                            # if( tsc.get_bullish_reversal_score(histRecords) > 0):
                            # print("-----------Buy------------")
                            print(name)
                            print(endDate)
                   
                          

                            # print("-----------------------")

                        # if (name == 'Buy Failed Swing'):
                        #     print(name)
                        #     print("-----------------------")
                        #     print(str(df.loc[pattern[0], 'date']) + ", " + str(df.loc[pattern[1], 'date']))
                        #     print("-----------------------")
                        # if (name == 'Buy Non-Failure Swing'):
                        #     print(name)
                        #     print("-----------------------")
                        #     print(str(df.loc[pattern[0], 'date']) + ", " + str(type(df.loc[pattern[1], 'date'])))
                file_object.close()
                # plot_window(prices, smooth_extrema, df, ax=None)
                # plot_window(prices, extrema, smooth_prices, smooth_extrema, df, ax=None)

        except Exception as e:
            alertMsg = "Live trade service failed (main block): " + str(e)
            print(alertMsg)

    util.logger(logEnableFlag, "info", "Program ended")
