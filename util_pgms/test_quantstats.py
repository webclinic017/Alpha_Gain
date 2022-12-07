# Note there few changed in the Quantstats library inorder to make it working 
# 1. There is a problem quantstats API _consecutive_losses is missing in the definition, needs to be checked before execution.
# 2. There is a problem quantstats API reading the date format def _drawdown_details(drawdown) while reading the dates start, end, valley date..  


import datetime
from datetime import timedelta
import time
from kiteconnect import KiteConnect
import mysql.connector
import logging
from utils import util_functions as util
from utils import util_functions as uf
from utils import broker_api_functions as baf
import talib
import matplotlib
import matplotlib.pyplot as plt
import quantstats as qs
from pandas import DataFrame

def getPortfolioPerformance(mySQLCursor, strategyId):
    selectStatment = "SELECT TRADE_DATE, (PORTFOLIO_VALUE + CASH_POSITION) AS CLOSING_PORTFOLIO_VAL FROM BT_PORTFOLIO_TREND WHERE STRATEGY='"+strategyId+"'"
    mySQLCursor.execute(selectStatment)
    # gets the number of rows affected by the command executed    
    results = mySQLCursor.fetchall()
    
    return results

# Main function is called by default, and the first function to be executed
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
        
    
    strategyId = "BT_LT1"

    dfPortfolioPerf = DataFrame(getPortfolioPerformance(mySQLCursor, strategyId))
    print(dfPortfolioPerf)
    dfPortfolioPerf.columns = ['DATE', 'PORTFOLIO_VALUE']
    dfPortfolioPerf.set_index('DATE', inplace= True)  
    dfPortfolioPerf['ROC1'] = talib.ROC(dfPortfolioPerf['PORTFOLIO_VALUE'], timeperiod=1)  
    print(dfPortfolioPerf)
    strategy = strategyId
    startDate = "" #dfPortfolioPerf.iloc[0][0]
    endDate = "" # dfPortfolioPerf.iloc[dfPortfolioPerf.shape-1][0]

    #print(startDate)
    # print(endDate)
    duration = ""
    equityStartValue = 0
    EquityEndValue = 0
   
    # extend pandas functionality with metrics, etc.
    qs.extend_pandas()
    
    # Performance Calculations from Quantstats
    avgReturn = qs.stats.avg_return(dfPortfolioPerf)[0]
    avgWin = qs.stats.avg_win(dfPortfolioPerf)[0]
    avgLoss= qs.stats.avg_loss(dfPortfolioPerf)[0]
    cagr = qs.stats.cagr(dfPortfolioPerf)[0]
    expectedReturn = qs.stats.expected_return(dfPortfolioPerf)[0]
    expectedShortfall = qs.stats.expected_shortfall(dfPortfolioPerf)[0]
    
    consecutiveLosses = qs.stats.consecutive_losses(dfPortfolioPerf)[0] # There is a problem quantstats API _consecutive_losses is missing in the definition.
    consecutiveWins = qs.stats.consecutive_wins(dfPortfolioPerf)[0] # There is a problem quantstats API _consecutive_losses is missing in the definition..
    gainToPainRatio = 5 #qs.stats.gain_to_pain_ratio(dfPortfolioPerf)[0]
    profitFactor = qs.stats.profit_factor(dfPortfolioPerf)[0]
    riskOfRuin = qs.stats.risk_of_ruin(dfPortfolioPerf)[0]
    
    winRate = qs.stats.win_rate(dfPortfolioPerf)[0]
    sharpeRatio = qs.stats.sharpe(dfPortfolioPerf)[0]
    sortino = qs.stats.sortino(dfPortfolioPerf)[0]
    calmar = qs.stats.calmar(dfPortfolioPerf)[0]
    informationRatio = 2 # qs.stats.information_ratio(dfPortfolioPerf)[0]
  
    kurtosis = qs.stats.kurtosis(dfPortfolioPerf)[0]
    skew = qs.stats.skew(dfPortfolioPerf)[0]
    valueAtRisk = qs.stats.value_at_risk(dfPortfolioPerf)[0]
    conditionalValueAtRisk = qs.stats.conditional_value_at_risk(dfPortfolioPerf)[0]
    volatility = qs.stats.volatility(dfPortfolioPerf)[0]

    outlierLossRatio = qs.stats.outlier_loss_ratio(dfPortfolioPerf)[0]
    outlierWinRatio = qs.stats.outlier_win_ratio(dfPortfolioPerf)[0]
    maxDrawdown = qs.stats.max_drawdown(dfPortfolioPerf)[0]
    riskReturnRatio = qs.stats.risk_return_ratio(dfPortfolioPerf)[0]
    worst = qs.stats.worst(dfPortfolioPerf)[0]

    best = qs.stats.best(dfPortfolioPerf)[0]
    kellyCriterion = qs.stats.kelly_criterion(dfPortfolioPerf)[0]
    winLossRatio = qs.stats.win_loss_ratio(dfPortfolioPerf)[0]
    ulcerIndex = qs.stats.ulcer_index(dfPortfolioPerf)[0]
    ulcerPerformanceIndex = qs.stats.ulcer_performance_index(dfPortfolioPerf)[0]

    drawdown=qs.stats.to_drawdown_series(dfPortfolioPerf) 
    # print(drawdown)
    drawdownDetails  = qs.stats.drawdown_details(drawdown)  # There is a problem quantstats API reading the date format def _drawdown_details(drawdown) while reading the dates start, end, valley date..
    print(type(drawdownDetails))
    
    def deleteTables (cnx, mySQLCursor):
        query = "DELETE FROM BT_WORST_DRAWDOWNS WHERE STRATEGY = '"+strategyId+"'"
        mySQLCursor.execute(query)
        query = "DELETE FROM BT_PERFORMANCE_SUMMARY WHERE STRATEGY = '"+strategyId+"'"
        mySQLCursor.execute(query)
        cnx.commit()

    deleteTables(cnx, mySQLCursor)

    insertVal = []
    insertQuery = "INSERT INTO BT_PERFORMANCE_SUMMARY (STRATEGY, START_DATE, END_DATE, DURATION,\
        EQUITY_START_VALUE, EQUITY_END_VALUE, AVG_RETURN, AVG_WIN, AVG_LOSS, CAGR, EXPECTED_RETURN, EXPECTED_SHORTFALL, CONSECUTIVE_LOSSES,\
        CONSECUTIVE_WINS, GAIN_TO_PAIN_RATIO, PROFIT_FACTOR, RISK_OF_RUIN, WIN_RATE, SHARPE_RATIO, SORTINO_RATIO, CALMAR_RATIO, INFORMATION_RATIO,\
        KURTOSIS, SKEW, VALUE_AT_RISK, CONDITIONAL_VALUE_AT_RISK, VOLATILITY, OUTLIER_LOSS_RATIO, OUTLIER_WIN_RATIO, MAX_DRAWDOWN,\
        RISK_RETURN_RATIO, WORST, BEST, KELLY_CRITERION, WIN_LOSS_RATIO, ULCER_INDEX, ULCER_PERFORMANCE_INDEX) VALUES (\
        %s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s)"

    insertVal.insert(0, str(strategy))
    insertVal.insert(1, str(startDate))
    insertVal.insert(2, str(endDate))
    insertVal.insert(3, str(duration))
    insertVal.insert(4, str(equityStartValue))
    insertVal.insert(5, str(EquityEndValue))
    insertVal.insert(6, str(avgReturn))
    insertVal.insert(7, str(avgWin))
    insertVal.insert(8, str(avgLoss))
    insertVal.insert(9, str(cagr))
    insertVal.insert(10, str(expectedReturn))
    insertVal.insert(11, str(expectedShortfall))
    insertVal.insert(12, str(consecutiveLosses))
    insertVal.insert(13, str(consecutiveWins))
    insertVal.insert(14, str(gainToPainRatio))
    insertVal.insert(15, str(profitFactor))
    insertVal.insert(16, str(riskOfRuin))
    insertVal.insert(17, str(winRate))
    insertVal.insert(18, str(sharpeRatio))
    insertVal.insert(19, str(sortino))
    insertVal.insert(20, str(calmar))
    insertVal.insert(21, str(informationRatio))
    insertVal.insert(22, str(kurtosis))
    insertVal.insert(23, str(skew))
    insertVal.insert(24, str(valueAtRisk))
    insertVal.insert(25, str(conditionalValueAtRisk))
    insertVal.insert(26, str(volatility))
    insertVal.insert(27, str(outlierLossRatio))
    insertVal.insert(28, str(outlierWinRatio))
    insertVal.insert(29, str(maxDrawdown))
    insertVal.insert(30, str(riskReturnRatio))
    insertVal.insert(31, str(worst))
    insertVal.insert(32, str(best))
    insertVal.insert(33, str(kellyCriterion))
    insertVal.insert(34, str(winLossRatio))
    insertVal.insert(35, str(ulcerIndex))
    insertVal.insert(36, str(ulcerPerformanceIndex))

    mySQLCursor.execute(insertQuery, insertVal)
    cnx.commit()        

    insertQuery = "INSERT INTO BT_WORST_DRAWDOWNS (STRATEGY, PEAK_DATE, VALLEY_DATE, RECOVERY_DATE, DURATION_DAYS,\
        MAX_DRAWDOWN_PCT, STRESSED_DRAWDOWN_99_PCT) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    
    insertVal = []
    insertArrayVal = []
    cnt = 0
    for data in drawdownDetails.values:
        insertVal.insert(0, str(strategy))
        insertVal.insert(1, str(data[0]))
        insertVal.insert(2, str(data[1]))
        insertVal.insert(3, str(data[2]))
        insertVal.insert(4, str(data[3]))
        insertVal.insert(5, str(data[4]))
        insertVal.insert(6, str(data[5]))
        insertArrayVal.insert(cnt, insertVal)
        insertVal = []
        cnt += 1
    mySQLCursor.executemany(insertQuery, insertArrayVal)
    cnx.commit()    
        

    uf.disconnectDB(cnx, mySQLCursor)
    finishTime = datetime.datetime.now()
    difference = finishTime - startTime
    print("Processed in " + str(difference) + " minutes")