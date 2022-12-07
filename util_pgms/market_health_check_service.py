import logging
from utils import util_functions as util
from utils import broker_api_functions as baf
import pandas as pd
import talib    
import numpy as np

def get_market_ranking(mySQLCursor, selectStatment):
    
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()  
    cnt = 0    
    marketCapRanking = []
    sma3KstRatio = []
    sma9KstRatio = []
    smaDiffRatio = []
    kstRatio = []

    for row in results:
        cnt += 1
        marketCapRanking.append(str(row[0]))                 
        kstRatio.append(str(row[1]))
        sma3KstRatio.append(str(row[2]))
        sma9KstRatio.append(str(row[3]))
        smaDiffRatio.append(str(row[4]))
        
    return marketCapRanking, kstRatio, sma3KstRatio, sma9KstRatio, smaDiffRatio


def current_trade_statistics(mySQLCursor, tradeAccount, strategyId):
    selectStatment = "SELECT TRADE_STATUS, SUM(BUY_VALUE) AS SUM_VALUE, COUNT(TRADE_STATUS) AS NO_OF_TRADES, AVG(PROFIT_PERCENT) AS AVG_PROFIT_PERCENT, \
        SUM(PROFIT_AMOUNT) AS SUM_PROFIT_AMOUNT, SUM(QUANTITY * CURRENT_MKT_PRICE) AS CURRENT_MKT_VALUE FROM TRADE_TRANSACTIONS WHERE \
            (TRADE_STATUS = 'OPEN' OR ( TRADE_STATUS = 'EXITED' AND DATE(SELL_ORDER_DATE) = '"+currDate+"' )) AND \
                STRATEGY_ID='"+str(strategyId)+"' AND TRADE_ACCOUNT='"+str(tradeAccount)+"' GROUP BY TRADE_STATUS ORDER BY TRADE_STATUS, BUY_ORDER_DATE DESC"
    mySQLCursor.execute(selectStatment)
    
    rowCount = mySQLCursor.rowcount
    exitedPositionsCnt = 0
    openPositionsCnt = 0
    exitedInvestmentValue = 0
    endingInvestmentValue = 0
    realisedProfit = 0
    endingMarketValue = 0
    unRealisedProfit = 0
    realisedProfitPercent = 0
    unRealisedProfitPercent = 0
    
    if rowCount != 0:        
        results = mySQLCursor.fetchall()
        # gets the number of rows affected by the command executed
        for row in results:
            if (str(row[0]) ==  'EXITED'):
                exitedPositionsCnt = row[2]
                exitedInvestmentValue = row[1]
                realisedProfit = row[4]
            elif(str(row[0]) ==  "OPEN"):
                openPositionsCnt =  row[2]
                endingInvestmentValue = row[1]
                endingMarketValue = row[5]
                unRealisedProfit = row[4]

        if (float(realisedProfit) != 0):
            realisedProfitPercent = (float(realisedProfit) / float(exitedInvestmentValue)) * 100
        if (float(unRealisedProfit) != 0):
            unRealisedProfitPercent = (float(unRealisedProfit) / float(endingInvestmentValue)) * 100

    return rowCount, exitedPositionsCnt, exitedInvestmentValue, realisedProfit, openPositionsCnt, endingInvestmentValue, endingMarketValue, unRealisedProfit, realisedProfitPercent, unRealisedProfitPercent
    

def start_stop_strategy_flag(cnx, mySQLCursor, startStopFlag, tradeAccount, strategyId):
    selectStatment = "SELECT STRATEGY_ON_OFF_FLAG, STRATEGY_ON_OFF_FLAG_DATE FROM USR_STRATEGY_SUBSCRIPTIONS WHERE STRATEGY_ID='"+str(strategyId)+"' AND TRADE_ACCOUNT='"+str(tradeAccount)+"'"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    tblStartStopFlag = ''
    onOffFlagAlert = ''
    for row in results:
        tblStartStopFlag = row[0]
        onOffFlagDate = row[1]

    if (tblStartStopFlag != startStopFlag):
        updateQuery ="UPDATE USR_STRATEGY_SUBSCRIPTIONS SET STRATEGY_ON_OFF_FLAG='"+str(startStopFlag)+"', STRATEGY_ON_OFF_FLAG_DATE = '" + str(updatedOn) + "', UPDATED_ON='" + str(updatedOn) + "' WHERE STRATEGY_ID='"+str(strategyId)+"' AND TRADE_ACCOUNT='"+str(tradeAccount)+"'"
        mySQLCursor.execute(updateQuery)
        cnx.commit()
        if (startStopFlag == 'OFF'):
            alertMsg = "Market panic situations; Stopping any new positions on : "+ str(strategyId)
            logging.info(alertMsg)
        else:
            alertMsg = "Market looks to be good; Switching on to get new positions : "+ str(strategyId)
            logging.info(alertMsg)
        
        # TBD - SEND THE UPDATE ALERT HERE 


def update_positions_limit(cnx, mySQLCursor, tradeAccount, strategyId, limit):
    selectStatment = "SELECT DAY_POSITION_LIMIT, DAY_POSITION_LIMIT_DATE FROM USR_STRATEGY_SUBSCRIPTIONS WHERE STRATEGY_ID='"+str(strategyId)+"' AND TRADE_ACCOUNT='"+str(tradeAccount)+"'"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    tblLimit = 0
    dayPositionLimitDate = ""
    for row in results:
        tblLimit = row[0]
        dayPositionLimitDate = row[1]
    if (tblLimit != limit):
        updateQuery ="UPDATE USR_STRATEGY_SUBSCRIPTIONS SET DAY_POSITION_LIMIT="+str(limit)+", DAY_POSITION_LIMIT_DATE = '" + str(updatedOn) + "' , UPDATED_ON='" + str(updatedOn) + "' WHERE STRATEGY_ID='"+str(strategyId)+"' AND TRADE_ACCOUNT='"+str(tradeAccount)+"'"
        mySQLCursor.execute(updateQuery)
        cnx.commit()
        
        alertMsg = "Updated the positions limit to "+ str(limit) + " for " + str(strategyId)
        logging.info(alertMsg)
        
        # TBD - SEND THE UPDATE ALERT HERE 


""" This function is used for updating """
def update_exit_signal_status(cnx, mySQLCursor, tradeAccount, strategyId):        
    try:
        updateQuery ="UPDATE TRADE_TRANSACTIONS SET EXIT_SIGNAL_STATUS='EXIT SIGNAL', EXIT_SIGNAL_REASON='MARKET PANIC', \
            EXIT_ALL_POSITIONS_FLAG_DATE ='" + str(updatedOn) + "', UPDATED_ON='" + str(updatedOn) + "' \
                WHERE TRADE_STATUS='OPEN' AND STRATEGY_ID='"+str(strategyId)+"' AND TRADE_ACCOUNT='"+str(tradeAccount)+"'"
        mySQLCursor.execute(updateQuery)
        cnx.commit()  
    except:
        logging.info('Unable to update update_uss_column')


def check_market_panic_conditions(cnx, mySQLCursor, tradeAccount, strategyId, advancingRatio, tgtStopLoss):

    # TBD - get these values in dictionary or list
    rowCount, exitedPositionsCnt, exitedInvestmentValue, realisedProfit, openPositionsCnt, endingInvestmentValue, \
        endingMarketValue, unRealisedProfit, realisedProfitPercent, unRealisedProfitPercent = current_trade_statistics(mySQLCursor, tradeAccount, strategyId)
    
    stopPositionsFlag = False

    if (rowCount != 0):
        
        # TBD - get these values in dictionary or list
        noPositionsExitedLoss = 0
        selectStatment = "SELECT TRADE_RESULT, COUNT(TRADE_STATUS) AS NO_OF_TRADES, SUM(PROFIT_AMOUNT) AS SUM_PROFIT_AMOUNT \
            FROM TRADE_TRANSACTIONS WHERE (TRADE_STATUS='EXITED' AND DATE(SELL_ORDER_DATE) = '"+currDate+"')\
                 AND STRATEGY_ID='"+str(strategyId)+"' AND TRADE_ACCOUNT='"+str(tradeAccount)+"' GROUP BY TRADE_RESULT"
        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()
        for row in results:
            if (row[0] ==  "LOSS"):
                noPositionsExitedLoss = row[1]
        
        # Looks like market is falling; better stop any new orders on the specific strategy
        if (advancingRatio < 50 and noPositionsExitedLoss == 3 and (unRealisedProfitPercent <= (tgtStopLoss * 0.25)  and unRealisedProfitPercent >= (tgtStopLoss * 0.4))):      
            
            # First, stop further buy orders send alert
            start_stop_strategy_flag(cnx, mySQLCursor, 'OFF', tradeAccount, strategyId)
            # TBD - add a functionalities to close the open buy orders 
            # delete all open orders
            # kcf.deleteOpenOrders(kite,"SWING")
            
            # Second, change the day position limit
            update_positions_limit(cnx, mySQLCursor, tradeAccount, strategyId, limit=0)
            stopPositionsFlag = True
        
        # Oops, market is going down further, stop all the orders and exit all positions
        elif (advancingRatio < 40 and noPositionsExitedLoss >= 3 and (unRealisedProfitPercent < (tgtStopLoss * 0.4))):
            # First, stop further buy orders send alert
            start_stop_strategy_flag(cnx, mySQLCursor, 'OFF', tradeAccount, strategyId)
            
            # Second, change the day position limit
            update_positions_limit(cnx, mySQLCursor, tradeAccount, strategyId, limit=0)
            
            # TBD - add a functionalities to close the open buy orders            # 
            # delete all open orders
            # kcf.deleteOpenOrders(kite,"SWING")
            
            # Third, exit all the open positions
            update_exit_signal_status(cnx, mySQLCursor, tradeAccount, strategyId)
            stopPositionsFlag = True
    
    return stopPositionsFlag

def check_reentry_conditions(mySQLCursor):

    selectStatment = """SELECT * FROM ( SELECT DATE, SUM(CASE WHEN ROC1 > 0 THEN 1 ELSE 0 END) AS ADVANCING ,
                            SUM(CASE WHEN ROC1 < 0 THEN 1 ELSE 0 END) AS DECLINE, SUM(CASE WHEN HIGH_252_FLAG = 'Y' THEN 1 ELSE 0 END) AS HIGH252D , 
                            SUM(CASE WHEN LOW_252_FLAG = 'Y' THEN 1 ELSE 0 END) AS LOW252D  FROM MARKET_PERFORMANCE_TBL 
                            GROUP BY DATE ORDER BY DATE DESC LIMIT 21) SUB ORDER BY DATE ASC"""

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()   
    
    df = pd.DataFrame()    

    dfNifty50 = pd.DataFrame(nifty50HistRecords)
    dfIndiaVix = pd.DataFrame(vixHistRecords)

    df = pd.DataFrame(results, columns=['DATE','ADVANCING','DECLINE','HIGH252D','LOW252D'])    
    df['ADVANCE_RATIO'] = (df['ADVANCING']  / (df['ADVANCING'] + df['DECLINE'])) * 100
    df['HIGH252D_RATIO'] = (df['HIGH252D']  /  (df['ADVANCING'] + df['DECLINE'])) * 100
    df['LOW252D_RATIO'] = (df['LOW252D']  /  (df['ADVANCING'] + df['DECLINE'])) * 100
    df['SMA3_ADVANCE_RATIO'] = talib.SMA(df['ADVANCE_RATIO'], timeperiod=3)
    df['SMA5_ADVANCE_RATIO'] = talib.SMA(df['ADVANCE_RATIO'], timeperiod=5)    
    df['SMA9_ADVANCE_RATIO'] = talib.SMA(df['ADVANCE_RATIO'], timeperiod=9)
    df['DIFF_SMA3_SMA5'] = df['SMA3_ADVANCE_RATIO'] - df['SMA5_ADVANCE_RATIO']

    df['ENTRY_FLAG'] = np.where(((df['DIFF_SMA3_SMA5'] > 0) & (df['DIFF_SMA3_SMA5'] > df['DIFF_SMA3_SMA5'].shift(1)) & ((df['ADVANCE_RATIO'] > 30) & (df['ADVANCE_RATIO'] < 40))), 'ENTRY1',
                                np.where(((df['DIFF_SMA3_SMA5'] > 0) & (df['DIFF_SMA3_SMA5'] > df['DIFF_SMA3_SMA5'].shift(1)) & ((df['ADVANCE_RATIO'] >= 40) & (df['ADVANCE_RATIO'] < 50))), 'ENTRY2',
                                    np.where(((df['DIFF_SMA3_SMA5'] > 0) & (df['DIFF_SMA3_SMA5'] > df['DIFF_SMA3_SMA5'].shift(1)) & (df['ADVANCE_RATIO'] >= 50)), 'ENTRY3',''
                                )))

    df['INDIA_VIX_CLOSE'] =  dfIndiaVix['close']
    df['NIFTY50_CLOSE'] = dfNifty50['close']
    df['NIFTY50_HIGH'] = dfNifty50['high']
    df['NIFTY50_LOW'] = dfNifty50['low']

    df['BBANDS_UPPER'], df['BBANDS_MIDDLE'], df['BBANDS_LOWER'] = talib.BBANDS(df['NIFTY50_CLOSE'], timeperiod=10, nbdevup=1, nbdevdn=1, matype=0) 
    #Keltner Channel
    df['TYPPRICE'] = talib.TYPPRICE( df['NIFTY50_HIGH'], df['NIFTY50_LOW'], df['NIFTY50_CLOSE'])
    df['ATR'] = talib.ATR(df['NIFTY50_HIGH'], df['NIFTY50_LOW'], df['NIFTY50_CLOSE'], timeperiod=10)
    df['EMA20_TYPPRICE'] = talib.EMA(df['TYPPRICE'], timeperiod=10)
    df['KC_UpperLine'] = df['EMA20_TYPPRICE'] + (1*df['ATR'])
    df['KC_LowerLine'] = df['EMA20_TYPPRICE'] - (1*df['ATR'])
    
    def in_Squeeze(df):
        Squeeze = 0
        if (df['BBANDS_UPPER'] < df['KC_UpperLine']) and (df['BBANDS_LOWER'] > df['KC_LowerLine']):
            Squeeze = 1
        else : 
            Squeeze = 0 
        return Squeeze

    df['TTMSqueeze'] = df.apply(in_Squeeze, axis=1)
    df['TTMSqueeze1'] = df.apply(in_Squeeze, axis=1).shift(1)
    df['TTMSLength'] = talib.SUM(abs(df['TTMSqueeze']), timeperiod=8)

    lastRecord = df.tail(1)
    macdValue = lastRecord['SMA3_ADVANCE_RATIO'].values[0]
    advanceRatio = lastRecord['ADVANCE_RATIO'].values[0]
    entryFlag = lastRecord['ENTRY_FLAG'].values[0]
    return advanceRatio, entryFlag

def get_live_trade_accounts(mySQLCursor):
    selectStatment = "SELECT DISTINCT USS.TRADE_ACCOUNT, USS.STRATEGY_ID, UTA.BROKER, USS.TGT_STOP_LOSS_PCT FROM USR_STRATEGY_SUBSCRIPTIONS USS LEFT JOIN USR_TRADE_ACCOUNTS UTA ON USS.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT AND USS.ACTIVE_FLAG='ACTIVE' WHERE UTA.ACCOUNT_TYPE='LIVE' ORDER BY USS.TRADE_ACCOUNT"    
    mySQLCursor.execute(selectStatment)
    return mySQLCursor.fetchall()

def get_usr_positions_limit(mySQLCursor):
    selectStatment = "SELECT DAY_POSITION_LIMIT, DAY_POSITION_LIMIT_BY_USR, DATE(DAY_POSITION_LIMIT_DATE) FROM USR_STRATEGY_SUBSCRIPTIONS WHERE STRATEGY_ID='"+str(strategyId)+"' AND TRADE_ACCOUNT='"+str(tradeAccount)+"'"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    dayPositionLimit = 0
    dayPositionLimitByUser = 0  
    dayPositionLimitDate = currDate                                  
    for row in results:
        dayPositionLimit = int(row[0])
        dayPositionLimitByUser = int(row[1])
        dayPositionLimitDate = row[2].strftime("%Y-%m-%d")

    return dayPositionLimit, dayPositionLimitByUser, dayPositionLimitDate

def get_market_rankings(mySQLCursor):
                        
    # Use these indicators for capital allocation 
    selectStatment = """SELECT TRADING_SYMBOL, KST_RATIO, SMA3_KST_RATIO, SMA9_KST_RATIO, (SMA3_KST_RATIO - SMA9_KST_RATIO) AS SMA_DIFF FROM MARKET_PERFORMANCE_TBL WHERE CATEGORY='Market Cap' and TRADING_SYMBOL != 'NIFTY 500' and DATE= (SELECT MAX(DATE) FROM MARKET_PERFORMANCE_TBL ORDER BY DATE DESC) ORDER BY KST_RATIO DESC"""
    mktCapRanking, mktKstRatio, mktSma3KstRatio, mktSma9KstRatio, mktSmaDiffRatio = get_market_ranking(mySQLCursor,selectStatment)
                                
    # allocateCapital(cnx, mySQLCursor, mktCapRanking, mktKstRatio, mktSma3KstRatio, mktSma9KstRatio, mktSmaDiffRatio)

    selectStatment = """SELECT TRADING_SYMBOL, KST_RATIO, SMA3_KST_RATIO, SMA9_KST_RATIO, (SMA3_KST_RATIO - SMA9_KST_RATIO) AS SMA_DIFF FROM MARKET_PERFORMANCE_TBL WHERE CATEGORY='Sector' and DATE= (SELECT MAX(DATE) FROM MARKET_PERFORMANCE_TBL ORDER BY DATE DESC) AND SMA3_KST_RATIO > SMA9_KST_RATIO AND KST_RATIO > 1 ORDER BY KST_RATIO DESC LIMIT 10"""
    secRanking, secKstRatio, secSma3KstRatio, secSma9KstRatio, secSmaDiffRatio = get_market_ranking(mySQLCursor,selectStatment)
                                
    selectStatment = "SELECT TRADING_SYMBOL, KST_RATIO, SMA3_KST_RATIO, SMA9_KST_RATIO, (SMA3_KST_RATIO - SMA9_KST_RATIO) AS SMA_DIFF FROM MARKET_PERFORMANCE_TBL WHERE CATEGORY='Stocks' and BENCHMARK_INDEX in "+ str(secRanking).replace('[','(').replace(']',')') +" and DATE= (SELECT MAX(DATE) FROM MARKET_PERFORMANCE_TBL ORDER BY DATE DESC) AND SMA3_KST_RATIO > SMA9_KST_RATIO AND KST_RATIO > 1 AND (VOLUME * CLOSE_PRICE) > 10000000  ORDER BY KST_RATIO DESC LIMIT 100"
    stkRanking, stkKstRatio, stkSma3KstRatio, stkSma9KstRatio, stkSmaDiffRatio = get_market_ranking(mySQLCursor,selectStatment)


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
    
    programName = configDict['MKT_HEALTH_CHECK_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['MKT_HEALTH_CHECK_PGM_NAME']) + '.log')

    programExitFlag = 'N'    

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)
    
    adminTradeAccount = configDict['ADMIN_TRADE_ACCOUNT']

    # Connect to Kite ST
    kite, isKiteConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = configDict['ADMIN_TRADE_BROKER'])    
    
    currDate = util.get_date_time_formatted("%Y-%m-%d")
    updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")

    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if (isKiteConnected):           
       
        alertMsg = 'The program (' + programName.replace('_','\_') + ')  started at ' + str(util.get_date_time_formatted("%d-%m-%Y %H:%M:%S"))
        
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)

        # Dates between which we need historical data
        fromDate = util.get_lookup_date(360)
        # toDate = kcf.getCurrDate()
        toDate = util.get_lookup_date(0)
        interval = "day"

        try:
            # Get historical data of given instrument token; the number 256265 is for nifty50 instrument            
            nifty50HistRecords = baf.get_historical_data(kite, '256265', fromDate, toDate, interval) 

            # Get historical data of given instrument token; the number 264969 is for India VIX instrument         
            vixHistRecords = baf.get_historical_data(kite, '264969', fromDate, toDate, interval)
        except:
            logging.info("Errored while trying to get historical records for nifty50HistRecords and vixHistRecords")

        # Continuously run the program until the exit flag turns to Y
        while programExitFlag != 'Y': 
            try:
                # Verify whether the connection to MySQL database is open
                cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
                
                sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
                currentTime = util.get_date_time_formatted("%H%M")
                updatedOn =util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")

                if (testingFlag or ((int(currentTime) >= int(sysSettings['SYSTEM_START_TIME'])) and (int(currentTime) <= int(sysSettings['SYSTEM_END_TIME'])))):
                    try: 
                        for row in get_live_trade_accounts(mySQLCursor):        
                            tradeAccount = row[0]
                            strategyId = row[1]
                            tgtStopLoss = row[3]

                            advancingRatio, entryFlag = check_reentry_conditions(mySQLCursor)

                            stopPositionsFlag = check_market_panic_conditions(cnx, mySQLCursor, tradeAccount, strategyId, advancingRatio, tgtStopLoss)
                           

                            if (not(stopPositionsFlag) and entryFlag != ''):

                                # First, stop further buy orders send alert
                                start_stop_strategy_flag(cnx, mySQLCursor, 'ON', tradeAccount, strategyId)

                                dayPositionLimit, dayPositionLimitByUser, dayPositionLimitDate = get_usr_positions_limit(mySQLCursor)

                                if (dayPositionLimit < int(dayPositionLimitByUser * 0.25)):
                                    # Second, change the day position limit
                                    update_positions_limit(cnx, mySQLCursor, tradeAccount, strategyId, limit = int(dayPositionLimitByUser * 0.25))
                                    # alertMsg = "Entry flag has been updated to lemon green, we will start new orders with limited positions"
                                    # kcf.sendAlerts(cnx, mySQLCursor, 'ST',  chatIdAdmin, 'ALERT', alertMsg, 'Y', 'Y')
                                elif ((entryFlag == 'ENTRY2' or entryFlag == 'ENTRY3') and dayPositionLimit < int(dayPositionLimitByUser * 0.5)):
                                    # Second, change the day position limit
                                    update_positions_limit(cnx, mySQLCursor, tradeAccount, strategyId, limit = int(dayPositionLimitByUser * 0.5))
                                    # alertMsg = "Entry flag has been updated to mint green, we will start new orders with half the available positions"
                                    # kcf.sendAlerts(cnx, mySQLCursor, 'ST',  chatIdAdmin, 'ALERT', alertMsg, 'Y', 'Y')                
                                elif ((entryFlag == 'ENTRY2' or entryFlag == 'ENTRY3') and dayPositionLimit < dayPositionLimitByUser and dayPositionLimitDate != currDate):
                                    update_positions_limit(cnx, mySQLCursor, tradeAccount, strategyId, limit = dayPositionLimitByUser)
                                    # alertMsg = "Entry flag has been updated to forest green, we will start new orders with full positions"
                                    # kcf.sendAlerts(cnx, mySQLCursor, 'ST',  chatIdAdmin, 'ALERT', alertMsg, 'Y', 'Y')
                                
                            
                            advancingSignal = ''
                            if(advancingRatio < 40):
                                advancingSignal = 'RED'
                                alertMsg = " The current advancingRatio is "+ str(advancingRatio) +". Market is expected to fall; be cautious.\n\n\t\t1. Avoid any new positions.\n\t\t2. Book profits if you have any short term positions"
                                # kcf.sendAlerts(cnx, mySQLCursor, 'ST',  chatIdAdmin, 'ALERT', alertMsg, 'Y', 'Y')
                                logging.info(alertMsg)

                            elif(advancingRatio > 40 and advancingRatio < 60):
                                advancingSignal = 'YELLOW'
                                alertMsg = "The current advancingRatio is "+ str(advancingRatio) +". Market is in YELLOW status"
                                logging.info(alertMsg)
                                # kcf.sendAlerts(cnx, mySQLCursor, 'ST',  chatIdAdmin, 'ALERT', alertMsg, 'Y', 'Y')
                                
                            elif(advancingRatio >= 60):
                                advancingSignal = 'GREEN'
                                alertMsg = "The current advancingRatio is "+ str(advancingRatio) +". Market is in GREEN status"
                                # kcf.sendAlerts(cnx, mySQLCursor, 'ST',  chatIdAdmin, 'ALERT', alertMsg, 'Y', 'Y')
                                logging.info(alertMsg)
                                                                
                                # TBD - Not used for now but needs to be reviewed latter on
                                # get_market_rankings(mySQLCursor)

                    except Exception as e:
                        alertMsg = 'Exceptions occured in market health check block: ' + str(e)                  
                        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    
                    
                                        
                elif (int(currentTime) > int(sysSettings['SYSTEM_END_TIME'])):
                    alertMsg = 'System end time reached; exiting the program now'                  
                    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    
                    programExitFlag = 'Y'
                
                util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
                util.disconnect_db(cnx, mySQLCursor)
            
            except Exception as e:
                alertMsg = 'Live trade service failed (main block): '+ str(e)
                util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)


    else:
        alertMsg = 'Unable to connect admin trade account from buy trade service. The singal records will not be updated for today until the issue is fixed.'
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='Y', programName=programName)    
        
        
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

    util.update_program_running_status(cnx, mySQLCursor,programName, 'INACTIVE')

    util.disconnect_db(cnx, mySQLCursor)
    util.logger(logEnableFlag, 'info', "Program ended") 
