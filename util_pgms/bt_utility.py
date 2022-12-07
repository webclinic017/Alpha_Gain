import pandas as pd
from utils import util_functions as util
from utils import trade_scoring_copy as tsc
from utils import broker_api_functions as baf
import logging


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
    
    TELG_ADMIN_ID = configDict['TELG_ADMIN_ID']

    programName = configDict['BT_PATTERNS_BASED_ENTRY_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(programName) + '.log')

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
        
        # Verify whether the connection to MySQL database is open
        cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
        
        sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
        currentTime = util.get_date_time_formatted("%H%M")
        updatedOn = util.get_date_time_formatted("%d-%m-%Y %H:%M:%S")
        try:
            strategyId = 'ALPHA_10A'
            reportDate = util.get_system_date()
            instList = util.get_all_fno_inst_list(mySQLCursor, limitCnt=200)
            interval = 'day'
            fromDate = util.get_bt_from_date_based_interval(interval, dateNow=reportDate)
            toDate = reportDate   
            instListLocal = []
            
            for instRow in instList:                      
                instListDict = {}
                instListDict['instrumentToken']  = instRow[0]
                instListDict['tradeSymbol'] = instRow[1]                            
                instListDict['stockName'] = instRow[2]
                histRecords = baf.get_historical_data(kite, instListDict['instrumentToken'], fromDate, toDate, interval)
         
                df = pd.DataFrame(histRecords)
                df.date = df.date + pd.Timedelta('05:30:00')                                
                instListDict['df'] = df                
                instListDict['futInstName'], instListDict['futInstToken'], instListDict['lotSize'], instListDict['expDate'] = util.get_futures_instruments(mySQLCursor, instListDict['tradeSymbol'] )                
                instListLocal.append(instListDict)
 
            for instRow in instListLocal: 
                df = instRow['df']
                dfStart = 200
                dfEnd = 461
                while dfEnd < df.shape[0]:                                       
                    dfTemp = df.iloc[dfStart:dfEnd]
                    op = dfTemp['open']
                    hi = dfTemp['high']
                    lo = dfTemp['low']
                    cl = dfTemp['close']
                    vol = dfTemp['volume']
                    dfTemp = tsc.get_ti_for_bullish_reversal(dfTemp, op, hi, lo, cl, vol)   
                    lastRec = dfTemp.tail(1)         
                    signalDate = lastRec['date'].values[0]
                    RSISellSignal = tsc.get_RSI2_sell_signal(lastRec)
                    dfStart += 1
                    dfEnd += 1

                    if (RSISellSignal == 'Sell'):
                        insertVal = []
                        insertQuery = "INSERT INTO BT_SHORT_LISTED_INSTRUMENTS (DATE, STRATEGY_ID, INSTRUMENT_TOKEN, TRADING_SYMBOL, STOCK_NAME, UPDATED_ON) VALUES (%s,%s,%s,%s,%s,%s)"
                        insertVal.insert(0, str(signalDate))
                        insertVal.insert(1, str(strategyId))
                        insertVal.insert(2, str(instRow['instrumentToken']))
                        insertVal.insert(3, str(instRow['tradeSymbol']))
                        insertVal.insert(4, str(instRow['stockName']))
                        insertVal.insert(5, str(updatedOn))             
                        mySQLCursor.execute(insertQuery, insertVal)
                        cnx.commit()


          
        except Exception as e:
            alertMsg = "Live trade service failed (main block): " + str(e)
            logging.info(alertMsg)

        
        # util.update_program_running_status(cnx, mySQLCursor,programN---`ame, 'ACTIVE')
        util.disconnect_db(cnx, mySQLCursor)


    util.logger(logEnableFlag, "info", "Program ended")
