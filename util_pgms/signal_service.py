from utils import util_functions as util
from utils import broker_api_functions as baf
import datetime
import numpy as np
import logging

def open_signal_flag(mySQLCursor, instrumentToken, strategyId):    
    selectStatment = "SELECT INSTRUMENT_TOKEN FROM TRADE_SIGNALS WHERE STRATEGY_ID='" + strategyId + "' AND INSTRUMENT_TOKEN = " + instrumentToken + "  AND SIGNAL_STATUS IN ('OPEN', 'PENDING')"
    mySQLCursor.execute(selectStatment)
    # gets the number of rows affected by the command executed
    rowCount = mySQLCursor.rowcount
    signalExistFlag = False
    if rowCount != 0:        
        signalExistFlag = True
    
    return signalExistFlag


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
    programName = configDict['SIGNAL_SVC_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['SIGNAL_SVC_PGM_NAME']) + '.log')

    programExitFlag = 'N'    

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)
    
    adminTradeAccount = configDict['ADMIN_TRADE_ACCOUNT']
    adminTradeBroker = configDict['ADMIN_TRADE_BROKER'] 
    # Connect to Kite ST
    apiConnObj, isAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = adminTradeBroker)    

    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if (isAPIConnected):           
        
        alertMsg = 'The program (' + programName.replace('_','\_') + ')  started at ' + str(util.get_date_time_formatted("%d-%m-%Y %H:%M:%S"))
        
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)
        
        # Continuously run the program until the exit flag turns to Y
        while programExitFlag != 'Y': 
            try:
                # Verify whether the connection to MySQL database is open
                cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
                sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
                currentTime = util.get_date_time_formatted("%H%M")

                if (testingFlag or ((int(currentTime) >= int(sysSettings['SYSTEM_START_TIME'])) and (int(currentTime) <= int(sysSettings['SYSTEM_END_TIME'])))):
                    try: 
                        cycleStartTime = util.get_system_time()                 

                        for row in util.get_active_strategy_id(mySQLCursor):                                    

                            strategyId = row[0]
                            strategy = row[1]  
                            productOrderType = row[2] 
                            instSelectionMethod = row[3] 
                            
                            # Get the list of stocks for DYNAMIC INSTRUMENTS SELECTION METHOD                    
                            if (instSelectionMethod == "DYNAMIC"):
                                instList = util.get_dynamic_insturments_list(mySQLCursor, strategyId)
                            # Get the list of stocks for PRE_DEFINED INSTRUMENTS SELECTION METHOD                          
                            elif(instSelectionMethod == "PRE_DEFINED"):                                
                                instList = util.get_pre_defined_inst_list(mySQLCursor, strategyId)
                            else:
                                instList = util.get_dynamic_insturments_list(mySQLCursor, strategyId)
                            
                            # Get the system strategy settings from database and store them in dictonary
                            sysStgyMasterDict  = util.get_system_master_strategy(mySQLCursor, strategyId)    
                            # Dates between which we need historical data
                            fromDate = util.get_lookup_date(sysStgyMasterDict['LOOK_BACK_DAYS_START'])
                            toDate = util.get_lookup_date(sysStgyMasterDict['LOOK_BACK_DAYS_END'])
                            interval = sysStgyMasterDict['PRICE_INTERVAL']
                            niftyHistRecords = baf.get_historical_data(apiConnObj, '268041', fromDate, toDate, interval, broker=adminTradeBroker, exchangeToken='1047')  
                                                      
                            # ------- TBD Completed-----------
                            
                            for instRow in instList:

                                instrumentToken = instRow[0]
                                tradeSymbol = instRow[1]                            
                                stockName = instRow[2]
                                exchangeToken = instRow[3]

                                if (not(open_signal_flag(mySQLCursor, instrumentToken, strategyId))):
                                    # Get historical data of given instrument token
                                    histRecords = baf.get_historical_data(apiConnObj, instrumentToken, fromDate, toDate, interval, broker=adminTradeBroker, exchangeToken=exchangeToken)

                                    # ------- TBD -----------
                                    # Get Indicators for the given stockname
                                    df, indSuccssFlag = util.get_indicators(histRecords, niftyHistRecords, stockName, category='Stocks')
                                    
                                    if (indSuccssFlag):
                                        # Get Buy and Sell Decisions
                                        signalRecord, transactionType, buySellScore = util.get_buy_sell_decision(df, strategy)

                                        if (transactionType == "BUY"):  

                                            lastPrice = signalRecord['close'].values[0]
                                            high5Price = signalRecord['HIGH5'].values[0]
                                            low5Price = signalRecord['LOW5'].values[0]                                    
                                            triggerPrice = util.get_trigger_price(float(high5Price), float(low5Price), float(lastPrice))
                                            
                                            if (productOrderType == 'MKT'):
                                                triggerPrice = lastPrice

                                            targetPrice = float(triggerPrice) * (1 + (float(sysStgyMasterDict['TGT_PROFIT_PCT']) / 100))
                                            stopLossPrice = float(triggerPrice) * (1 - (float(sysStgyMasterDict['TGT_PROFIT_PCT']) / 100))
                                            tradeSignalsDict = {}
                                            tradeSignalsDict['instrumentToken']=instrumentToken
                                            tradeSignalsDict['strategyId']=strategyId
                                            tradeSignalsDict['stockName']=stockName
                                            tradeSignalsDict['triggerPrice']=triggerPrice
                                            tradeSignalsDict['lastPrice']=lastPrice
                                            tradeSignalsDict['stopLossPrice']=stopLossPrice
                                            tradeSignalsDict['targetPrice']=targetPrice
                                            tradeSignalsDict['targetPercent']=sysStgyMasterDict['TGT_PROFIT_PCT']
                                            tradeSignalsDict['stopLossPercent']=sysStgyMasterDict['TGT_STOP_LOSS_PCT']
                                            tradeSignalsDict['tradeSymbol']=tradeSymbol
                                            tradeSignalsDict['horizon']=sysStgyMasterDict['TGT_HORIZON']
                                            tradeSignalsDict['transactionType']=transactionType
                                            tradeSignalsDict['buySellScore']=buySellScore
                                            tradeSignalsDict['productOrderType']=productOrderType


                                            util.insert_trade_signals(cnx, mySQLCursor, tradeSignalsDict) 

                                            alertMsg = "*Stock Name:  " + str(stockName).replace('&', ' and ') + "*\n*Call:*  Buy\n*Period:*  Up to "+ str(sysStgyMasterDict['TGT_HORIZON']) + " days \n*Trigger Price:*  " + str('%.2f' % triggerPrice) +"\n*Target Price:*  " + str(targetPrice) + "\n*Strategy:*  " + str(strategyId) + "\n\nThis is an automated and system generated call for testing and educational purposes only. You must consider all relevant risk factors, including your own personal financial situation, before trading."

                                            util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'SIGNALS', alertMsg, telgUpdateFlag='Y', programName=programName, strategyId=strategyId, stockName=stockName)   

                        cycleEndTime = util.get_system_time()   
                        cycleDiff = cycleEndTime - cycleStartTime                
                        util.logger(logEnableFlag, 'info', "Cycle ended in " + str(cycleDiff) + " minutes") 

                    except Exception as e:
                        alertMsg = 'Exceptions occured in signal service: ' + str(e)                  
                        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    


                elif (int(currentTime) > int(sysSettings['SYSTEM_END_TIME'])):
                    alertMsg = 'System end time reached; exiting the program now'                  
                    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    
                    
                    programExitFlag = 'Y'

                util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
                util.disconnect_db(cnx, mySQLCursor)

            except Exception as e:
                alertMsg = 'Signal Service failed (main block): '+ str(e)
                util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)
                
    else:
        alertMsg = 'Unable to connect admin trade account from signal service. There will be no signals generated for today until the issue is fixed.'
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='Y', programName=programName)    
        
    
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

    util.update_program_running_status(cnx, mySQLCursor,programName, 'INACTIVE')
    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', 'Program ended', telgUpdateFlag='N', programName=programName)    
    util.disconnect_db(cnx, mySQLCursor)