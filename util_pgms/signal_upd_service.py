from utils import util_functions as util
from utils import broker_api_functions as baf
import logging


def update_pending_signals(cnx, mySQLCursor, kite):
    selectStatment = "SELECT AUTO_ID, INSTRUMENT_TOKEN, TRIGGER_PRICE, TRIGGER_PRICE_LEVEL, SIGNAL_STATUS, TGT_PROFIT_PCT, TGT_STOP_LOSS_PCT FROM TRADE_SIGNALS WHERE SIGNAL_STATUS = 'PENDING'"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall() 
    currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
    
    updateOpenValues = []
    updatePendingValues = []
    updtOpenQuery = "UPDATE TRADE_SIGNALS SET UPDATED_ON = %s, SIGNAL_STATUS = %s, ENTRY_PRICE = %s, CURRENT_MKT_PRICE=%s, ENTRY_DATE=%s, \
                        TARGET_PRICE=%s, STOP_LOSS_PRICE=%s WHERE AUTO_ID= %s"
    updtPendingQuery = "UPDATE TRADE_SIGNALS SET UPDATED_ON = %s, CURRENT_MKT_PRICE=%s WHERE AUTO_ID= %s"                    
    tmpOpenCnt = 0
    tmpPendingCnt = 0

    for row in results:
        autoId = row[0]
        instToken = str(row[1])        
        triggerPrice = float(row[2])
        triggerPriceLevel = row[3]
        signalStatus = row[4]
        targetProfitPct = row[5]
        targetStoplossPct = row[6]
        ltpPrice = -1

        try:            
            try:
                getLTPData =  baf.get_ltp(kite, instToken)
                ltpPrice = getLTPData[instToken]['last_price']
                targetPrice = float(ltpPrice) * (1 + (float(targetProfitPct) / 100))
                stopLossPrice = float(ltpPrice) * (1 + (float(targetStoplossPct) / 100))
            except:
                pass
            
            if ( ltpPrice != -1 ):                
                if (signalStatus == 'PENDING'):                  
                    if (triggerPriceLevel == "BELOW" and triggerPrice >= float(ltpPrice)):
                        signalStatus = 'OPEN'                        
                    elif (triggerPriceLevel == "ABOVE" and  triggerPrice <= float(ltpPrice)):
                        signalStatus = 'OPEN'
                updateVal = []   
                
                if (signalStatus == 'OPEN'):  
                    updateVal.insert(0, str(currDateTime))                
                    updateVal.insert(1, str(signalStatus))
                    updateVal.insert(2, str(ltpPrice))                
                    updateVal.insert(3, str(ltpPrice))
                    updateVal.insert(4, str(currDateTime))
                    updateVal.insert(5, str(targetPrice)) 
                    updateVal.insert(6, str(stopLossPrice))
                    updateVal.insert(7, str(autoId)) 
 
                    updateOpenValues.insert(tmpOpenCnt, updateVal)
                    tmpOpenCnt += 1 
                else:
                    updateVal.insert(0, str(currDateTime))                
                    updateVal.insert(1, str(ltpPrice))
                    updateVal.insert(2, str(autoId))         
                    updatePendingValues.insert(tmpPendingCnt, updateVal)
                    tmpPendingCnt += 1 

                if (tmpOpenCnt == 10):
                    # insertMarketPerformance(cnx, mySQLCursor,insertVal)               
                    mySQLCursor.executemany(updtOpenQuery, updateOpenValues) 
                    cnx.commit()   
                    tmpOpenCnt = 0
                    updateOpenValues = []

                if (tmpPendingCnt == 10):
                    mySQLCursor.executemany(updtPendingQuery, updatePendingValues) 
                    cnx.commit()   
                    tmpPendingCnt = 0
                    updatePendingValues = []
                
        except Exception as e:    
            logging.info("Errored while updating the CMP details")
            logging.info(str(e))        
            pass
    
    # Update remaining open LTP prices
    if (tmpOpenCnt > 0):        
        mySQLCursor.executemany(updtOpenQuery, updateOpenValues) 
        cnx.commit()   

    # Update remaining pending LTP prices
    if (tmpPendingCnt > 0):
        mySQLCursor.executemany(updtPendingQuery, updatePendingValues) 
        cnx.commit()   
  

def update_open_signals(cnx, mySQLCursor, kite):
    selectStatment = "SELECT AUTO_ID, INSTRUMENT_TOKEN, ENTRY_PRICE, TARGET_PRICE, STOP_LOSS_PRICE, TGT_HORIZON, \
        DATE(ENTRY_DATE), TRIGGER_PRICE, SIGNAL_STATUS FROM TRADE_SIGNALS WHERE SIGNAL_STATUS = 'OPEN'"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()        
    signalResult = ''
    exitReason = ''
    exitPrice = 0
    currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
             
    exitDate = ""
    updateOpenValues = []
    updateExitedValues = []
    updtExitedQuery = "UPDATE TRADE_SIGNALS SET UPDATED_ON = %s, EXIT_REASON = %s, SIGNAL_STATUS = %s, SIGNAL_RESULT = %s, ACTUAL_HORIZON = %s, \
                        EXIT_DATE = %s, EXIT_PRICE = %s, CURRENT_MKT_PRICE=%s, PROFIT_PERCENT=%s, PROFIT_AMOUNT=%s  WHERE AUTO_ID= %s"
    updtOpenQuery = "UPDATE TRADE_SIGNALS SET UPDATED_ON = %s, ACTUAL_HORIZON = %s, CURRENT_MKT_PRICE=%s, PROFIT_PERCENT=%s, PROFIT_AMOUNT=%s  WHERE AUTO_ID= %s"
    tmpOpenCnt = 0 
    tmpExitedCnt = 0

    for row in results:
        autoId = row[0]
        instToken = str(row[1])
        entryPrice = float(row[2])
        targetPrice = float(row[3])     
        stopLossPrice = float(row[4])     
        horizon = int(row[5])
        entryDate = row[6]
        signalStatus = row[7]
        ltpPrice = -1

        try:            
            try:
                getLTPData =  baf.get_ltp(kite, instToken)
                ltpPrice = getLTPData[instToken]['last_price']
            except:
                pass
            
            if ( ltpPrice != -1 ):
                profitPercent, profitAmount = util.get_profit(float(entryPrice), float(ltpPrice), 1)
                exitPrice = ''
                exitDate = ''
                actualHorizon = util.get_date_difference(entryDate)                    
                if (float(ltpPrice) >= float(targetPrice)):                 
                    exitReason = 'TARGET HIT'
                    signalStatus = 'EXITED'
                    signalResult = 'GAIN'       
                    exitDate = currDateTime
                    exitPrice = ltpPrice                             

                elif (float(ltpPrice) <= float(stopLossPrice)): 
                    exitReason = 'STOPLOSS HIT'
                    signalStatus = 'EXITED'
                    signalResult = 'LOSS'
                    exitDate = currDateTime
                    exitPrice = ltpPrice               

                elif (int(actualHorizon) >= horizon):
                    exitReason = 'EXPIRED'
                    signalStatus = 'EXITED'                
                    exitDate = currDateTime
                    exitPrice = ltpPrice                             

                    if (float(profitAmount) > 0): 
                        signalResult = 'GAIN'
                    else:
                        signalResult = 'LOSS'

                updateVal = []
                if (signalStatus == 'EXITED'):  

                    updateVal.insert(0, str(currDateTime))
                    updateVal.insert(1, str(exitReason))
                    updateVal.insert(2, str(signalStatus))
                    updateVal.insert(3, str(signalResult))
                    updateVal.insert(4, str(actualHorizon))
                    updateVal.insert(5, str(exitDate))
                    updateVal.insert(6, str(exitPrice))
                    updateVal.insert(7, str(ltpPrice))
                    updateVal.insert(8, str(profitPercent))
                    updateVal.insert(9, str(profitAmount))                    
                    updateVal.insert(10, str(autoId))         
                    updateExitedValues.insert(tmpExitedCnt, updateVal)                    
                    tmpExitedCnt += 1                     

                else:
                    
                    updateVal.insert(0, str(currDateTime))
                    updateVal.insert(1, str(actualHorizon))                    
                    updateVal.insert(2, str(ltpPrice))
                    updateVal.insert(3, str(profitPercent))
                    updateVal.insert(4, str(profitAmount))                    
                    updateVal.insert(5, str(autoId))         
                    updateOpenValues.insert(tmpOpenCnt, updateVal)                    
                    tmpOpenCnt += 1   

                if (tmpOpenCnt == 10):                                 
                    mySQLCursor.executemany(updtOpenQuery, updateOpenValues) 
                    cnx.commit()   
                    tmpOpenCnt = 0
                    updateOpenValues = []

                if (tmpExitedCnt == 10):
                    mySQLCursor.executemany(updtExitedQuery, updateExitedValues) 
                    cnx.commit()   
                    tmpExitedCnt = 0
                    updateExitedValues = []
                
        except Exception as e:
            logging.info("Errored while updating the CMP details")
            logging.info(str(e))        
            pass
    
    # Update remaining open LTP prices
    if (tmpOpenCnt > 0):                                 
        mySQLCursor.executemany(updtOpenQuery, updateOpenValues) 
        cnx.commit()   

    # Update remaining exited LTP prices
    if (tmpExitedCnt > 0):
        mySQLCursor.executemany(updtExitedQuery, updateExitedValues) 
        cnx.commit()   

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

    programName = configDict['SIGNAL_SVC_UPD_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['SIGNAL_SVC_UPD_PGM_NAME']) + '.log')

    programExitFlag = 'N'    

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)
    
    adminTradeAccount = configDict['ADMIN_TRADE_ACCOUNT']

    # Connect to Kite ST
    kite, isKiteConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = configDict['ADMIN_TRADE_BROKER'])    

    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if (isKiteConnected):           
        
        alertMsg = 'The program (' + programName.replace('_','\_') + ')  started at ' + str(util.get_date_time_formatted("%d-%m-%Y %H:%M:%S"))
        
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)
        
        
        
        # Continuously run the program until the exit flag turns to Y
        while programExitFlag != 'Y': 
            try:
                # Verify whether the connection to MySQL database is open
                cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

                cycleStartTime = util.get_system_time()                 
                sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
                currentTime = util.get_date_time_formatted("%H%M")

                if (testingFlag or (int(currentTime) <= int(sysSettings['SYSTEM_END_TIME']))):       
                    try: 
                        update_open_signals(cnx, mySQLCursor, kite)
                    except Exception as e:
                        alertMsg = 'Exceptions occured in update_signals_data OPEN block: ' + str(e)                  
                        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    

                    try: 
                        update_pending_signals(cnx, mySQLCursor, kite)   
                    except Exception as e:
                        alertMsg = 'Exceptions occured in update_signals_data PENDING block: ' + str(e)                  
                        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    

                else:
                    alertMsg = 'System end time reached; exiting the program now'                                  
                    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    
                    programExitFlag = 'Y'
                
                util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
                util.disconnect_db(cnx, mySQLCursor)
            
            except Exception as e:
                alertMsg = 'Signal update service failed (main block): '+ str(e)
                util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)
        

    else:
        alertMsg = 'Unable to connect admin trade account from signal service. The singal records will not be updated for today until the issue is fixed.'
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='Y', programName=programName)    
        
    
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

    util.update_program_running_status(cnx, mySQLCursor,programName, 'INACTIVE')
    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', 'Program ended', telgUpdateFlag='N', programName=programName)    
    util.disconnect_db(cnx, mySQLCursor)
