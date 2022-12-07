from utils import util_functions as util
from utils import broker_api_functions as baf
import datetime
import numpy as np
import logging

def paper_trade_service(cnx, mySQLCursor, kite):    
    for row in util.get_open_signals(mySQLCursor):
        ptDataDict = {}        
        ptDataDict['strategyId'] = row[0]
        for row1 in util.get_open_signals(mySQLCursor, strategyId = ptDataDict['strategyId']): 
            ptDataDict['instrumentToken'] = str(row1[0])
            ptDataDict['stockName']  = row1[1]
            ptDataDict['tradeSymbol'] = row1[2]
            ptDataDict['triggerPrice'] = row1[3]                        
            ptDataDict['triggerDate'] = row1[4]
            ptDataDict['signalId']  = row1[5]
            ptDataDict['targetPrice'] = row1[6]
            ptDataDict['stopLossPrice'] = row1[7]
            ptDataDict['horizon'] = row1[8]
            ptDataDict['targetPercent'] = row1[9]
            ptDataDict['stopLossPercent'] = row1[10]
            getLTPData =  baf.get_ltp(kite, ptDataDict['instrumentToken'])            
            ptDataDict['lastPrice'] =getLTPData[ptDataDict['instrumentToken']]['last_price']

            usrStrategySettingList = util.get_usr_strategy_setttings(mySQLCursor, ptDataDict['strategyId'], accountType='PAPER') 
            for usrStrategySettings in usrStrategySettingList:                    
                ptDataDict['accountId'] =usrStrategySettings['TRADE_ACCOUNT']                
                # Check whether existing order is available; proceed to order only when no existing order present in mysql database
                orderExistFlag = util.check_existing_orders(mySQLCursor, ptDataDict['instrumentToken'], ptDataDict['strategyId'], ptDataDict['accountId'], accountType = 'PAPER')
                noOfDayPositions = util.get_no_of_day_positions(mySQLCursor, ptDataDict['strategyId'], ptDataDict['accountId'], accountType = 'PAPER')                    
                noOfTotalPositions = util.get_total_no_of_positions(mySQLCursor, ptDataDict['strategyId'], ptDataDict['accountId'], accountType = 'PAPER')
                
                ptDataDict['quantity'] =util.calc_quantity(ptDataDict['lastPrice'], usrStrategySettings['POSITION_SIZE'])

                # Check the number of active GTT orders; if it exceeds the limit provided in mysql trade variables, do not place new gtt order
                if (not(orderExistFlag) and ptDataDict['quantity'] > 0 and noOfDayPositions < int(usrStrategySettings['DAY_POSITION_LIMIT']) and \
                        noOfTotalPositions < int(usrStrategySettings['TOTAL_POSITION_LIMIT'])  and usrStrategySettings['STRATEGY_ON_OFF_FLAG'] == 'ON'):                        
                    # Connect to Kite ST                       
                    ptDataDict['buyValue'] = int(ptDataDict['quantity']) * float(ptDataDict['triggerPrice'])                                                                                
                    ptDataDict['transactionType'] = 'BUY'
                    ptDataDict['orderStatus'] = 'ACTIVE'
                    util.insert_paper_trades(cnx, mySQLCursor, ptDataDict)
                    util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'PAPER TRADE', 'A new stock has been added to paper trade', userName=ptDataDict['accountId'], programName=programName, strategyId=ptDataDict['strategyId'], stockName=ptDataDict['stockName'])
                else:
                    if (noOfDayPositions > int(usrStrategySettings['DAY_POSITION_LIMIT'])):
                        alertMsg = "Day Position Limit is exceeded. Can't buy " + str(ptDataDict['stockName'])
                        util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=ptDataDict['accountId'], programName=programName, strategyId=ptDataDict['strategyId'], stockName=str(ptDataDict['stockName']))
                    elif (noOfTotalPositions > int(usrStrategySettings['TOTAL_POSITION_LIMIT'])):
                        alertMsg = "Total Position Limit is exceeded. Can't buy " + str(ptDataDict['stockName'])
                        util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=ptDataDict['accountId'], programName=programName, strategyId=ptDataDict['strategyId'], stockName=str(ptDataDict['stockName']))

                    

def update_paper_trading(cnx, mySQLCursor, kite):
    selectStatment = "SELECT AUTO_ID, INSTRUMENT_TOKEN, BUY_ORDER_PRICE, TARGET_PRICE, STOP_LOSS_PRICE, HORIZON, DATE(BUY_ORDER_DATE), QUANTITY FROM PAPER_TRADE_TRANSACTIONS WHERE ORDER_STATUS = 'ACTIVE'"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()        
    signalResult = ''
    exitReason = ''
    exitPrice = 0
    currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
    updateArrayValues = []
    tmpCnt = 0    
    exitDate = ""
    # updateQuery = "UPDATE TRADE_SIGNALS SET UPDATED_ON = '"+ str(currDateTime) +"', EXIT_REASON = '"+ str(exitReason) +"', SIGNAL_STATUS = '"+ signalStatus +"', SIGNAL_RESULT = '"+ str(signalResult) +"', ACTUAL_HORIZON = "+ str(actualHorizon) +", EXIT_DATE = '"+str(currDateTime)+"', EXIT_PRICE ="+str(ltpPrice)+", CURRENT_MKT_PRICE="+str(ltpPrice)+", PROFIT_PERCENT="+str(profitPercent)+", PROFIT_AMOUNT="+str(profitAmount)+" WHERE AUTO_ID="+str(autoId)
    updateQuery = "UPDATE PAPER_TRADE_TRANSACTIONS SET UPDATED_ON = %s, EXIT_REASON = %s, ORDER_STATUS = %s, TRADE_RESULT = %s, ACTUAL_HORIZON = %s, SELL_ORDER_DATE = %s, SELL_ORDER_PRICE = %s, CURRENT_MKT_PRICE=%s, PROFIT_PERCENT=%s, PROFIT_AMOUNT=%s WHERE AUTO_ID= %s"

    for row in results:
        autoId = row[0]
        instToken = str(row[1])
        entryPrice = float(row[2])
        targetPrice = float(row[3])     
        stopLossPrice = float(row[4])     
        horizon = int(row[5])
        entryDate = row[6]
        quantity = float(row[7])
        signalStatus = 'ACTIVE'
        signalResult = ''

        try:            
            getLTPData =  baf.get_ltp(kite, instToken)
            ltpPrice = getLTPData[instToken]['last_price']
            profitPercent, profitAmount = util.get_profit(float(entryPrice), float(ltpPrice), quantity)
            actualHorizon = util.get_date_difference(entryDate)
            exitDate = currDateTime
            exitPrice = ltpPrice
            
            if (float(ltpPrice) >= float(targetPrice)): 
                exitReason = 'TARGET HIT'
                signalStatus = 'EXITED'
                signalResult = 'GAIN'
                
            elif (float(ltpPrice) <= float(stopLossPrice)): 
                exitReason = 'STOPLOSS HIT'
                signalStatus = 'EXITED'
                signalResult = 'LOSS'
                
            elif (int(actualHorizon) >= horizon): 
                exitReason = 'EXPIRED'
                signalStatus = 'EXITED'                
                if (float(profitAmount) > 0): 
                    signalResult = 'GAIN'
                else:
                    signalResult = 'LOSS'
            else:
                exitPrice = ''
                exitDate = ''

            updateVal = []
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
            updateArrayValues.insert(tmpCnt, updateVal)
            
            tmpCnt += 1  
            if (tmpCnt == 2):
                # insertMarketPerformance(cnx, mySQLCursor,insertVal)               
                mySQLCursor.executemany(updateQuery, updateArrayValues) 
                cnx.commit()   
                tmpCnt = 0   
            
        except Exception as e:    
            util.logger(logEnableFlag, 'info', "Errored while updating the CMP details:" + str(e)) 
            pass
    
    if (tmpCnt > 0):       
        mySQLCursor.executemany(updateQuery, updateArrayValues)
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

    programName = configDict['PAPER_TRADE_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['PAPER_TRADE_PGM_NAME']) + '.log')

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
                cycleStartTime = util.get_system_time()                 
                # Verify whether the connection to MySQL database is open
                cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

                sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
                currentTime = util.get_date_time_formatted("%H%M")

                if (testingFlag or (int(currentTime) <= int(sysSettings['SYSTEM_END_TIME']))):
                    
                    try: 
                        paper_trade_service(cnx, mySQLCursor, kite)
                    except Exception as e:
                        alertMsg = 'Error occured in paper_trade_service block: ' + str(e)                  
                        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='N', programName=programName)    
                    
                    try: 
                        update_paper_trading(cnx, mySQLCursor, kite)    
                    except Exception as e:
                        alertMsg = 'Exceptions occured in update_paper_trading block: ' + str(e)                  
                        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='N', programName=programName)    
                                
                    
                    
                elif (int(currentTime) > int(sysSettings['SYSTEM_END_TIME'])):
                    alertMsg = 'System end time reached; exiting the program now'                  
                    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    
                    programExitFlag = 'Y'
                
                util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
                util.disconnect_db(cnx, mySQLCursor)
            except Exception as e:
                alertMsg = 'Paper trade service failed (main block): '+ str(e)
                util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='N', programName=programName)
                
    else:
        alertMsg = 'Unable to connect admin trade account from signal service. The singal records will not be updated for today until the issue is fixed.'
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='Y', programName=programName)    
        

    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

    util.update_program_running_status(cnx, mySQLCursor,programName, 'INACTIVE')
    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', 'Program ended', telgUpdateFlag='N', programName=programName)    
    util.disconnect_db(cnx, mySQLCursor)

