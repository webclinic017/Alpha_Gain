from utils import util_functions as util
from utils import broker_api_functions as baf
import datetime
import requests
import logging
import re

def get_margin_data(brokerApi, broker, accountId):
    if (broker == 'ZERODHA'):
        marginData = baf.getKiteMargins(brokerApi)    
        availableCash =  float(marginData['available']['cash'])
        totalCashValue = float(marginData['net']) + float(marginData['utilised']['delivery'])
        usedMargin = float(marginData['utilised']['debits'])
        availableMargin =  float(marginData['net'])
        return availableCash, totalCashValue, usedMargin, availableMargin
    elif (broker == 'PROSTOCKS'):

        data = "jData={\"uid\":\""+str(accountId)+"\", \"actid\":\""+str(accountId)+"\"}&jKey=" + brokerApi
        response = requests.post('https://starapi.prostocks.com/NorenWClientTP/Limits', data=data)
        marginData = response.json()

        if (marginData['stat'] == 'Ok'):
            availableCash =  float(marginData['cash'])
            totalCashValue = float(marginData['cash'])
            usedMargin = 0
            if 'marginused' in marginData:
                usedMargin = float(marginData['marginused'])
            availableMargin =  availableCash - usedMargin

            return availableCash, totalCashValue, usedMargin, availableMargin
        else:
            return -1, -1, -1, -1

def update_cash_positions(cnx, mySQLCursor, tradeAccount, availableCash, totalCashValue, usedMargin, availableMargin):
    try:
        updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")        
        updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET AVAILABLE_MARGIN='"+str(availableMargin)+"', AVAILABLE_CASH='"+str(availableCash)+"', \
            USED_MARGIN='"+str(usedMargin)+"', TOTAL_CASH_VALUE='"+str(totalCashValue)+"', UPDATED_ON= '"+str(updatedOn)+"' WHERE TRADE_ACCOUNT='"+str(tradeAccount)+"'")

        mySQLCursor.execute(updateQuery)
        cnx.commit()
    except Exception as e:    
        logging.info("Errored while updating the update_cash_positions details: " + str(e))
        pass

def update_single_db_column(cnx, mySQLCursor, tradeAccount, tableName, columnName, columnValue):
    try:
        updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        updateQuery = ("UPDATE "+str(tableName)+" SET "+str(columnName)+"='"+str(columnValue)+"', UPDATED_ON= '"+str(updatedOn)+"' WHERE TRADE_ACCOUNT='"+str(tradeAccount)+"'")
        mySQLCursor.execute(updateQuery)
        cnx.commit()
    except Exception as e:    
        logging.info("Errored while updating the update_single_db_column details")
        logging.info(str(e))        
        pass

def update_holdings_cmp(kite, cnx, mySQLCursor):
    selectStatment = "SELECT INSTRUMENT_TOKEN, BUY_ORDER_PRICE, QUANTITY FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'OPEN'"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()        
    
    for row in results:
        instToken = str(row[0])
        buyPrice = row[1]
        quantity = row[2]        
        try:
            getLTPData =  baf.getLTP(kite, instToken)
            ltpPrice = getLTPData[instToken]['last_price']
            profitPercent, profitAmount = util.get_profit(float(buyPrice), float(ltpPrice), int(quantity))
            updateQuery = ("UPDATE TRADE_TRANSACTIONS SET CURRENT_MKT_PRICE="+str(ltpPrice)+", PROFIT_PERCENT="+str(profitPercent)+", PROFIT_AMOUNT="+str(profitAmount)+" WHERE INSTRUMENT_TOKEN='"+instToken+"' AND TRADE_STATUS IN ('OPEN')")
            mySQLCursor.execute(updateQuery)
            cnx.commit()            
            
        except Exception as e:    
            logging.info("Errored while updating the CMP details")
            logging.info(str(e))        
            pass

def update_order_status(kite, accountId, cnx, mySQLCursor):
    response = {}
    try: 
        orderData = baf.getKiteOrders(kite)
        print(orderData)
        for orders in orderData:

            transactionType = orders["transaction_type"]
            updateOn = orders["exchange_update_timestamp"]
            try:
                if (updateOn is None or updateOn == 'None'):
                    updateOn = int(orders["order_timestamp"]['$date']) / 1000
                    updateOn = datetime.datetime.utcfromtimestamp(updateOn).strftime("%Y-%m-%d %H:%M:%S")
            except:
                updateOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S") 

                
            orderId = orders["order_id"]
            orderPrice = orders["average_price"]
            filledQuantity = int(orders['filled_quantity'])
            pendingQuantity = int(orders['pending_quantity'])

            orderStatus = orders["status"]            
            orderRemarks = orders["status_message"]
            quantity = int(orders["quantity"])

            buyValue = quantity * float(orderPrice)
            updateQuery = ""

            if (transactionType == 'BUY'):
                if (orderStatus == 'COMPLETE'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'OPEN' AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount
                    if rowCount == 0:
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET BUY_ORDER_PRICE="+str(orderPrice) + ", TRADE_STATUS='OPEN', QUANTITY="+str(quantity)+", BUY_ORDER_STATUS='COMPLETE', BUY_ORDER_DATE='" +
                                str(updateOn)+"', BUY_VALUE='"+str(buyValue)+"' WHERE BUY_ORDER_ID='"+str(orderId) + "' AND TRADE_STATUS NOT IN ('OPEN','EXITED')")


                elif (orderStatus == 'OPEN' and filledQuantity > 0 and pendingQuantity > 0):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'P-OPEN' AND QUANTITY="+str(filledQuantity)+" AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:
                        buyValue = filledQuantity * float(orderPrice)                
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET ORDER_REMARKS ='PARTIALLY EXECUTED', BUY_ORDER_PRICE="+str(orderPrice) + ", TRADE_STATUS='P-OPEN', BUY_ORDER_STATUS='P-COMPLETE', \
                            BUY_ORDER_DATE='" +str(updateOn)+ "', BUY_VALUE='"+str(buyValue)+"', QUANTITY="+str(filledQuantity)+" WHERE BUY_ORDER_ID='"+str(orderId) + "'")       

                elif (orderStatus == 'CANCELLED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'CANCELLED' AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:                        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET ORDER_REMARKS ='Buy Order is cancelled by either Zerotha or User', BUY_ORDER_STATUS='CANCELLED', TRADE_STATUS='CANCELLED' WHERE BUY_ORDER_ID='"+str(orderId) + "'")
                
                elif (orderStatus == 'REJECTED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'REJECTED' AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:                        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET TRADE_STATUS='REJECTED', BUY_ORDER_STATUS='REJECTED', ORDER_REMARKS ='" +
                                                    str(orderRemarks) + "' WHERE BUY_ORDER_ID='"+str(orderId) + "'")


            elif(transactionType == 'SELL'):
                if (orderStatus == 'COMPLETE'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'EXITED' AND SELL_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                
                
                    if rowCount == 0:
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_PRICE="+str(orderPrice) + ", SELL_ORDER_STATUS='COMPLETE', TRADE_STATUS='EXITED', SELL_ORDER_DATE='"+str(updateOn)+"', PROFIT_PERCENT=(("+str(float(orderPrice))+" - BUY_ORDER_PRICE) / BUY_ORDER_PRICE) * 100, \
                            PROFIT_AMOUNT=("+str(float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY), TRADE_RESULT = IF ((("+str(float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY)) > 0, 'GAIN','LOSS') WHERE SELL_ORDER_ID ='"+str(orderId)+"'")

                elif(orderStatus == 'REJECTED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE SELL_ORDER_STATUS = 'REJECTED' AND SELL_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:                        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_STATUS = 'REJECTED', SELL_ORDER_DATE='"+str(updateOn)+"' WHERE SELL_ORDER_ID ='"+str(orderId)+"'")
                
                elif (orderStatus == 'CANCELLED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE SELL_ORDER_STATUS = 'CANCELLED' AND SELL_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:                        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_REMARKS ='Sell Order is cancelled by either Zerotha or User', SELL_ORDER_STATUS='CANCELLED' WHERE SELL_ORDER_ID='"+str(orderId) + "'")
                

            if (updateQuery != ""):
                mySQLCursor.execute(updateQuery)
                cnx.commit()

        response['status'] = 'success'
        response['remarks'] = 'Successfully updated the orders from Zerodha for account: ' + str(accountId)
            
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to complete the orders update for Zerodha for account: ' + str(accountId)
    
    return response

def update_order_status_prostocks(accessToken, cnx, mySQLCursor):
    data = "jData={\"uid\":\"R0428\", \"prd\":\"C\"}&jKey=" + accessToken
    response = requests.post('https://starapi.prostocks.com/NorenWClientTP/OrderBook', data=data)
    orderData = response.json()

    updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")   
    orderRemarks = ''     
    if (isinstance(orderData, list) and len(orderData) > 0 and orderData[0]['stat'] == 'Ok'):
        for orders in orderData:
            try:
                product = orders["prd"]
                transactionType = orders["trantype"]
                
                orderId = orders["norenordno"]
                orderPrice = orders["prc"]
                exch = orders["exch"]
                tradeSymbol = orders["tsym"]
                orderStatus = orders["status"] 

                if (orderStatus == 'COMPLETE'):
                    orderPrice = orders["avgprc"]
                elif (orderStatus == 'REJECTED'):
                    orderRemarks = orders["rejreason"]
                elif (orderStatus == 'CANCELED'):
                    orderRemarks = 'CANCELED'

                quantity = orders["qty"]
                buyValue = float(quantity) * float(orderPrice)
                updateQuery = ""

                if( transactionType == 'B' ):

                    if(orderStatus == 'COMPLETE'):
                        selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'OPEN' AND BUY_ORDER_STATUS='COMPLETE' AND BUY_ORDER_ID='"+str(orderId) + "'"
                        mySQLCursor.execute(selectStatment)
                        rowCount = mySQLCursor.rowcount
                        if rowCount == 0:        
                            updateQuery = ("UPDATE TRADE_TRANSACTIONS SET BUY_ORDER_PRICE="+str(orderPrice) + ", TRADE_STATUS='OPEN', BUY_ORDER_STATUS='COMPLETE', BUY_ORDER_DATE='" + str(updatedOn)+"', BUY_VALUE="+str(buyValue)+" WHERE BUY_ORDER_ID='"+str(orderId) + "'")


                    elif( orderStatus == 'REJECTED'):
                        selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'REJECTED' AND BUY_ORDER_STATUS='REJECTED' AND BUY_ORDER_ID='"+str(orderId)+"'"
                        mySQLCursor.execute(selectStatment)
                        rowCount = mySQLCursor.rowcount
                            
                        if rowCount == 0:        
                            updateQuery = ("UPDATE TRADE_TRANSACTIONS SET TRADE_STATUS='REJECTED', BUY_ORDER_STATUS='REJECTED', ORDER_REMARKS ='" + str(orderRemarks) + "' WHERE BUY_ORDER_ID='"+str(orderId) + "'")


                    elif( orderStatus == 'CANCELED'):
                            selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'CANCELED' AND BUY_ORDER_STATUS='CANCELED' AND BUY_ORDER_ID='"+str(orderId)+"'"
                            mySQLCursor.execute(selectStatment)
                            rowCount = mySQLCursor.rowcount                           
                            
                            if rowCount == 0:        
                                updateQuery = ("UPDATE TRADE_TRANSACTIONS SET TRADE_STATUS='CANCELED', BUY_ORDER_STATUS='CANCELED', ORDER_REMARKS ='" +
                                        str(orderRemarks) + "' WHERE BUY_ORDER_ID='"+str(orderId) + "'")
  
                    
                if (transactionType == 'S'):           

                    if(orderStatus == 'COMPLETE'):
                        selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE SELL_ORDER_STATUS = 'COMPLETE' AND TRADE_STATUS='EXITED' AND SELL_ORDER_ID='"+str(orderId)+"'"
                        mySQLCursor.execute(selectStatment)
                        rowCount = mySQLCursor.rowcount     

                        if rowCount == 0: 
                            updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_ID ='"+str(orderId) + "',  SELL_ORDER_PRICE="+str(orderPrice) + ", SELL_ORDER_STATUS='COMPLETE', TRADE_STATUS='EXITED', SELL_ORDER_DATE='"+str(updatedOn)+"', PROFIT_PERCENT=(("+str(float(orderPrice))+" - BUY_ORDER_PRICE) / BUY_ORDER_PRICE) * 100, PROFIT_AMOUNT=("+str(
                                    float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY), TRADE_RESULT = IF ((("+str(float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY)) > 0, 'GAIN','LOSS') WHERE SELL_ORDER_ID ='"+str(orderId)+"' AND TRADE_STATUS IN ('OPEN')")

                    elif (orderStatus == 'REJECTED'):
                        selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE SELL_ORDER_STATUS = 'ABANDONED' AND SELL_ORDER_ID='"+str(orderId)+"'"
                        mySQLCursor.execute(selectStatment)
                        rowCount = mySQLCursor.rowcount

                        if rowCount == 0: 
                            if (re.search('Check Holdings Including BTST', str(orderRemarks)) or re.search('NRO sqroff not allowed', str(orderRemarks))):
                                updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_STATUS='ABANDONED', SELL_ORDER_REMARKS ='" +
                                                str(orderRemarks) + "' WHERE SELL_ORDER_ID='"+str(orderId) + "' AND TRADE_STATUS NOT IN ('ABANDONED')")
                            else:
                                updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_STATUS='REJECTED', SELL_ORDER_REMARKS ='" +
                                                str(orderRemarks) + "' WHERE SELL_ORDER_ID='"+str(orderId) + "' AND TRADE_STATUS NOT IN ('REJECTED')")
                            

                if (updateQuery != ""):
                    mySQLCursor.execute(updateQuery)
                    cnx.commit()
                    
            except Exception as e:
                logging.info("Errored while updating the order status: " + str(e))            
                pass

# Not used - to be removed
def update_gtt_order_status(kite, cnx, mySQLCursor, currentDateTime, tableName):
    gttOrders = baf.getKiteGTTOrders(kite)
    for gttData in gttOrders:
        try:
            gttStatus = gttData['status']
            tradingSymbol = gttData['orders'][0]['tradingsymbol']
            triggerId = gttData['id']
            if (gttStatus == 'active'):
                instrumentToken = str(gttData['condition']['instrument_token'])
                getLTPData =  baf.getLTP(kite, instrumentToken)
                lastPrice = getLTPData[instrumentToken]['last_price']
                orderDate = str(gttData['updated_at'])
                orderDate = datetime.datetime.strptime(str(orderDate), '%Y-%m-%d %H:%M:%S')
                currentDateTime = datetime.datetime.strptime(str(currentDateTime), '%Y-%m-%d %H:%M:%S')
                diff = currentDateTime - orderDate
                orderTimeDiff = diff.total_seconds()/3600
                triggerValue = float(gttData['condition']['trigger_values'][0])
                distanceFromTrigger = float(((lastPrice / triggerValue) - 1) * 100)

                if(float(orderTimeDiff) >= 24):                    
                    logging.info("Trading Symbol: " + tradingSymbol)
                    logging.info("This is open for " + str(orderTimeDiff) + " , so deleting it")
                    baf.deleteGTTOrder(kite, triggerId)
                    updateQuery = ("UPDATE TRADE_TRANSACTIONS SET TRADE_STATUS='DELETED', ORDER_REMARKS='Crossed 24 hours' WHERE TRIGGER_ID='" +str(triggerId)+"' AND TRADE_STATUS IN ('ACTIVE')")
                    
                    mySQLCursor.execute(updateQuery)
                    cnx.commit()
                elif((distanceFromTrigger >= 5 or distanceFromTrigger <= -5)):
                    logging.info("Trading Symbol: " + tradingSymbol)
                    logging.info(
                        "Distance from Trigger is exceeded 3%, so delete it")
                    baf.deleteGTTOrder(kite, triggerId)
                    updateQuery = ("UPDATE TRADE_TRANSACTIONS SET TRADE_STATUS='DELETED', ORDER_REMARKS='Distance from Trigger is exceeded 3%' WHERE TRIGGER_ID='" +str(triggerId)+"' AND TRADE_STATUS IN ('ACTIVE')")

                    mySQLCursor.execute(updateQuery)
                    cnx.commit()
            elif (gttStatus == 'triggered'):

                orderId = gttData['orders'][0]['result']['order_result']['order_id']
                updateQuery = ("UPDATE TRADE_TRANSACTIONS SET BUY_ORDER_ID='"+str(orderId) +"',TRADE_STATUS='TRIGGERED' WHERE TRIGGER_ID='"+str(triggerId)+"' AND TRADE_STATUS IN ('ACTIVE')")
                mySQLCursor.execute(updateQuery)
                rowCount = mySQLCursor.rowcount
                
                if rowCount != 0:        
                    alertMsg= tradingSymbol + ' is triggered at ' + str(triggerValue)
                    # util.send_alerts(cnx, mySQLCursor, 'ST', TELG_ADMIN_ID, 'INFO', alertMsg, 'Y', 'Y')

                cnx.commit()
	
        except Exception as e:
            logging.info("Errored while updating the GTT order status")
            logging.info(str(e))
            pass


def update_indice_prices(cnx, mySQLCursor, kite):
    response = {}
    try:        
        insertQuery = "UPDATE LAST_TRADED_INDICES_PRICE SET CLOSE_PRICE=%s, UPDATED_ON=%s WHERE INSTRUMENT_TOKEN=%s"

        # Fetch list of all instruments from stock universe for the provided category
        selectStatment = "SELECT INSTRUMENT_TOKEN, TRADINGSYMBOL FROM STOCK_UNIVERSE WHERE CATEGORY='Market Cap'"
        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()    
        
        insertArrayValues = []

        tmpCnt = 0
        for row in results:
            instrumentToken = row[0]       
            
            # Get the last traded price for the insturments given
            ltpData =  baf.get_ltp(kite, instrumentToken)
            closePrice = ltpData[str(instrumentToken)]['last_price']
          
            updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")

            insertVal = []
            insertVal.insert(0, str(closePrice))
            insertVal.insert(1, str(updatedOn))
            insertVal.insert(2, str(instrumentToken))

        
            insertArrayValues.insert(tmpCnt, insertVal)
            tmpCnt += 1

        if (tmpCnt > 0):                        
            mySQLCursor.executemany(insertQuery, insertArrayValues)
            cnx.commit()                  
        response['status'] = 'success'
        response['remarks'] = 'Market Cap price records are updated'

    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to update indice price records. The error is, ' + str(e)

    return response




def update_existing_portfolio(cnx, mySQLCursor, kite):
    try:
        selectStatment = "SELECT AUTO_ID, INSTRUMENT_TOKEN, BUY_ORDER_PRICE, BUY_ORDER_DATE, QUANTITY, PREV_CLOSE_PRICE FROM MANUAL_PORTFOLIO"
    
        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()                
        currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
        updateArrayValues = []
        tmpCnt = 0    
        updateQuery = ""

        for row in results:
            autoId = row[0]
            instToken = str(row[1])
            entryPrice = row[2]
            entryDate = row[3]
            quantity = row[4]        
            prevClosePrice = row[5]
            dayChageprofitPercent = 0
            dayChangeprofitAmount = 0
                    
            getLTPData =  baf.get_ltp(kite, instToken)
            ltpPrice = getLTPData[str(instToken)]['last_price']
            # get profit for given qty
            profitPercent, profitAmount = util.get_profit(float(entryPrice), float(ltpPrice), quantity)
            if (float(prevClosePrice) == 0):
                dayChageprofitPercent, dayChangeprofitAmount = util.get_profit(float(entryPrice), float(ltpPrice), quantity)
            else:
                dayChageprofitPercent, dayChangeprofitAmount = util.get_profit(float(prevClosePrice), float(ltpPrice), quantity)
            
            actualHorizon = 0
            if (entryDate is not None and entryDate != ''):
                actualHorizon = util.get_date_difference(entryDate)
          

            updateQuery = "UPDATE MANUAL_PORTFOLIO SET UPDATED_ON = %s, CURRENT_MKT_PRICE=%s, PROFIT_PERCENT=%s, PROFIT_AMOUNT=%s, \
                ACTUAL_HORIZON=%s, TODAY_PROFIT_PCT=%s, TODAY_PROFIT=%s WHERE AUTO_ID= %s"
            updateVal = []
            updateVal.insert(0, str(currDateTime))
            updateVal.insert(1, str(ltpPrice))
            updateVal.insert(2, str(profitPercent))
            updateVal.insert(3, str(profitAmount))                       
            updateVal.insert(4, str(actualHorizon))
            updateVal.insert(5, str(dayChageprofitPercent))
            updateVal.insert(6, str(dayChangeprofitAmount))
            updateVal.insert(7, str(autoId))
               
            updateArrayValues.insert(tmpCnt, updateVal)

            tmpCnt += 1  
            if (tmpCnt == 20):                
                mySQLCursor.executemany(updateQuery, updateArrayValues) 
                cnx.commit()   
                tmpCnt = 0   
                updateArrayValues = []


    
        if (tmpCnt > 0):       
            mySQLCursor.executemany(updateQuery, updateArrayValues)
            cnx.commit()    

        response['status'] = 'success'        

    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to update / complete the existing portfoli0 records. The error is, ' + str(e)

    return response


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

    programName = configDict['LIVE_TRADES_UPD_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['LIVE_TRADES_UPD_PGM_NAME']) + '.log')

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

        try:
                # Verify whether the connection to MySQL database is open
                cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

                cycleStartTime = util.get_system_time()                 
                sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
                currentTime = util.get_date_time_formatted("%H%M")
                data = "jData={\"uid\":\""+str('R0428')+"\", \"exch\":\""+str('NSE')+"\",\"token\":\""+str('5633')+"\",\"st\":\""+str('1600000000')+"\",\"et\":\""+str('1634279348')+"\"}&jKey=cbfafb3c90bf3b64ac46edd322abaeee81cf81e4408442fa2ee0ec24794b6975"
                response = requests.post('https://starapi.prostocks.com/NorenWClientTP/TPSeries', data=data)
                f = open("demofile3.txt", "w")
                f.write(response.text)
                f.close()
                # marginData = response.json()
                # print(marginData)
                
            
        except Exception as e:
            print('Live trade update service failed (main block): '+ str(e))

    util.disconnect_db(cnx, mySQLCursor)
    