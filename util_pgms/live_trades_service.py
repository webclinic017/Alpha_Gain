import logging
from utils import util_functions as util
from utils import broker_api_functions as baf
import re



def get_futures_instruments(mySQLCursor, tradeSymbol):
    expiryMonth = util.get_fno_expiry_month() 
    
    instSearchString = str(tradeSymbol) + str(expiryMonth)
    
    selectStatment = "SELECT tradingsymbol, instrument_token, lot_size, expiry FROM INSTRUMENTS WHERE name = '"+str(tradeSymbol)+"' AND tradingsymbol LIKE '" + instSearchString + "%' AND exchange='NFO' AND instrument_type='FUT' order by expiry limit 1"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    futInstName = ''
    futInstToken = ''
    lotSize = 0
    expDate = ''

    for row in results:
        futInstName = row[0]
        futInstToken = row[1]
        lotSize = row[2]
        expDate = str(row[3].strftime("%d%b%y")).upper()

    return futInstName, futInstToken, lotSize, expDate

def get_options_instruments(mySQLCursor, tradeSymbol, orderType):    
    if (orderType != 'CALL'):
        orderType = 'PUT'

    selectStatment = "SELECT OPTION_INSTRUMENT, OPTION_INSTRUMENT_TOKEN, LOT_SIZE, EXPIRY_DATE, PUT_OPTION_STRIKE, OPTION_VALUE, DELTA \
        FROM CASH_SECURED_PUT_WATCHLIST WHERE TRADE_SYMBOL='"+str(tradeSymbol)+"' AND REPORT_DATE='"+currDate+"' AND OPTION_TYPE='"+orderType+"' LIMIT 1"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    lotSize = 0
    expDate = ''
    strikePrice = 0
    optionsInstName = ""
    optionsInstToken = ""
    optionsBuyValue = 0
    deltaValue = 0

    for row in results:
        optionsInstName = row[0]
        optionsInstToken = row[1]
        lotSize = row[2]
        expDate = str(row[3].strftime("%d%b%y")).upper()
        strikePrice = row[4]
        optionsBuyValue = row[5]
        deltaValue = lotSize * strikePrice * row[6]

    return optionsInstName, optionsInstToken, lotSize, expDate, strikePrice, optionsBuyValue, deltaValue

# Call the broker api to get market depth and calculate the last price and trigger price for given instruments. 

def get_options_price_data(brokerAPI, optionsInstToken):
    optionsTriggerPrice = 0
    optionsLastPrice = 0
    try: 

        quoteData = baf.get_quote(brokerAPI, optionsInstToken)        
        optionsLastPrice = quoteData[optionsInstToken]['last_price']                        
        fnoBuyPrice = quoteData[optionsInstToken]['depth']['buy'][0]['price']
        fnoSellPrice = quoteData[optionsInstToken]['depth']['sell'][0]['price']
        
        if ((float(fnoBuyPrice) + float(fnoSellPrice)) == 0):
            optionsTriggerPrice = ("%.1f" % float(optionsLastPrice))
        else:
            optionsTriggerPrice = ("%.1f" % float((float(fnoBuyPrice) + float(fnoSellPrice)) / 2))

    except Exception as e:
        logging.info("Unable to get the options price data from broker API")

    return optionsLastPrice, optionsTriggerPrice



def place_future_order(kite, cnx, mySQLCursor, brokerAPI, tradeDataDict):

    # send the order to the corresponding account's broker trading platform for the leg 2                                      
    orderId, orderRemarks  = baf.place_future_buy_order(brokerAPI, tradeDataDict)
    tradeDataDict['orderId']=orderId
    tradeDataDict['orderRemarks']=orderRemarks
    tradeDataDict['exchange']='NFO'
    # insert the order details to the MySQL trade transactions table for the leg 1
    if (int(orderId) > 0):                                     
        util.insert_future_order_details(cnx, mySQLCursor, tradeDataDict, tradeDataDict['orderType'])
    
    return orderId

def place_options_order(kite, cnx, mySQLCursor, brokerAPI, tradeDataDict):    
    tradeDataDict['optionsLastPrice'],tradeDataDict['optionsTriggerPrice'] = get_options_price_data(kite, tradeDataDict['optionsInstToken'])                                   
    
    # send the order to the corresponding account's broker trading platform for the leg 1 
    orderId, orderRemarks  = baf.place_options_buy_order(brokerAPI, tradeDataDict)

    tradeDataDict['orderId']=orderId
    tradeDataDict['orderRemarks']=orderRemarks
    tradeDataDict['exchange']='NFO'

    # insert the order details to the MySQL trade transactions table for the leg 1
    if (int(orderId) > 0):                                          
        util.insert_options_order_details(cnx, mySQLCursor, tradeDataDict, tradeDataDict['orderType'])
    
    return orderId

def place_cash_order(cnx, mySQLCursor, brokerAPI, tradeDataDict):

    orderId, orderRemarks = baf.place_cash_buy_order(brokerAPI, tradeDataDict)                                        
    tradeDataDict['orderId']=orderId
    tradeDataDict['orderRemarks']=orderRemarks

    tradeDataDict['orderStatus']='PENDING'
    tradeDataDict['exchange']='NSE'

    if (int(orderId) > 0):                                     
        util.insert_cash_order_details(cnx, mySQLCursor, tradeDataDict, tradeDataDict['orderType'])  

    elif (re.search('Buying is restricted', str(orderRemarks))):
        updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        updateQuery = ("UPDATE TRADE_SIGNALS SET SIGNAL_STATUS='BUY RESTRICTED', UPDATED_ON='"+str(updatedOn)+"' WHERE SIGNAL_ID='"+str(tradeDataDict['signalId'])+"'")
        mySQLCursor.execute(updateQuery)
        cnx.commit()  
    
    return orderId

def is_account_under_limit (mySQLCursor, usrStrategySettings, tradeDataDict):
    strategyId = tradeDataDict['strategyId']
    accountId = usrStrategySettings['TRADE_ACCOUNT']                                    
    instrumentToken = tradeDataDict['instrumentToken']
    availableMargin = float(usrStrategySettings['AVAILABLE_MARGIN'])
    capitalAllocation = float(usrStrategySettings['CAPITAL_ALLOCATION'])
    orderType = usrStrategySettings['PRODUCT_ORDER_TYPE']
    perIndustryCapLimitPct = usrStrategySettings['PER_INDUSTRY_CAP_LIMIT_PCT']
    utilizeCaptial = util.get_utilized_capital(mySQLCursor, accountId, strategyId=strategyId)

    isUtilzedCapitalLimited =  utilizeCaptial <= capitalAllocation

    limitExeededFlag = False

    if (isUtilzedCapitalLimited):
        noOfTotalPositions = util.get_total_no_of_positions(mySQLCursor, strategyId, accountId)

        isNoOfTotalPositionsLimited = noOfTotalPositions < int(usrStrategySettings['TOTAL_POSITION_LIMIT'])
        if (isNoOfTotalPositionsLimited):
            
            noOfDayPositions = util.get_no_of_day_positions(mySQLCursor, strategyId, accountId)
            isNoOfDayPositionsLimited = noOfDayPositions < int(usrStrategySettings['DAY_POSITION_LIMIT'])
            
            if (isNoOfDayPositionsLimited):
                utilizedPerIndustryCap= util.get_utilized_cap_per_industry(mySQLCursor, accountId, instrumentToken)
                totalAllocatedCaptial = util.get_total_capital_allocation(mySQLCursor, accountId)
                
                buyValue = tradeDataDict['buyValue']
                isMarginAvailable = False

                if (orderType == 'LMT' or orderType == 'MKT' or orderType == '2L_CASH'):        
                    isMarginAvailable = (availableMargin >= buyValue)        
                    if (orderType == '2L_CASH'):
                        optionsBuyValue = tradeDataDict['optionsBuyValue']
                        isMarginAvailable = (availableMargin >= (buyValue + optionsBuyValue))

                    utilizedPerIndustryCapPct= ((utilizedPerIndustryCap + buyValue) / totalAllocatedCaptial) * 100
                
                elif (orderType == 'FUT' or orderType == '2L_FUT'):
                    futBuyValue = tradeDataDict['futBuyValue']
                    
                    if (orderType == '2L_FUT'):
                        optionsBuyValue = tradeDataDict['optionsBuyValue']
                        isMarginAvailable = (availableMargin >= ((futBuyValue * 0.10)  + optionsBuyValue))
                    else:
                        isMarginAvailable = (availableMargin >= (futBuyValue * 0.25))

                    utilizedPerIndustryCapPct= ((utilizedPerIndustryCap + futBuyValue) / totalAllocatedCaptial) * 100
                
                elif (orderType == 'CALL' or orderType == 'PUT' ):        
                    deltaValue = tradeDataDict['deltaValue']
                    optionsBuyValue = tradeDataDict['optionsBuyValue']
                    utilizedPerIndustryCapPct= ((utilizedPerIndustryCap + deltaValue) / totalAllocatedCaptial) * 100
                    isMarginAvailable = (availableMargin >= optionsBuyValue)

                

                isIndustryUsageLimited = utilizedPerIndustryCapPct <= perIndustryCapLimitPct

                if (isIndustryUsageLimited and isMarginAvailable):  
                    limitExeededFlag = True
                    
    return limitExeededFlag

def buy_trade_service(cnx, mySQLCursor, kite, configDict):    
    for row in util.get_open_signals(mySQLCursor):        
        strategyId = row[0]	
        for row1 in util.get_open_signals(mySQLCursor, strategyId = strategyId): 
            instrumentToken = str(row1[0])
            stockName = row1[1]
            tradeSymbol = row1[2]
            triggerPrice = row1[3]   
            signalDate = row1[4]   
            signalId = row1[5]

            getLTPData =  baf.get_ltp(kite, instrumentToken)
            lastPrice = getLTPData[instrumentToken]['last_price']
            usrStrategySettingList = util.get_usr_strategy_setttings(mySQLCursor, strategyId, accountType='LIVE')
               
            for usrStrategySettings in usrStrategySettingList:
                accountId = usrStrategySettings['TRADE_ACCOUNT']
                
                # Check whether existing order is available; proceed to order only when no existing order present in mysql database
                orderExistFlag = util.check_existing_orders(mySQLCursor, instrumentToken, strategyId, accountId)
                quantity = util.calc_quantity(lastPrice, usrStrategySettings['POSITION_SIZE'])
                buyValue = 0
                tradeDataDict = {}

                if (not(orderExistFlag) and quantity > 0 and usrStrategySettings['STRATEGY_ON_OFF_FLAG'] == 'ON'):

                    broker = usrStrategySettings['BROKER']
                    availableMargin = float(usrStrategySettings['AVAILABLE_MARGIN'])
                    orderType = usrStrategySettings['PRODUCT_ORDER_TYPE']
                    buyValue = int(quantity) * float(triggerPrice)

                    # Base insturments related details
                    tradeDataDict['accountId']=accountId
                    tradeDataDict['broker']=broker
                    tradeDataDict['quantity']=quantity                                
                    tradeDataDict['tradeSymbol']=tradeSymbol                                
                    tradeDataDict['triggerPrice']=triggerPrice                                
                    tradeDataDict['lastPrice']=lastPrice                                
                    tradeDataDict['stockName']=stockName                                
                    tradeDataDict['orderType']=orderType                                                                                       
                    
                    tradeDataDict['tgtProfitAmt']=usrStrategySettings['TGT_PROFIT_AMT']
                    tradeDataDict['tgtProfitPct']=usrStrategySettings['TGT_PROFIT_PCT']
                    tradeDataDict['tgtStopLossAmt']=usrStrategySettings['TGT_STOP_LOSS_AMT']
                    tradeDataDict['tgtStopLossPct']=usrStrategySettings['TGT_STOP_LOSS_PCT']
                    tradeDataDict['exitStrategyId']=usrStrategySettings['EXIT_STRATEGY_ID']
                    tradeDataDict['trailingThresholdPct']=usrStrategySettings['TRAILING_THRESHOLD_PCT']
                    tradeDataDict['instrumentToken']=instrumentToken
                    tradeDataDict['transType']='BUY'
                    tradeDataDict['strategyId']=strategyId
                    tradeDataDict['buyValue']=buyValue
                    tradeDataDict['signalDate']=signalDate
                    tradeDataDict['signalId']=signalId
                    tradeDataDict['orderStatus']='PENDING'

                    if (orderType in OPTIOINS_FUTURE_ORDER_TYPE_LIST):

                        if (orderType in OPTIONS_ORDER_TYPE_LIST):
                            optionsInstName, optionsInstToken, lotSize, expDate, strikePrice, optionsBuyValue, deltaValue = get_options_instruments(mySQLCursor, tradeSymbol, orderType)
                            buyValue = int(lotSize) * float(triggerPrice)

                            tradeDataDict['buyValue'] = buyValue
                            tradeDataDict['optionsTradeSymbol']=optionsInstName                              
                            tradeDataDict['optionsInstToken']=optionsInstToken  
                            tradeDataDict['optionsBuyValue']=optionsBuyValue      
                            tradeDataDict['strikePrice']=strikePrice 
                            tradeDataDict['quantity']=lotSize
                            tradeDataDict['expDate']=expDate   
                            tradeDataDict['deltaValue']=deltaValue   

                            if(broker == "PROSTOCKS" and orderType == "CALL"):                                        
                                tradeDataDict['tradeSymbol']=(tradeDataDict['tradeSymbol']).replace('&', '%26') 
                                tradeDataDict['optionsTradeSymbol'] = tradeSymbol + expDate + 'C' + str(strikePrice).replace('.0','')                                
                            elif(broker == "PROSTOCKS"):
                                tradeDataDict['tradeSymbol']=(tradeDataDict['tradeSymbol']).replace('&', '%26') 
                                tradeDataDict['optionsTradeSymbol'] = tradeSymbol + expDate + 'P' + str(strikePrice).replace('.0','')                                

                        if (orderType in FUTURE_ORDER_TYPE_LIST):                                  
                            futInstName, futInstToken, lotSize, expDate = get_futures_instruments(mySQLCursor, tradeSymbol)
                            futQuoteData = baf.get_quote(kite, futInstToken)    
                            tradeDataDict['futLastPrice'] = futQuoteData[futInstToken]['last_price']                        
                            futBuyPrice = futQuoteData[futInstToken]['depth']['buy'][0]['price']
                            futSellPrice = futQuoteData[futInstToken]['depth']['sell'][0]['price']
                            tradeDataDict['quantity']=lotSize
                            tradeDataDict['expDate']=expDate 

                            if ((float(futBuyPrice) + float(futSellPrice)) == 0):
                                tradeDataDict['futTriggerPrice'] = ("%.1f" % float(tradeDataDict['futLastPrice']))
                            else:
                                tradeDataDict['futTriggerPrice'] = ("%.1f" % float((float(futBuyPrice) + float(futSellPrice)) / 2))     
                                    
                            futBuyValue = int(lotSize) * float(tradeDataDict['futTriggerPrice'])

                            

                            # Future instrument related details
                            tradeDataDict['futTradeSymbol']=futInstName        
                            
                            if(broker == "PROSTOCKS"):                                        
                                tradeDataDict['futTradeSymbol']=tradeSymbol + expDate + 'F'   

                            tradeDataDict['futTriggerPrice']=triggerPrice                                
                            tradeDataDict['futInstToken']=futInstToken
                            tradeDataDict['futBuyValue']=futBuyValue

                    isAcccountUnderLimit = is_account_under_limit(mySQLCursor, usrStrategySettings, tradeDataDict)
                    # Check the number of active orders; if it exceeds the limit provided in mysql trade variables, do not place new gtt order
                    if (isAcccountUnderLimit):                        
                        
                        # Connect to Kite ST
                        brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, accountId, broker)

                        if (isbrokerAPIConnected):
                            if (orderType == "LMT" or orderType == "MKT" ):
                                if (availableMargin >= buyValue):                                             
                                    orderId = place_cash_order(cnx, mySQLCursor, brokerAPI, tradeDataDict)
                                    if (int(orderId) > 0):
                                        alertMsg = "*Stock Name:  " +str(stockName).replace('&', ' and ')+ "*\n*Call:  Buy*\nPeriod:  Short Term (30 days) \n*Entry Price:  " +str("%.2f" % triggerPrice)+ "*\nTarget Profit %: " +str(tradeDataDict['tgtProfitPct'])+ "%\
                                                    \nStop Loss %:  " +str(tradeDataDict['tgtStopLossPct'])+ " %\nBuy Value:  " +str("%.2f" % buyValue)+ "\n\nThis is an automated and system generated call for testing and educational purposes only. You must consider all relevant risk factors, including your own personal financial situation, before trading."
                                        util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=stockName, telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)                                   

                            elif (orderType == "2L_CASH"):
                                if (optionsInstName != ''):                                    
                                    tradeDataDict['optionsLastPrice'],tradeDataDict['optionsTriggerPrice'] = get_options_price_data(kite, optionsInstToken)                                    
                                    tradeDataDict['orderType'] = 'LEG1_PUT'
                                    # send the order to the corresponding account's broker trading platform for the leg 1 
                                    orderId = place_options_order(kite, cnx, mySQLCursor, brokerAPI, tradeDataDict)  

                                    if (int(orderId) > 0):   
                                        alertMsg = "*Stock Name:  " +str(stockName).replace('&', ' and ')+ "*\n*Call:  Buy*\nPeriod:  Short Term (30 days) \n*Entry Price:  " +str("%.2f" % triggerPrice)+ "*\nTarget Profit %: " +str(tradeDataDict['tgtProfitPct'])+ "%\
                                                    \nStop Loss %:  " +str(tradeDataDict['tgtStopLossPct'])+ " %\nBuy Value:  " +str("%.2f" % buyValue)+ "\n\nThis is an automated and system generated call for testing and educational purposes only. You must consider all relevant risk factors, including your own personal financial situation, before trading."
                                        util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=tradeDataDict['optionsTradeSymbol'], telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)                                  
                                                                                
                                        tradeDataDict['orderType'] = 'LEG2_CASH'
                                        orderId = place_cash_order(cnx, mySQLCursor, brokerAPI, tradeDataDict)
                                        if (int(orderId) > 0): 
                                            alertMsg = "*Stock Name:  " +str(stockName).replace('&', ' and ')+ "*\n*Call:  Buy*\nPeriod:  Short Term (30 days) \n*Entry Price:  " +str("%.2f" % triggerPrice)+ "*\nTarget Profit %: " +str(tradeDataDict['tgtProfitPct'])+ "%\
                                                        \nStop Loss %:  " +str(tradeDataDict['tgtStopLossPct'])+ " %\nBuy Value:  " +str("%.2f" % buyValue)+ "\n\nThis is an automated and system generated call for testing and educational purposes only. You must consider all relevant risk factors, including your own personal financial situation, before trading."
                                            util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=stockName, telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)                                    
                                                    

                            elif (orderType == "2L_FUT"):                              
                                if ((optionsInstName != '' and futInstName != '')):                                    
                                    tradeDataDict['orderType'] = 'LEG1_PUT'
                                    # send the order to the corresponding account's broker trading platform for the leg 1 
                                    orderId = place_options_order(kite, cnx, mySQLCursor, brokerAPI, tradeDataDict)                                    
                                    # insert the order details to the MySQL trade transactions table for the leg 1
                                    
                                    if (int(orderId) > 0):      
                                        alertMsg = "*Stock Name:  " +str(stockName).replace('&', ' and ')+ "*\n*Call:  Buy*\nPeriod:  Short Term (30 days) \n*Entry Price:  " +str("%.2f" % triggerPrice)+ "*\nTarget Profit %: " +str(tradeDataDict['tgtProfitPct'])+ "%\
                                                    \nStop Loss %:  " +str(tradeDataDict['tgtStopLossPct'])+ " %\nBuy Value:  " +str("%.2f" % buyValue)+ "\n\nThis is an automated and system generated call for testing and educational purposes only. You must consider all relevant risk factors, including your own personal financial situation, before trading."
                                        util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=tradeDataDict['optionsTradeSymbol'], telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)    

                                        tradeDataDict['orderType'] = 'LEG2_FUT'                                    
                                        orderId = place_future_order(kite, cnx, mySQLCursor, brokerAPI, tradeDataDict)                                 
                                                                                     
                                        if (int(orderId) > 0): 
                                            alertMsg = "*Stock Name:  " +str(stockName).replace('&', ' and ')+ "*\n*Call:  Buy*\nPeriod:  Short Term (30 days) \n*Entry Price:  " +str("%.2f" % triggerPrice)+ "*\nTarget Profit %: " +str(tradeDataDict['tgtProfitPct'])+ "%\
                                                        \nStop Loss %:  " +str(tradeDataDict['tgtStopLossPct'])+ " %\nBuy Value:  " +str("%.2f" % buyValue)+ "\n\nThis is an automated and system generated call for testing and educational purposes only. You must consider all relevant risk factors, including your own personal financial situation, before trading."
                                            util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=tradeDataDict['futTradeSymbol'], telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)                                    

                            
                            elif (orderType == "FUT"):                               
                                if (futInstName != ''):        
                                    orderId = place_future_order(kite, cnx, mySQLCursor, brokerAPI, tradeDataDict) 
                                    if (int(orderId) > 0):                                              
                                        alertMsg = "*Stock Name:  " +str(stockName).replace('&', ' and ')+ "*\n*Call:  Buy*\nPeriod:  Short Term (30 days) \n*Entry Price:  " +str("%.2f" % triggerPrice)+ "*\nTarget Profit %: " +str(tradeDataDict['tgtProfitPct'])+ "%\
                                                    \nStop Loss %:  " +str(tradeDataDict['tgtStopLossPct'])+ " %\nBuy Value:  " +str("%.2f" % buyValue)+ "\n\nThis is an automated and system generated call for testing and educational purposes only. You must consider all relevant risk factors, including your own personal financial situation, before trading."
                                        util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=tradeDataDict['futTradeSymbol'], telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)                                    
                                        
                            
                            elif (orderType == "CALL" or orderType == "PUT"):            
                                if (optionsInstName != ''):                                    
                                    orderId = place_options_order(kite, cnx, mySQLCursor, brokerAPI, tradeDataDict)
                                    if (int(orderId) > 0):                                           
                                        alertMsg = "*Stock Name:  " +str(stockName).replace('&', ' and ')+ "*\n*Call:  Buy*\nPeriod:  Short Term (30 days) \n*Entry Price:  " +str("%.2f" % triggerPrice)+ "*\nTarget Profit %: " +str(tradeDataDict['tgtProfitPct'])+ "%\
                                                    \nStop Loss %:  " +str(tradeDataDict['tgtStopLossPct'])+ " %\nBuy Value:  " +str("%.2f" % buyValue)+ "\n\nThis is an automated and system generated call for testing and educational purposes only. You must consider all relevant risk factors, including your own personal financial situation, before trading."
                                        util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=tradeDataDict['optionsTradeSymbol'], telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)                                    

                        else:
                            alertMsg = 'Unable to connect to API account of ' + str(accountId)
                            util.send_alerts(configDict['ENABLE_LOG_FLAG'], cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, 'Y', 'Y') 
                    
                    # else:
                    #     if (noOfDayPositions > int(usrStrategySettings['DAY_POSITION_LIMIT'])):
                    #         alertMsg = "Day Position Limit is exceeded. Can't buy " + str(stockName) + " for strategy ID : " + str(strategyId)
                    #         util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=stockName)
                    #     elif (noOfTotalPositions > int(usrStrategySettings['TOTAL_POSITION_LIMIT'])):
                    #         alertMsg = "Total Position Limit is exceeded. Can't buy " + str(stockName) + " for strategy ID : " + str(strategyId)
                    #         util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=stockName)
                    #     elif (utilizedPerIndustryCapPct > perIndustryCapLimitPct):
                    #         alertMsg = "Total Industry Exposure exceeded. Can't buy " + str(stockName) + " for strategy ID : " + str(strategyId)
                    #         util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=accountId, programName=programName, strategyId=strategyId, stockName=stockName)

                    


# Main function is called by default, and the first function to be executed
if __name__ == "__main__":    
    
    # Read the system configration file that contains logs informations, and telegram ids
    configFileH = open('conf/config.ini')
    configList = configFileH.readlines()
    configFileH.close()
    configDict = {}
    OPTIOINS_FUTURE_ORDER_TYPE_LIST = ['FUT', '2L_CASH', '2L_FUT', 'CALL','PUT']
    OPTIONS_ORDER_TYPE_LIST = ['2L_CASH', '2L_FUT', 'CALL','PUT']
    FUTURE_ORDER_TYPE_LIST = ['2L_FUT', '']

    # Store the configuraiton files in a dictionary for reusablity. 
    for configItem in configList:
        configItem = configItem.strip('\n').strip('\r').split('=')
        if(len(configItem) > 1):            
            configDict[str(configItem[0])] = configItem[1]    
    
    logEnableFlag = True if configDict['ENABLE_LOG_FLAG'] == 'True' else False 
    testingFlag = True if configDict['TESTING_FLAG'] == 'True' else False
    
    TELG_ADMIN_ID = configDict['TELG_ADMIN_ID']

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
        
        # Continuously run the program until the exit flag turns to Y
        while programExitFlag != 'Y': 
            try:
                # Verify whether the connection to MySQL database is open
                cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
                
                sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
                currentTime = util.get_date_time_formatted("%H%M")

                if (testingFlag or ((int(currentTime) >= int(sysSettings['SYSTEM_START_TIME'])) and (int(currentTime) <= int(sysSettings['SYSTEM_END_TIME'])))):
                    
                    try: 
                        buy_trade_service(cnx, mySQLCursor, kite, configDict)
                    except Exception as e:
                        alertMsg = 'Exceptions occured in buy_trade_service block: ' + str(e)                  
                        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='N', programName=programName)    
                    
                    
                    
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

