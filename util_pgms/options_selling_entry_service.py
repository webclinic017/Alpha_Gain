from cmath import log
from importlib.resources import path
import matplotlib.pyplot as plt
import pandas as pd
from scipy.signal import argrelextrema

from utils import util_functions as util

from utils import broker_api_functions as baf

from scipy.stats import linregress
import logging
import requests
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import mibian

def check_existing_options_position(mySQLCursor, instrumentToken, optionType=None, orderType='ENTRY'):

    if (orderType == 'ENTRY'):
        selectStatment = f"SELECT INSTRUMENT_TOKEN FROM TRADE_TRANSACTIONS WHERE BASE_INSTRUMENT_TOKEN='{instrumentToken}' AND ((TRADE_STATUS ='PENDING' OR TRADE_STATUS ='OPEN' OR TRADE_STATUS ='P-OPEN') OR (BUY_ORDER_STATUS= 'REJECTED' AND DATE(BUY_ORDER_DATE) = '{str(currDate)}'))"
    elif (orderType == 'ADJUSTMENT'):
        selectStatment = f"SELECT INSTRUMENT_TOKEN FROM TRADE_TRANSACTIONS WHERE INSTRUMENT_TYPE={optionType} AND INSTRUMENT_TOKEN='{instrumentToken}' AND ((TRADE_STATUS ='PENDING' OR TRADE_STATUS ='OPEN' OR TRADE_STATUS ='P-OPEN') OR (BUY_ORDER_STATUS= 'REJECTED' AND DATE(BUY_ORDER_DATE) = '{str(currDate)}'))"

    mySQLCursor.execute(selectStatment)
    # gets the number of rows affected by the command executed
    rowCount = mySQLCursor.rowcount
    orderExistFlag = False
    if rowCount > 0:
        orderExistFlag = True   
    
    return orderExistFlag


def insert_options_order_details(cnx, mySQLCursor, tradeDataDict, optionsBSMData):
    try: 
        insertVal = []
        updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        
        orderQuery = "INSERT INTO TRADE_TRANSACTIONS (TRADE_ACCOUNT, STRATEGY_ID, EXIT_STRATEGY_ID, BUY_ORDER_DATE, SIGNAL_DATE, \
                    INSTRUMENT_TYPE, BUY_ORDER_ID, INSTRUMENT_TOKEN, BASE_INSTRUMENT_TOKEN, TRADE_SYMBOL, STOCK_NAME, BUY_ORDER_PRICE, CURRENT_MKT_PRICE, BUY_VALUE, \
                    QUANTITY, BUY_ORDER_STATUS, ORDER_REMARKS, TRADE_STATUS, TGT_PROFIT_PCT, TGT_STOP_LOSS_PCT, TRAILING_THRESHOLD_PCT, \
                    BROKER, EXCHANGE, UPDATED_ON, BASE_STOCK_ENTRY_PRICE, POSITIONS_GROUP_ID, TRADE_SEQUENCE, TRADED_DELTA, TRADED_IMPLIED_VOLATILITY, \
                    OPTION_STRIKE_PRICE, INITIAL_DAYS_TO_EXPIRY, EXPIRY_DATE, BASE_TRADE_SYMBOL) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        insertVal.insert(0, str(tradeDataDict['accountId']))
        insertVal.insert(1, str(tradeDataDict['strategyId']))  
        insertVal.insert(2, str(tradeDataDict['exitStrategyId']))   
        insertVal.insert(3, str(updatedOn))      
        insertVal.insert(4, str(tradeDataDict['signalDate']))
        insertVal.insert(5, str(optionsBSMData['optionsType']))
        insertVal.insert(6, str(tradeDataDict['orderId']))  
        insertVal.insert(7, str(tradeDataDict['futInstToken']))
        insertVal.insert(8, str(tradeDataDict['instrumentToken']))
        insertVal.insert(9, str(tradeDataDict['optionsTradeSymbol']))
        insertVal.insert(10, str(tradeDataDict['optionsTradeSymbol']))
        insertVal.insert(11, str(tradeDataDict['futTriggerPrice']))     
        insertVal.insert(12, str(tradeDataDict['futLastPrice']))        
        insertVal.insert(13, str(tradeDataDict['futBuyValue'] * -1))
        insertVal.insert(14, str(tradeDataDict['quantity'] * -1))                
        insertVal.insert(15, str('PENDING'))
        insertVal.insert(16, str(tradeDataDict['orderRemarks']))       
        insertVal.insert(17, str('PENDING'))        
        insertVal.insert(18, str(tradeDataDict['tgtProfitPct']))                        
        insertVal.insert(19, str(tradeDataDict['tgtStopLossPct']))      
        insertVal.insert(20, str(tradeDataDict['trailingThresholdPct']))       
        insertVal.insert(21, str(tradeDataDict['broker']))
        insertVal.insert(22, str(tradeDataDict['exchange']))
        insertVal.insert(23, str(updatedOn))
        insertVal.insert(24, str(optionsBSMData['stockClosePrice']))        
        insertVal.insert(25, str(tradeDataDict['positionGroupId']))        
        insertVal.insert(26, str('STAGE-1'))
        insertVal.insert(27, str(optionsBSMData['selectedDelta']))
        insertVal.insert(28, str(optionsBSMData['selectedImpliedVol']))
        insertVal.insert(29, str(optionsBSMData['selectedStrikePrice']))
        insertVal.insert(30, str(optionsBSMData['daysToExpiry']))        
        insertVal.insert(31, str(optionsBSMData['fnoExpiry']))        
        insertVal.insert(32, str(tradeDataDict['tradeSymbol']))        
        mySQLCursor.execute(orderQuery, insertVal)
        cnx.commit()
    
    except Exception as e:
        logging.info("DB FAILURE: UNABLE TO INSERT: " + tradeDataDict['optionsTradeSymbol'])
        logging.info(str(e))   
    


def bsm_call_options_pricing(mySQLCursor,kite, stockClosePrice, tradeSymbol, selectionLimit = 5):
    try:
        expiryMonth = util.get_fno_expiry_month()
        instSearchString = str(tradeSymbol) + str(expiryMonth)
        selectStatment = f"SELECT instrument_token, strike, expiry, tradingsymbol, tick_size, lot_size FROM INSTRUMENTS WHERE tradingsymbol LIKE '{instSearchString}%' AND exchange='NFO' AND instrument_type='CE' AND strike >=  {str(stockClosePrice)} ORDER BY strike DESC LIMIT {str(selectionLimit)}"
        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()

        # impliedVolCallList = []
        callPriceBSMList = []
        deltaCallList = []
        thetaCallList = []
        gammaList = []
        vegaList = []
        strikePriceList = []
        fnoVolumeList = []
        fnoOpenIntrestList = []
        fnoBuyPriceList = []
        fnoSellPriceList = []
        impliedVolList = []
        interestRate = 6
        fnoTradingSymbolList = []
        fnoInstrumentTokenList = []
        fnoLastPriceList = []
        fnoTickSize = 0
        fnoLotSize = 0

        for row in results:
        
            fnoInstrumentToken = row[0]
            fnoStrikePrice = row[1]
            fnoExpiry = row[2]
            fnoTradingSymbol = row[3]
            fnoTickSize = row[4]
            fnoLotSize = row[5]

            fDate = datetime.datetime.strptime(str(util.get_date_time_formatted("%Y-%m-%d")), "%Y-%m-%d")
            lDate = datetime.datetime.strptime(str(fnoExpiry), "%Y-%m-%d")

            daysToExpiry = lDate - fDate
            quoteData = baf.get_quote(kite, fnoInstrumentToken)
    
            fnoLastPrice = quoteData[fnoInstrumentToken]['last_price']
            fnoOpenInterest = quoteData[fnoInstrumentToken]['oi']
            fnoVolume = quoteData[fnoInstrumentToken]['volume']
            fnoBuyPrice = quoteData[fnoInstrumentToken]['depth']['buy'][0]['price']
            fnoSellPrice = quoteData[fnoInstrumentToken]['depth']['sell'][0]['price']
            
            strikePriceList.append(fnoStrikePrice)
            fnoVolumeList.append(fnoVolume)
            fnoOpenIntrestList.append(fnoOpenInterest)
            fnoBuyPriceList.append(fnoBuyPrice)
            fnoSellPriceList.append(fnoSellPrice)
            fnoTradingSymbolList.append(fnoTradingSymbol)
            fnoLastPriceList.append(fnoLastPrice)
            fnoInstrumentTokenList.append(fnoInstrumentToken)
            
            if (float(fnoLastPrice) > 0):

                iv = mibian.BS([stockClosePrice, fnoStrikePrice, interestRate, int(daysToExpiry.days)], callPrice=fnoLastPrice)
                impliedVolList.append(float("{:.2f}".format(iv.impliedVolatility)))
                c = mibian.BS([stockClosePrice, fnoStrikePrice, interestRate, int(daysToExpiry.days)], volatility=float("{:.2f}".format(iv.impliedVolatility)))
                callPriceBSMList.append(float("{:.2f}".format(c.callPrice)))
                deltaCallList.append(float("{:.2f}".format(c.callDelta)))
                thetaCallList.append(float("{:.2f}".format(c.callTheta)))
                gammaList.append(float("{:.2f}".format(c.gamma)))
                vegaList.append(float("{:.2f}".format(c.vega)))
            else:
                response = {}   
                response['status'] = 'failed'
                response['remarks'] = 'FNO (PUT) Price is zero for ' + tradeSymbol
                return response

        
        if (len(strikePriceList) > 0):
            # Get the closest delta value to 0.40 (40)
            closestStrikeIndex = min(range(len(deltaCallList)), key=lambda i: abs(deltaCallList[i]-0.40))
            selectedDelta = deltaCallList[closestStrikeIndex]

            selectedStrikePrice = strikePriceList[closestStrikeIndex]
            selectedVolume = fnoVolumeList[closestStrikeIndex]
            selectedOpenIntrest = fnoOpenIntrestList[closestStrikeIndex]
            selectedBuyPrice = fnoBuyPriceList[closestStrikeIndex]
            selectedSellPrice = fnoSellPriceList[closestStrikeIndex]
            selectedLastPrice = fnoLastPriceList[closestStrikeIndex]
            selectedImpliedVol = impliedVolList[closestStrikeIndex]
            selectedTradingSymbol =  fnoTradingSymbolList[closestStrikeIndex]
            selectedInsturmentToken = fnoInstrumentTokenList[closestStrikeIndex]
    
            fnoBuyPrice = float(selectedBuyPrice) + float(fnoTickSize)
            fnoBuyValue = fnoBuyPrice * fnoLotSize
            cashValue = fnoLotSize * float(selectedStrikePrice)
            marginRequired = cashValue * 0.25
            premiumValuePct = (fnoBuyValue / cashValue ) * 100
            breakEvenPrice = float(selectedStrikePrice) - float(selectedSellPrice)            
            priceProtection =  ((breakEvenPrice / float(stockClosePrice)) - 1) * 100
            probabilityProfit = 1 + float(selectedDelta)

            response = {}   
            response['tradeSymbol'] = tradeSymbol
            response['stockClosePrice'] = stockClosePrice
            response['fnoLotSize'] = fnoLotSize
            response['cashValue'] = cashValue
            response['marginRequired'] = marginRequired
            response['selectedTradingSymbol'] = selectedTradingSymbol
            response['lDate'] = lDate
            response['selectedStrikePrice'] = selectedStrikePrice
            response['fnoBuyPrice'] = fnoBuyPrice
            response['fnoBuyValue'] = fnoBuyValue
            response['premiumValuePct'] = premiumValuePct
            response['selectedDelta'] = selectedDelta
            response['probabilityProfit'] = probabilityProfit
            response['breakEvenPrice'] = breakEvenPrice
            response['priceProtection'] = priceProtection
            response['selectedImpliedVol']= selectedImpliedVol
            response['selectedVolume']= selectedVolume
            response['daysToExpiry'] = daysToExpiry.days
            response['selectedOpenIntrest'] = selectedOpenIntrest            
            response['selectedInsturmentToken'] = selectedInsturmentToken
            response['selectedLastPrice'] = selectedLastPrice
            response['fnoExpiry'] = fnoExpiry
            response['optionsType'] = 'CALL'            
            response['status'] = 'success'
            response['remarks'] = 'success'                                  
            
            return response

        else:  
            response = {}   
            response['status'] = 'failed'
            response['remarks'] = 'Strike Price List doesn\'t have any value for ' + tradeSymbol
            return response

    except Exception as e:            
        response = {}   
        response['status'] = 'error'
        response['remarks'] = "Errored while getting the BSM CALL Options Pricing for " + tradeSymbol + ": "+ str(e)
        return response

def bsm_put_options_pricing(mySQLCursor, kite, stockClosePrice, tradeSymbol, selectionLimit = 5):
    try:
        expiryMonth = util.get_fno_expiry_month()    
        instSearchString = str(tradeSymbol) + str(expiryMonth)        

        selectStatment = f"SELECT instrument_token, strike, expiry, tradingsymbol, tick_size, lot_size FROM INSTRUMENTS WHERE tradingsymbol LIKE '{instSearchString}%' AND exchange='NFO' AND instrument_type='PE' AND strike <=  {str(stockClosePrice)} ORDER BY strike DESC LIMIT {str(selectionLimit)}"
        
        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()

        # impliedVolputList = []
        putPriceBSMList = []
        deltaPutList = []
        thetaPutList = []
        gammaList = []
        vegaList = []
        strikePriceList = []
        fnoVolumeList = []
        fnoOpenIntrestList = []
        fnoBuyPriceList = []
        fnoSellPriceList = []
        impliedVolList = []
        interestRate = 6
        fnoTradingSymbolList = []
        fnoInstrumentTokenList = []
        fnoLastPriceList = []
        fnoTickSize = 0
        fnoLotSize = 0

        for row in results:
        
            fnoInstrumentToken = row[0]
            fnoStrikePrice = row[1]
            fnoExpiry = row[2]
            fnoTradingSymbol = row[3]
            fnoTickSize = row[4]
            fnoLotSize = row[5]

            fDate = datetime.datetime.strptime(str(util.get_date_time_formatted("%Y-%m-%d")), "%Y-%m-%d")
            lDate = datetime.datetime.strptime(str(fnoExpiry), "%Y-%m-%d")

            daysToExpiry = lDate - fDate
                
            
            quoteData = baf.get_quote(kite, fnoInstrumentToken)

            fnoLastPrice = quoteData[fnoInstrumentToken]['last_price']
            fnoOpenInterest = quoteData[fnoInstrumentToken]['oi']
            fnoVolume = quoteData[fnoInstrumentToken]['volume']
            fnoBuyPrice = quoteData[fnoInstrumentToken]['depth']['buy'][0]['price']
            fnoSellPrice = quoteData[fnoInstrumentToken]['depth']['sell'][0]['price']
            
            strikePriceList.append(fnoStrikePrice)
            fnoVolumeList.append(fnoVolume)
            fnoOpenIntrestList.append(fnoOpenInterest)
            fnoBuyPriceList.append(fnoBuyPrice)
            fnoSellPriceList.append(fnoSellPrice)
            fnoTradingSymbolList.append(fnoTradingSymbol)
            fnoLastPriceList.append(fnoLastPrice)
            fnoInstrumentTokenList.append(fnoInstrumentToken)
            
            if (float(fnoLastPrice) > 0):
                
                iv = mibian.BS([stockClosePrice, fnoStrikePrice, interestRate, int(daysToExpiry.days)], putPrice=fnoLastPrice)
                impliedVolList.append(float("{:.2f}".format(iv.impliedVolatility)))
                p = mibian.BS([stockClosePrice, fnoStrikePrice, interestRate, int(daysToExpiry.days)], volatility=float("{:.2f}".format(iv.impliedVolatility)))
                
                putPriceBSMList.append(float("{:.2f}".format(p.putPrice)))
                deltaPutList.append(float("{:.2f}".format(p.putDelta)))
                thetaPutList.append(float("{:.2f}".format(p.putTheta)))
                gammaList.append(float("{:.2f}".format(p.gamma)))
                vegaList.append(float("{:.2f}".format(p.vega)))
            else:               
                response = {}   
                response['status'] = 'failed'
                response['remarks'] = 'FNO (CALL) Price is zero for ' + tradeSymbol
                return response

        if (len(strikePriceList) > 0):
            # Get the closest delta value to 0.40 (40)
            closestStrikeIndex = min(range(len(deltaPutList)), key=lambda i: abs(deltaPutList[i]+0.40))
            selectedDelta = deltaPutList[closestStrikeIndex]            
            selectedImpliedVol = impliedVolList[closestStrikeIndex]
            selectedStrikePrice = strikePriceList[closestStrikeIndex]
            selectedVolume = fnoVolumeList[closestStrikeIndex]
            selectedOpenIntrest = fnoOpenIntrestList[closestStrikeIndex]            
            selectedSellPrice = fnoSellPriceList[closestStrikeIndex]
            selectedLastPrice = fnoLastPriceList[closestStrikeIndex]            
            selectedTradingSymbol =  fnoTradingSymbolList[closestStrikeIndex]
            selectedInsturmentToken = fnoInstrumentTokenList[closestStrikeIndex]           

            fnoBuyPrice = float(selectedSellPrice) + float(fnoTickSize)
            fnoBuyValue = fnoBuyPrice * fnoLotSize
            
            cashValue = fnoLotSize * float(selectedStrikePrice)
            marginRequired = cashValue * 0.25
            premiumValuePct = (fnoBuyValue / cashValue ) * 100
            breakEvenPrice = float(selectedStrikePrice) - float(selectedSellPrice)            
            priceProtection =  ((breakEvenPrice / float(stockClosePrice)) - 1) * 100
            probabilityProfit = 1 + float(selectedDelta)


            response = {}   
            response['tradeSymbol'] = tradeSymbol
            response['stockClosePrice'] = stockClosePrice
            response['fnoLotSize'] = fnoLotSize
            response['cashValue'] = cashValue
            response['marginRequired'] = marginRequired
            response['selectedTradingSymbol'] = selectedTradingSymbol
            response['lDate'] = lDate
            response['selectedStrikePrice'] = selectedStrikePrice
            response['fnoBuyPrice'] = fnoBuyPrice
            response['fnoBuyValue'] = fnoBuyValue
            response['premiumValuePct'] = premiumValuePct
            response['selectedDelta'] = selectedDelta
            response['probabilityProfit'] = probabilityProfit
            response['breakEvenPrice'] = breakEvenPrice
            response['priceProtection'] = priceProtection
            response['selectedImpliedVol']= selectedImpliedVol
            response['selectedVolume']= selectedVolume
            response['daysToExpiry'] = daysToExpiry.days
            response['selectedOpenIntrest'] = selectedOpenIntrest            
            response['selectedInsturmentToken'] = selectedInsturmentToken
            response['selectedLastPrice'] = selectedLastPrice
            response['optionsType'] = 'PUT'  
            response['fnoExpiry'] = fnoExpiry
            response['status'] = 'success'
            response['remarks'] = 'success'         
            return response

        else:  
            response = {}   
            response['status'] = 'failed'
            response['remarks'] = 'Strike Price List doesn\'t have any value for ' + tradeSymbol
            return response

    except Exception as e:            
        response = {}   
        response['status'] = 'error'
        response['remarks'] = "Errored while getting the BSM CALL Options Pricing for " + tradeSymbol + ": "+ str(e)
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
    
    TELG_ADMIN_ID = configDict['TELG_ADMIN_ID']

    programName = configDict['OPTIONS_SELLING_ENTRY_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['OPTIONS_SELLING_ENTRY_PGM_NAME']) + '.log')

    programExitFlag = 'N'    

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)
    
    adminTradeAccount = configDict['ADMIN_TRADE_ACCOUNT']
    
    currDate = util.get_date_time_formatted("%Y-%m-%d")

    # Connect to Kite ST
    kite, isKiteConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = configDict['ADMIN_TRADE_BROKER'])    
    
    exitAllPosFlag = False
    
    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if (isKiteConnected):           
       
        alertMsg = 'The program (' + programName.replace('_','\_') + ')  started at ' + str(util.get_date_time_formatted("%d-%m-%Y %H:%M:%S"))
        
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)
        
        # Continuously run the program until the exit flag turns to Y
        while programExitFlag != 'Y': 
            # Verify whether the connection to MySQL database is open
            cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
            
            sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
            currentTime = util.get_date_time_formatted("%H%M")

            if (testingFlag or ((int(currentTime) >= int(sysSettings['SYSTEM_START_TIME'])) and (int(currentTime) <= int(sysSettings['SYSTEM_END_TIME'])))):
                try:
                    start=datetime.datetime.now()
                    ivLimit = 50
                    tradeDataDict = {}
                    tradeDataDict['broker'] = 'PROSTOCKS'                    
                    tradeDataDict['accountId'] = 'R0428'
                    tradeDataDict['transactionType'] = "S"
                    tradeDataDict['trailingThresholdPct'] = 80
                    tradeDataDict['tgtProfitPct'] = 25                     
                    tradeDataDict['tgtStopLossPct'] = -25
                    tradeDataDict['trailingThresholdPct'] = 80
                    tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
                    tradeDataDict['signalId'] = 1
                    tradeDataDict['futOrderType'] = 'LMT'                
                    tradeDataDict['orderStatus']= 'PENDING'                
                    tradeDataDict['strategyId'] = 'ALPHA_11'                    
                    tradeDataDict['exitStrategyId'] = 'OPTIONS_STRANGLE_EXIT'                    
                    tradeDataDict['exchange'] = 'NFO'                                                            
                    tradeDataDict['futTransactionType'] = "SELL"                    

                    
                    currDate = '2022-05-06'

                    instSelectionQuery = f"SELECT A.BASE_INSTRUMENT_TOKEN, A.TRADE_SYMBOL, A.EXPIRY_DATE, A.LOT_SIZE, C.IMPLIED_VOLATILITY AS CALL_IV, D.IMPLIED_VOLATILITY AS PUT_IV  \
                                            FROM PROD.CASH_SECURED_PUT_WATCHLIST AS A LEFT JOIN (SELECT BASE_INSTRUMENT_TOKEN,TRADE_SYMBOL, IMPLIED_VOLATILITY FROM PROD.CASH_SECURED_PUT_WATCHLIST WHERE OPTION_TYPE = 'CALL') AS C ON C.BASE_INSTRUMENT_TOKEN = A.BASE_INSTRUMENT_TOKEN  \
                                            LEFT JOIN (SELECT BASE_INSTRUMENT_TOKEN,TRADE_SYMBOL, IMPLIED_VOLATILITY FROM PROD.CASH_SECURED_PUT_WATCHLIST WHERE OPTION_TYPE = 'PUT') AS D ON D.BASE_INSTRUMENT_TOKEN = A.BASE_INSTRUMENT_TOKEN \
                                            WHERE C.IMPLIED_VOLATILITY >= {ivLimit} AND D.IMPLIED_VOLATILITY >= {ivLimit} AND A.REPORT_DATE = '{currDate}' GROUP BY A.BASE_INSTRUMENT_TOKEN ORDER BY C.IMPLIED_VOLATILITY DESC"


                    mySQLCursor.execute(instSelectionQuery)
                    instList = mySQLCursor.fetchall()                                        
                    brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'])

                    for instRow in instList:
                        baseInstToken = str(instRow[0])
                        tradeDataDict['instrumentToken'] = baseInstToken
                        tradeSymbol = str(instRow[1])
                        expDate = str(instRow[2].strftime("%d%b%y")).upper()
                        lotSize = int(instRow[3])
                        
                        tradeDataDict['tradeSymbol'] = tradeSymbol
                        tradeDataDict['quantity'] = lotSize

                        getLTPData =  baf.get_ltp(kite, baseInstToken)
                        ltpPrice = getLTPData[baseInstToken]['last_price']
                        putOptResponseData = bsm_put_options_pricing(mySQLCursor, kite, ltpPrice, tradeSymbol)
                        callOptResponseData = bsm_call_options_pricing(mySQLCursor, kite, ltpPrice, tradeSymbol)                        
                        
                        existingPostionFoundFlag = check_existing_options_position(mySQLCursor, baseInstToken, orderType='ENTRY')
                        

                        if (not(existingPostionFoundFlag) and float(putOptResponseData['selectedImpliedVol']) >= ivLimit and float(callOptResponseData['selectedImpliedVol']) >= ivLimit):

                            optionsInstToken = putOptResponseData['selectedInsturmentToken']
                            quoteData = baf.get_quote(kite, optionsInstToken)                        
                            tradeDataDict['optionsTriggerPrice'] = float(quoteData[str(optionsInstToken)]['depth']['sell'][0]['price']) - 0.1
                            
                            # ********************** to be removed
                            tradeDataDict['optionsTriggerPrice'] =  float(quoteData[str(optionsInstToken)]['last_price'])
                            # ********************** to be removed

                            tradeDataDict['optionsTradeSymbol'] = (tradeSymbol).replace('&', '%26') + expDate + 'P' + str(putOptResponseData['selectedStrikePrice']).replace('.0','')                                                        
                            
                            tradeDataDict['futInstToken'] = putOptResponseData['selectedInsturmentToken']                            
                            tradeDataDict['futLastPrice'] = tradeDataDict['optionsTriggerPrice']
                            tradeDataDict['futTriggerPrice'] = tradeDataDict['optionsTriggerPrice']     
                            tradeDataDict['futBuyValue'] = tradeDataDict['quantity'] * float(tradeDataDict['optionsTriggerPrice'])

                            existingOrderFound, proStocksOrderId, proStocksTransType, proStocksQty = baf.get_prostocks_orders(brokerAPI, tradeDataDict['optionsTradeSymbol'])
                            # Modify the existing orders
                            if (existingOrderFound):   
                                baf.modify_fno_order(brokerAPI, tradeDataDict['accountId'], proStocksOrderId, tradeDataDict['optionsTriggerPrice'], tradeDataDict['optionsTradeSymbol'], proStocksQty)

                            elif (float(tradeDataDict['optionsTriggerPrice']) > 1):                                
                                orderId, orderRemarks  = baf.place_options_order(brokerAPI, tradeDataDict)
                                if (int(orderId) > 0):
                                    tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')
                                    tradeDataDict['orderId']=orderId
                                    tradeDataDict['orderRemarks']=orderRemarks
                                    insert_options_order_details(cnx, mySQLCursor, tradeDataDict, putOptResponseData)

                                    # alertMsg = f"*Stock Name:  {tradeDataDict['futTradeSymbol']}*\nCall:  Sell\n*Entry Price:  {str('%.2f' % tradeDataDict['futLastPrice'])}* \nBuy Value:  {str('%.2f' % tradeDataDict['futBuyValue'])}"
                                    # util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=tradeDataDict['accountId'], programName=programName, strategyId='1', stockName=tradeDataDict['futTradeSymbol'], telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)                                                                                       
                                
                                    optionsInstToken = callOptResponseData['selectedInsturmentToken']
                                    quoteData = baf.get_quote(kite, optionsInstToken)                        
                                    tradeDataDict['optionsTriggerPrice'] = float(quoteData[str(optionsInstToken)]['depth']['sell'][0]['price']) - 0.1
                                    # ********************** to be removed
                                    tradeDataDict['optionsTriggerPrice'] =  float(quoteData[str(optionsInstToken)]['last_price'])



                                    tradeDataDict['optionsTradeSymbol'] = (tradeSymbol).replace('&', '%26') + expDate + 'C' + str(callOptResponseData['selectedStrikePrice']).replace('.0','')
                                    tradeDataDict['futInstToken'] = callOptResponseData['selectedInsturmentToken']                            
                                    tradeDataDict['futLastPrice'] = tradeDataDict['optionsTriggerPrice']
                                    tradeDataDict['futTriggerPrice'] = tradeDataDict['optionsTriggerPrice']     
                                    tradeDataDict['futBuyValue'] = tradeDataDict['quantity'] * float(tradeDataDict['optionsTriggerPrice'])



                                    if (float(tradeDataDict['optionsTriggerPrice']) > 1):
                                        orderId, orderRemarks  = baf.place_options_order(brokerAPI, tradeDataDict)                                                
                                        if (int(orderId) > 0):                            
                                            tradeDataDict['orderId']=orderId
                                            tradeDataDict['orderRemarks']=orderRemarks
                                            insert_options_order_details(cnx, mySQLCursor, tradeDataDict, callOptResponseData)
                                            # alertMsg = f"*Stock Name:  {tradeDataDict['futTradeSymbol']}*\nCall:  Sell\n*Entry Price:  {str('%.2f' % tradeDataDict['futLastPrice'])}* \nBuy Value:  {str('%.2f' % tradeDataDict['futBuyValue'])}"
                                            # util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=tradeDataDict['accountId'], programName=programName, strategyId='1', stockName=tradeDataDict['futTradeSymbol'], telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)                                                                                       

                except Exception as e:
                    alertMsg = "ERROR: Live trade service failed (main block): " + str(e)
                    logging.info(alertMsg)

            elif (int(currentTime) > int(sysSettings['SYSTEM_END_TIME'])):           
                programExitFlag = 'Y'
            
            # util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
            util.disconnect_db(cnx, mySQLCursor)


    util.logger(logEnableFlag, "info", "Program ended")
