from utils import util_functions as util
from utils import broker_api_functions as baf
import datetime, time
import requests
import logging
import re
from config import Config
import os
import mibian
import concurrent.futures
from utils import prostocks_api_helper as prostocksApi

def bsm_options_pricing(brokerApi, exchangeToken, strikePrice, expiry, baseLtpPrice, tradeSymbol, instrumentType):
    bsmDataDict = {}
    try:
        interestRate = 6
        fDate = datetime.datetime.strptime(str(util.get_date_time_formatted("%Y-%m-%d")), "%Y-%m-%d")
        lDate = datetime.datetime.strptime(str(expiry), "%Y-%m-%d")
        daysToExpiry1 = 1 
        daysToExpiry = lDate - fDate

        if (daysToExpiry.days == 0):
            daysToExpiry1 = 1
        else:
            daysToExpiry1 = int(daysToExpiry.days)
    
        quoteData = prostocksApi.get_quotes(brokerApi, exchangeToken, exchange='NFO')

        fnoLastPrice = quoteData['lp']

        if (float(fnoLastPrice) > 0):
            # bsmDataDict['OI_VALUE'] = float(quoteData[instToken]['oi']) * float(closePrice)
            bsmDataDict['VOLUME_VALUE']= float(quoteData['v']) * float(baseLtpPrice)

            if (instrumentType == 'PUT'):
                iv = mibian.BS([baseLtpPrice, strikePrice, interestRate, daysToExpiry1], putPrice=fnoLastPrice)                
                c = mibian.BS([baseLtpPrice, strikePrice, interestRate, daysToExpiry1], volatility=float("{:.2f}".format(iv.impliedVolatility)))
                bsmDataDict['delta'] = float("{:.2f}".format(c.putDelta))
                bsmDataDict['theta'] = float("{:.2f}".format(c.putTheta))
            else:
                iv = mibian.BS([baseLtpPrice, strikePrice, interestRate, daysToExpiry1], callPrice=fnoLastPrice)
                c = mibian.BS([baseLtpPrice, strikePrice, interestRate, daysToExpiry1], volatility=float("{:.2f}".format(iv.impliedVolatility)))
                bsmDataDict['delta'] = float("{:.2f}".format(c.callDelta))
                bsmDataDict['theta'] = float("{:.2f}".format(c.callTheta))
            
            bsmDataDict['vega'] = float("{:.2f}".format(c.vega))
            bsmDataDict['gamma'] = float("{:.5f}".format(c.gamma))

            bsmDataDict['IMPLIED_VOLATILITY']= float("{:.2f}".format(iv.impliedVolatility))
            bsmDataDict['daysToExpiry'] = daysToExpiry1
            bsmDataDict['status'] = 'success'
            bsmDataDict['remarks'] = 'success'               
        else:            
            bsmDataDict['status'] = 'failed'
            bsmDataDict['remarks'] = 'FNO Price is zero for ' + tradeSymbol

    except Exception as e:            
        bsmDataDict['status'] = 'failed'
        bsmDataDict['remarks'] = "Errored while getting the BSM Options Pricing for " + tradeSymbol + ": "+ str(e)
    
    return bsmDataDict

def get_margin_data(brokerApi, broker, accountId):
    try:
        if (broker == 'ZERODHA'):
            marginData = baf.getKiteMargins(brokerApi)    
            availableCash =  float(marginData['available']['cash'])
            totalCashValue = float(marginData['net']) + float(marginData['utilised']['delivery'])
            usedMargin = float(marginData['utilised']['debits'])
            availableMargin =  float(marginData['net'])
            return availableCash, totalCashValue, usedMargin, availableMargin
        elif (broker == 'PROSTOCKS'):
            marginData = brokerApi.get_limits()

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
    except Exception as e:    
        alertMsg = f"Exception occurred in get_margin_data: {str(e)}"
        util.add_logs(cnx, mySQLCursor, 'ERROR',  alertMsg, sysDict)

def update_cash_positions(cnx, mySQLCursor, tradeAccount, availableCash, totalCashValue, usedMargin, availableMargin):
    try:
        updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")        
        updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET AVAILABLE_MARGIN='"+str(availableMargin)+"', AVAILABLE_CASH='"+str(availableCash)+"', \
            USED_MARGIN='"+str(usedMargin)+"', TOTAL_CASH_VALUE='"+str(totalCashValue)+"', UPDATED_ON= '"+str(updatedOn)+"' WHERE TRADE_ACCOUNT='"+str(tradeAccount)+"'")

        mySQLCursor.execute(updateQuery)
        cnx.commit()
    except Exception as e:    
        alertMsg = f"Exception occurred in update_cash_positions: {str(e)}"
        util.add_logs(cnx, mySQLCursor, 'INFO',  alertMsg, sysDict)

def update_order_status_prostocks(orderData, cnx, mySQLCursor):
    response = {}
    try:
        updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")   
        orderRemarks = ''     
        for orders in orderData:                        
            transactionType = orders["trantype"]            
            orderId = orders["norenordno"]
            orderPrice = orders["prc"]
            exch = orders["exch"]            
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

            if (transactionType == 'B' and exch == 'NFO'):           

                if(orderStatus == 'COMPLETE'):
                    selectStatment = "SELECT AUTO_ID FROM TRADE_TRANSACTIONS WHERE SELL_ORDER_STATUS = 'PENDING' AND SELL_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount     

                    # Check if this is exit trade; if yes, update the profit and loss for EXIT BUY
                    if rowCount == 1:
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_PRICE="+str(orderPrice) + ", SELL_ORDER_STATUS='COMPLETE', TRADE_STATUS='EXITED', SELL_ORDER_DATE='"+str(updatedOn)+"', PROFIT_PERCENT=(("+str(float(orderPrice))+" - BUY_ORDER_PRICE) / BUY_ORDER_PRICE) * -100, PROFIT_AMOUNT=("+str(
                                float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY), TRADE_RESULT = IF ((("+str(float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY)) > 0, 'GAIN','LOSS') WHERE SELL_ORDER_ID ='"+str(orderId)+"' AND TRADE_STATUS IN ('OPEN')")
                    else:
                        # Check if this entry trade; if yes, check for the status of the order and update it
                        selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE BUY_ORDER_STATUS = 'PENDING' AND BUY_ORDER_ID='"+str(orderId)+"'"
                        mySQLCursor.execute(selectStatment)
                        rowCount = mySQLCursor.rowcount                                
                        if rowCount == 1: 
                            updateQuery = ("UPDATE TRADE_TRANSACTIONS SET BUY_ORDER_PRICE="+str(orderPrice) + ", TRADE_STATUS='OPEN', BUY_ORDER_STATUS='COMPLETE', BUY_ORDER_DATE='" + str(updatedOn)+"', BUY_VALUE="+str(buyValue)+" WHERE BUY_ORDER_ID='"+str(orderId) + "'")
                
                elif( orderStatus == 'REJECTED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'PENDING' AND BUY_ORDER_STATUS='PENDING' AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount
                        
                    if rowCount == 1:        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET TRADE_STATUS='REJECTED', BUY_ORDER_STATUS='REJECTED', ORDER_REMARKS ='" + str(orderRemarks) + "' WHERE BUY_ORDER_ID='"+str(orderId) + "'")
                    else:
                        selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'OPEN' AND SELL_ORDER_STATUS = 'PENDING' AND SELL_ORDER_ID='"+str(orderId)+"'"
                        mySQLCursor.execute(selectStatment)
                        rowCount = mySQLCursor.rowcount                                
                        if rowCount == 1: 
                            if (re.search('Check Holdings Including BTST', str(orderRemarks)) or re.search('NRO sqroff not allowed', str(orderRemarks))):
                                updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_STATUS='ABANDONED', SELL_ORDER_REMARKS ='" +
                                                str(orderRemarks) + "' WHERE SELL_ORDER_ID='"+str(orderId) + "'")
                            else:
                                updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_STATUS='REJECTED', SELL_ORDER_REMARKS ='" +
                                                str(orderRemarks) + "' WHERE SELL_ORDER_ID='"+str(orderId) + "'")


            elif( transactionType == 'B' ):
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


            elif (transactionType == 'S' and exch == 'NFO'):           

                if(orderStatus == 'COMPLETE'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE SELL_ORDER_STATUS = 'PENDING' AND SELL_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount     

                    if rowCount == 1: 
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_PRICE="+str(orderPrice) + ", SELL_ORDER_STATUS='COMPLETE', TRADE_STATUS='EXITED', SELL_ORDER_DATE='"+str(updatedOn)+"', PROFIT_PERCENT=(("+str(float(orderPrice))+" - BUY_ORDER_PRICE) / BUY_ORDER_PRICE) * 100, PROFIT_AMOUNT=("+str(
                                float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY), TRADE_RESULT = IF ((("+str(float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY)) > 0, 'GAIN','LOSS') WHERE SELL_ORDER_ID ='"+str(orderId)+"' AND TRADE_STATUS IN ('OPEN')")

                    else:
                        selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE BUY_ORDER_STATUS = 'PENDING' AND BUY_ORDER_ID='"+str(orderId)+"'"
                        mySQLCursor.execute(selectStatment)
                        rowCount = mySQLCursor.rowcount                                
                        if rowCount == 1: 
                            updateQuery = ("UPDATE TRADE_TRANSACTIONS SET BUY_ORDER_PRICE="+str(orderPrice) + ", TRADE_STATUS='OPEN', BUY_ORDER_STATUS='COMPLETE', BUY_ORDER_DATE='" + str(updatedOn)+"', BUY_VALUE="+str(buyValue)+" WHERE BUY_ORDER_ID='"+str(orderId) + "'")


                elif( orderStatus == 'REJECTED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'PENDING' AND BUY_ORDER_STATUS='PENDING' AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount
                        
                    if rowCount == 1:        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET TRADE_STATUS='REJECTED', BUY_ORDER_STATUS='REJECTED', ORDER_REMARKS ='" + str(orderRemarks) + "' WHERE BUY_ORDER_ID='"+str(orderId) + "'")
                    else:
                        selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'OPEN' AND SELL_ORDER_STATUS = 'PENDING' AND SELL_ORDER_ID='"+str(orderId)+"'"
                        mySQLCursor.execute(selectStatment)
                        rowCount = mySQLCursor.rowcount                                
                        if rowCount == 1: 
                            if (re.search('Check Holdings Including BTST', str(orderRemarks)) or re.search('NRO sqroff not allowed', str(orderRemarks))):
                                updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_STATUS='ABANDONED', SELL_ORDER_REMARKS ='" +
                                                str(orderRemarks) + "' WHERE SELL_ORDER_ID='"+str(orderId) + "'")
                            else:
                                updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_STATUS='REJECTED', SELL_ORDER_REMARKS ='" +
                                                str(orderRemarks) + "' WHERE SELL_ORDER_ID='"+str(orderId) + "'")



            elif (transactionType == 'S'):           

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
         
        response['status'] = 'success'        
    
    except Exception as e:    
        response['status'] = 'failed'
        response['remarks'] = f'Exception occurred in update_all_trade_transactions. The error is , {str(e)}'
        
    return response        

def update_all_trade_transactions(cnx, mySQLCursor, brokerApi, tradeAccount):
    response = {}
    
    try:
        selectStatment = f"SELECT AUTO_ID, INSTRUMENT_TOKEN, BUY_ORDER_PRICE, DATE(BUY_ORDER_DATE), \
            QUANTITY, PREV_CLOSE_PRICE, MAX_PROFIT_PERCENT, MAX_PROFIT_AMOUNT, STRATEGY_ID, EXCHANGE, EXCHANGE_TOKEN \
            FROM TRADE_TRANSACTIONS WHERE TRADE_ACCOUNT='{tradeAccount}' AND (TRADE_STATUS IN ('OPEN','P-OPEN') AND (EXIT_SIGNAL_STATUS IS NULL OR EXIT_SIGNAL_STATUS = '')) \
                OR (TRADE_STATUS = 'OPEN' AND EXIT_SIGNAL_STATUS NOT IN ('EXIT SIGNAL') AND SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE')) \
                    OR (TRADE_STATUS = 'OPEN' AND EXIT_SIGNAL_STATUS IN ('EXIT SIGNAL') AND SELL_ORDER_STATUS IN ('ABANDONED'))"

 
        mySQLCursor.execute(selectStatment)
        rowCount = mySQLCursor.rowcount     
        if rowCount > 0:      

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
                maxProfitPercent = row[6]
                maxProfitAmount = row[7]
                strategyId = row[8]
                exchange = row[9]
                exchangeToken = row[10]
                dayChageprofitPercent = 0
                dayChangeprofitAmount = 0

                try:            
                    
                    ltpPrice = prostocksApi.get_last_traded_price(brokerApi, exchangeToken, exchange='NFO')

                    # get profit for given qty
                    profitPercent, profitAmount = util.get_profit(float(entryPrice), float(ltpPrice), quantity)

                    if (float(profitPercent) > float(maxProfitPercent)):
                        maxProfitPercent = profitPercent
                        maxProfitAmount = profitAmount

                    if (float(prevClosePrice) == 0):
                        dayChageprofitPercent, dayChangeprofitAmount = util.get_profit(float(entryPrice), float(ltpPrice), quantity)
                    else:
                        dayChageprofitPercent, dayChangeprofitAmount = util.get_profit(float(prevClosePrice), float(ltpPrice), quantity)

                    actualHorizon = util.get_date_difference(entryDate)            
                        
                    updateQuery = "UPDATE TRADE_TRANSACTIONS SET UPDATED_ON = %s, CURRENT_MKT_PRICE=%s, PROFIT_PERCENT=%s, PROFIT_AMOUNT=%s, \
                        ACTUAL_HORIZON=%s, TODAY_PROFIT_PCT=%s, TODAY_PROFIT=%s, MAX_PROFIT_AMOUNT=%s, MAX_PROFIT_PERCENT=%s WHERE AUTO_ID= %s"
                    
                    updateVal = []
                    updateVal.insert(0, str(currDateTime))            
                    updateVal.insert(1, str(ltpPrice))
                    updateVal.insert(2, str(profitPercent))
                    updateVal.insert(3, str(profitAmount))                                            
                    updateVal.insert(4, str(actualHorizon))
                    updateVal.insert(5, str(dayChageprofitPercent))
                    updateVal.insert(6, str(dayChangeprofitAmount))
                    updateVal.insert(7, str(maxProfitAmount))
                    updateVal.insert(8, str(maxProfitPercent))
                    updateVal.insert(9, str(autoId))
                    updateArrayValues.insert(tmpCnt, updateVal)
                    
                    tmpCnt += 1  
                    if (tmpCnt == 50):                
                        mySQLCursor.executemany(updateQuery, updateArrayValues) 
                        cnx.commit()   
                        tmpCnt = 0   
                        updateArrayValues = []

                except Exception as e:                                 
                    alertMsg = f"Exception occurred while updating indivtual CMP records in update_all_trade_transactions: {str(e)}"
                    util.add_logs(cnx, mySQLCursor, 'ERROR',  alertMsg, sysDict)
                    pass
            
            if (tmpCnt > 0):       
                mySQLCursor.executemany(updateQuery, updateArrayValues)
                cnx.commit()    

        response['status'] = 'success'        

    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Exception occurred in update_all_trade_transactions. The error is, ' + str(e)

    return response        

def update_addtional_trans_data(cnx, mySQLCursor, brokerApi, tradeAccount):
    response = {}
    try:
        selectStatment = f"SELECT AUTO_ID, INSTRUMENT_TOKEN, TRADE_SYMBOL, OPTION_STRIKE_PRICE, EXPIRY_DATE, BASE_INSTRUMENT_TOKEN, EXCHANGE, EXCHANGE_TOKEN, \
            BASE_EXCHANGE_TOKEN, INSTRUMENT_TYPE FROM TRADE_TRANSACTIONS WHERE TRADE_ACCOUNT='{tradeAccount}' AND TRADE_STATUS ='OPEN' AND INSTRUMENT_TYPE IN ('PUT', 'CALL')"
        mySQLCursor.execute(selectStatment)
        rowCount = mySQLCursor.rowcount     
        # Profit and Loss update for EXIT BUY
        if rowCount > 0:      
            results = mySQLCursor.fetchall()                
            currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')            
            for row in results:

                ttAutoId = row[0] 
                futInstToken = row[1] 
                tradeSymbol = row[2] 
                strikePrice = row[3]  
                rawExpiry = row[4]  
                baseInstToken = str(row[5])                
                exchange = row[6]
                exchangeToken = str(row[7])
                baseExchangeToken = str(row[8])
                instrumentType = row[9]
                
                # Get the LTP for base insturments
                baseLtpPrice = prostocksApi.get_last_traded_price(brokerApi, baseExchangeToken, exchange='NSE')

                bsmDataDict = bsm_options_pricing(brokerApi, exchangeToken, strikePrice, rawExpiry, baseLtpPrice, tradeSymbol, instrumentType)
                
                if (bsmDataDict['status'] == 'success'):

                    updateQuery = f"UPDATE TRANSACTIONS_ADDITIONAL_DATA SET UPDATED_ON='{currDateTime}', CURRENT_DELTA={bsmDataDict['delta']}, CURRENT_THETA={bsmDataDict['theta']}, \
                        CURRENT_GAMMA={bsmDataDict['gamma']}, CURRENT_VEGA={bsmDataDict['vega']}, CURRENT_IMPLIED_VOLATILITY={bsmDataDict['IMPLIED_VOLATILITY']} WHERE TT_AUTO_ID={ttAutoId}" 
                
                    mySQLCursor.execute(updateQuery) 
                    cnx.commit()   
            
        response['status'] = 'success'     
    except Exception as e:                                 
        response['status'] = 'failed'
        response['remarks'] = 'Exception occurred while updating update_addtional_trans_data. The error is, ' + str(e)    

    return response

def update_max_loss(cnx, mySQLCursor, tradeAccount):
    response = {}
    try:
        selectQuery = f"SELECT O.BUY_VALUE, F.BUY_ORDER_PRICE, O.OPTION_STRIKE_PRICE, ABS(O.QUANTITY), A.POSITIONS_GROUP_ID, O.AUTO_ID, F.AUTO_ID FROM TRADE_TRANSACTIONS AS A \
                    LEFT JOIN TRADE_TRANSACTIONS AS O ON O.POSITIONS_GROUP_ID = A.POSITIONS_GROUP_ID AND (O.INSTRUMENT_TYPE='CALL' OR O.INSTRUMENT_TYPE='PUT')  AND O.TRADE_STATUS = 'OPEN' \
                    LEFT JOIN TRADE_TRANSACTIONS AS F ON F.POSITIONS_GROUP_ID = A.POSITIONS_GROUP_ID AND F.INSTRUMENT_TYPE='FUT' AND F.TRADE_STATUS = 'OPEN'  \
                    WHERE A.TRADE_ACCOUNT='{tradeAccount}' AND A.STRATEGY_ID IN ('ALPHA_10A', 'ALPHA_10B', 'ALPHA_4A') AND A.TRADE_STATUS ='OPEN' AND A.MAX_LOSS = 0	GROUP BY A.POSITIONS_GROUP_ID"
                    
        mySQLCursor.execute(selectQuery)
        rowCount = mySQLCursor.rowcount     
        # Profit and Loss update for EXIT BUY
        if rowCount == 1:                                         
            results = mySQLCursor.fetchall()                                                            
        
            for row in results:                                            
                oBuyValue =  0 if row[0] is None else float(row[0])
                fBuyOrderPrice= 0 if row[1] is None else float(row[1])
                oOptionStrikePrice = 0 if row[2] is None else float(row[2])
                quantity = 0 if row[3] is None else int(row[3])
                posGroupId = row[4]
                oAutoId = row[5]
                fAutoId = row[6]
                maxLoss = abs(oBuyValue + ((fBuyOrderPrice - oOptionStrikePrice) * quantity))

                updateQuery = f"UPDATE TRADE_TRANSACTIONS SET MAX_LOSS={maxLoss} WHERE POSITIONS_GROUP_ID = '{posGroupId}'"
        
                mySQLCursor.execute(updateQuery) 
                cnx.commit()

        response['status'] = 'success'     
    except Exception as e:                                 
        response['status'] = 'failed'
        response['remarks'] = 'Exception occurred while updating update_max_loss. The error is, ' + str(e)    

    return response

def send_staged_order_to_broker(cnx, mySQLCursor, broker, accountId, brokerApi):
    response = {}
    try:
        # Check if the sell is having a corresponding adjustment order                                    
        selectStatment = f"SELECT TRADE_DIRECTION, ABS(QUANTITY), TRADE_SYMBOL, INSTRUMENT_TOKEN, AUTO_ID, POSITIONS_GROUP_ID, \
            EXCHANGE_TOKEN FROM TRADE_TRANSACTIONS WHERE BUY_ORDER_ID = '77777' AND TRADE_ACCOUNT='{accountId}' AND TRADE_STATUS='PENDING'"
        mySQLCursor.execute(selectStatment)                                                                                    
        rowCount = mySQLCursor.rowcount
        if ( rowCount > 0 ):
            tradeDataDict = {}
            tradeDataDict['accountId'] = accountId
            tradeDataDict['broker'] = broker                                       
            tradeDataDict['exchange'] = 'NFO'
            tradeDataDict['futOrderType'] = 'LMT'            
            results = mySQLCursor.fetchall() 
            for row in results:                                            
                tradeDataDict['futTransactionType'] = row[0]
                tradeDataDict['quantity'] = int(row[1])
                tradeDataDict['futTradeSymbol'] = row[2]
                tradeDataDict['futInstToken'] = row[3]                
                autoId = row[4]
                posGroupId = row[5]
                tradeDataDict['futExchangeToken'] = row[6]

                # Make sure that there are no other pending orders in the position group
                selectStatment = f"SELECT AUTO_ID FROM TRADE_TRANSACTIONS WHERE POSITIONS_GROUP_ID='{posGroupId}' AND BUY_ORDER_STATUS='PENDING' AND AUTO_ID NOT IN ({autoId})"
                mySQLCursor.execute(selectStatment)                                                                                    
                rowCount = mySQLCursor.rowcount
                
                # Proceed to place the adjustment order only when it doesn't have any other pending orders
                if ( rowCount == 0 ):
                    tradeDataDict['futTriggerPrice'] = util.get_best_trigger_price(brokerApi, tradeDataDict['futExchangeToken'], 'NFO', tradeDataDict['futTransactionType'])

                    tradeDataDict['futLastPrice'] = tradeDataDict['futTriggerPrice']
                    orderId, orderRemarks  = baf.place_future_buy_order(brokerApi, tradeDataDict)
                    
                    if (int(orderId) > 0):                                                
                        updateQuery = f"UPDATE TRADE_TRANSACTIONS SET BUY_ORDER_ID='{orderId}' WHERE AUTO_ID = '{autoId}'"
                        mySQLCursor.execute(updateQuery)
                        cnx.commit()                                                
                        
                        alertMsg = f"Staged entry order has been sent to broker with order ID {orderId} \nTrade:  Entry {tradeDataDict['futTransactionType']} \nInstrument Name: {tradeDataDict['futTradeSymbol']} \nEntry Price: {str('%.2f' % tradeDataDict['futLastPrice'])}"                                                
                        util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)
       
        response['status'] = 'success'     
    
    except Exception as e:                                 
        response['status'] = 'failed'
        response['remarks'] = 'Exception occurred while send_dummy_order_to_broker. The error is, ' + str(e)    

    return response


def exit_dummy_orders(cnx, mySQLCursor, broker, accountId, brokerApi):
    response = {}

    try:
        # Check if the sell is having a corresponding adjustment order                                    
        selectStatment = f"SELECT TRADE_DIRECTION, ABS(QUANTITY), TRADE_SYMBOL, INSTRUMENT_TOKEN, AUTO_ID, POSITIONS_GROUP_ID, EXCHANGE_TOKEN \
            FROM TRADE_TRANSACTIONS WHERE SELL_ORDER_ID = '66666' AND TRADE_ACCOUNT='{accountId}' AND TRADE_STATUS='OPEN'"
        mySQLCursor.execute(selectStatment)                                                                                    
        rowCount = mySQLCursor.rowcount
        if ( rowCount > 0 ):
            tradeDataDict = {}
            tradeDataDict['accountId'] = accountId
            tradeDataDict['broker'] = broker                                       
            tradeDataDict['exchange'] = 'NFO'
            tradeDataDict['futOrderType'] = 'LMT'
            results = mySQLCursor.fetchall() 
            for row in results:                                            
                futTransactionType = row[0]
                tradeDataDict['quantity'] = int(row[1])
                tradeDataDict['futTradeSymbol'] = row[2]
                tradeDataDict['futInstToken'] = row[3]
                autoId = row[4]
                posGroupId = row[5]
                tradeDataDict['futExchangeToken'] = row[6]

                if (futTransactionType == 'SELL'):
                    tradeDataDict['futTransactionType'] = 'BUY'                    
                else:
                    tradeDataDict['futTransactionType'] = 'SELL'

                # Make sure that there are no other pending orders in the position group
                selectStatment = f"SELECT AUTO_ID FROM TRADE_TRANSACTIONS WHERE POSITIONS_GROUP_ID='{posGroupId}' AND (TRADE_STATUS='OPEN' AND SELL_ORDER_STATUS='PENDING') AND AUTO_ID NOT IN ({autoId})"
                mySQLCursor.execute(selectStatment)                                                                                    
                rowCount = mySQLCursor.rowcount
                
                # Proceed to place the adjustment order only when it doesn't have any other pending orders
                if ( rowCount == 0 ):
                    
                    tradeDataDict['futTriggerPrice'] = util.get_best_trigger_price(brokerApi, tradeDataDict['futExchangeToken'], 'NFO', tradeDataDict['futTransactionType'])
                    tradeDataDict['futLastPrice'] = tradeDataDict['futTriggerPrice']
                    orderId, orderRemarks  = baf.place_future_buy_order(brokerApi, tradeDataDict)
                    
                    if (int(orderId) > 0):                                                
                        updateQuery = f"UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_ID='{orderId}' WHERE AUTO_ID = '{autoId}'"
                        mySQLCursor.execute(updateQuery)
                        cnx.commit()                                                
                        
                        alertMsg = f"Exit order has been sent to broker with order ID {orderId} \nTrade:  Exit {tradeDataDict['futTransactionType']} \nInstrument Name: {tradeDataDict['futTradeSymbol']} \Exit Price: {str('%.2f' % tradeDataDict['futLastPrice'])}"                                                
                        util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)
       
        response['status'] = 'success'     
    
    except Exception as e:                                 
        response['status'] = 'failed'
        response['remarks'] = 'Exception occurred while place_adjusment_entry_order. The error is, ' + str(e)    

    return response

def place_adjusment_entry_order(cnx, mySQLCursor, broker, accountId, brokerApi):
    response = {}

    try:
        # Check if the sell is having a corresponding adjustment order                                    
        selectStatment = f"SELECT TRADE_DIRECTION, ABS(QUANTITY), TRADE_SYMBOL, INSTRUMENT_TOKEN, AUTO_ID, POSITIONS_GROUP_ID \
            FROM TRADE_TRANSACTIONS WHERE BUY_ORDER_ID = '88888' AND TRADE_ACCOUNT='{accountId}' AND TRADE_STATUS='PENDING'"
        mySQLCursor.execute(selectStatment)                                                                                    
        rowCount = mySQLCursor.rowcount
        if ( rowCount > 0 ):
            tradeDataDict = {}
            tradeDataDict['accountId'] = accountId
            tradeDataDict['broker'] = broker                                       
            tradeDataDict['exchange'] = 'NFO'
            tradeDataDict['futOrderType'] = 'LMT'
            results = mySQLCursor.fetchall() 
            for row in results:                                            
                tradeDataDict['futTransactionType'] = row[0]
                tradeDataDict['quantity'] = int(row[1])
                tradeDataDict['futTradeSymbol'] = row[2]
                tradeDataDict['futInstToken'] = row[3]
                autoId = row[4]
                posGroupId = row[5]

                # Make sure that there are no other pending orders in the position group
                selectStatment = f"SELECT AUTO_ID FROM TRADE_TRANSACTIONS WHERE POSITIONS_GROUP_ID='{posGroupId}' AND SELL_ORDER_STATUS='PENDING' AND AUTO_ID NOT IN ({autoId})"
                mySQLCursor.execute(selectStatment)                                                                                    
                rowCount = mySQLCursor.rowcount
                
                # Proceed to place the adjustment order only when it doesn't have any other pending orders
                if ( rowCount == 0 ):
                    tradeDataDict['futTriggerPrice'] = util.get_best_trigger_price(brokerApi, tradeDataDict['futInstToken'], tradeDataDict['exchange'], tradeDataDict['futTransactionType'])

                    tradeDataDict['futLastPrice'] = tradeDataDict['futTriggerPrice']
                    orderId, orderRemarks  = baf.place_future_buy_order(brokerApi, tradeDataDict)
                    
                    if (int(orderId) > 0):                                                
                        updateQuery = f"UPDATE TRADE_TRANSACTIONS SET BUY_ORDER_ID='{orderId}' WHERE AUTO_ID = '{autoId}'"
                        mySQLCursor.execute(updateQuery)
                        cnx.commit()                                                
                        
                        alertMsg = f"Adjustment order has been sent to broker with order ID {orderId} \nTrade:  Entry {tradeDataDict['futTransactionType']} \nInstrument Name: {tradeDataDict['futTradeSymbol']} \nEntry Price: {str('%.2f' % tradeDataDict['futLastPrice'])}"                                                
                        util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)
       
        response['status'] = 'success'     
    
    except Exception as e:                                 
        response['status'] = 'failed'
        response['remarks'] = 'Exception occurred while place_adjusment_entry_order. The error is, ' + str(e)    

    return response

def modify_open_prostocks_orders(cnx, mySQLCursor, accountId, sysDict, orderData, brokerApi):
    response = {}
    try:
        orderId = 0
        transactionType = ''
        quantity = 0
        
        for orders in orderData:                            
            posTradeSymbol = orders["tsym"]
            orderStatus = orders["status"] 
            quantity = orders["qty"]
            orderId = orders["norenordno"]
            transactionType = orders["trantype"]
            lastOrderPrice = float(orders["prc"])
            
            if (int(quantity) != 0 and orderStatus == 'OPEN'):
                
                instSelectionQuery = f"SELECT INSTRUMENT_TOKEN FROM TRADE_TRANSACTIONS WHERE BUY_ORDER_ID='{orderId}' OR SELL_ORDER_ID='{orderId}' ORDER BY AUTO_ID DESC LIMIT 1"
                mySQLCursor.execute(instSelectionQuery)
                results = mySQLCursor.fetchall()                    
                for row in results:
                    instToken = str(row[0])
                    
                    triggerPrice = 0
                    alertMsg  = ""
                    _tmpTriggerPrice = util.get_best_trigger_price(brokerApi, instToken, 'NFO', transactionType)

                    if (transactionType == 'B'):    
                        alertMsg = f"Buy - {posTradeSymbol} - lastOrderPrice: {lastOrderPrice} ; New Price: {_tmpTriggerPrice}"
                        if (lastOrderPrice != _tmpTriggerPrice): 
                            triggerPrice = _tmpTriggerPrice
                    else:
                        alertMsg = f"Sell - {posTradeSymbol} - lastOrderPrice: {lastOrderPrice} ; New Price: {_tmpTriggerPrice}"
                        if (lastOrderPrice != _tmpTriggerPrice): 
                            triggerPrice = _tmpTriggerPrice


                    if (triggerPrice != 0): 
                        util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, sysDict)
                        jsonResp = brokerApi.modify_order(orderId, 'NFO', tradingsymbol=posTradeSymbol, newquantity=quantity, newprice_type='LMT', newprice=triggerPrice)

                        if (jsonResp != None and jsonResp['stat'] == 'Ok'):                              
                            orderId = jsonResp['result']
                            alertMsg=f"Modify order completed for {posTradeSymbol} with order id: {str(orderId)} in account: {str(accountId)}"
                            util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, sysDict)  

        response['status'] = 'success'     
    
    except Exception as e:                                 
        response['status'] = 'failed'
        response['remarks'] = f"Exception occured in modify_open_prostocks_orders: The error is {str(e)}"

    return response


def process_updates(tradeAccount):
    accountId = tradeAccount[0]
    broker = tradeAccount[1]  
    global sysDict
    cnx, mySQLCursor = util.connect_mysql_db()
    
    brokerApi = None
    isApiConnected = False

    if (broker == 'PROSTOCKS'):
        brokerApi, isApiConnected = prostocksApi.connect_broker_api(cnx, mySQLCursor, accountId, broker)
    
        try: 
            
            try:
                orderData = brokerApi.get_order_book()
                if (orderData != None and len(orderData) > 0):
                    update_order_status_prostocks(orderData, cnx, mySQLCursor)
                    
                    response = modify_open_prostocks_orders(cnx, mySQLCursor, accountId, sysDict, orderData,brokerApi)
                    if (response['status'] == 'failed'):                                  
                        util.add_logs(cnx, mySQLCursor, 'ERROR',  response['remarks'], sysDict)
            except:
                pass

            availableCash, totalCashValue, usedMargin, availableMargin = get_margin_data(brokerApi, broker, accountId) 
            
            if (availableCash != -1):
                update_cash_positions(cnx, mySQLCursor, accountId, availableCash, totalCashValue, usedMargin, availableMargin) 
            
            # Update all the transactions PNL and current market prices
            response = update_all_trade_transactions(cnx, mySQLCursor, brokerApi, accountId)
            if (response['status'] == 'failed'):                                  
                util.add_logs(cnx, mySQLCursor, 'ERROR',  response['remarks'], sysDict)

            # Update the additional options data such as IV, theta, gamma etc...
            response = update_addtional_trans_data(cnx, mySQLCursor, brokerApi, accountId)
            if (response['status'] == 'failed'):                                  
                util.add_logs(cnx, mySQLCursor, 'ERROR',  response['remarks'], sysDict)
            
            # Update the max loss for certain strategies
            response = update_max_loss(cnx, mySQLCursor, accountId)
            if (response['status'] == 'failed'):                                  
                util.add_logs(cnx, mySQLCursor, 'ERROR',  response['remarks'], sysDict)

            response = send_staged_order_to_broker(cnx, mySQLCursor, broker, accountId, brokerApi)
            if (response['status'] == 'failed'):                                  
                util.add_logs(cnx, mySQLCursor, 'ERROR',  response['remarks'], sysDict)

            # Place the adjustment order
            response = place_adjusment_entry_order(cnx, mySQLCursor, broker, accountId, brokerApi)
            if (response['status'] == 'failed'):                                  
                util.add_logs(cnx, mySQLCursor, 'ERROR',  response['remarks'], sysDict)

            # Exit the hedge orders (placed as dummy orders)
            response = exit_dummy_orders(cnx, mySQLCursor, broker, accountId, brokerApi)
            if (response['status'] == 'failed'):                                  
                util.add_logs(cnx, mySQLCursor, 'ERROR',  response['remarks'], sysDict)

        except Exception as e:
            alertMsg = 'Exceptions occured while updating PROSTOCKS orders and margin details: ' + str(e)                  
            util.add_logs(cnx, mySQLCursor, 'ERROR',  alertMsg, sysDict)


                 
# Main function is called by default, and the first function to be executed
if __name__ == "__main__":    

    telegramAdminIds = Config.TELG_ADMIN_ID
    adminTradeAccount = Config.ADMIN_TRADE_ACCOUNT
    adminTradeBroker = Config.ADMIN_TRADE_BROKER
    programName = os.path.splitext(os.path.basename(__file__))[0]
    sysDict = {}
    sysDict['programName'] = programName
    sysDict['telegramAdminIds'] = telegramAdminIds

    # Initialized the log files 
    util.initialize_logs(str(programName) + '.log')

    programExitFlag = True
    
    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db()
    
    alertMsg = f"The program ({programName}) started"
    util.add_logs(cnx, mySQLCursor, 'ALERT', alertMsg, sysDict)            
    # Continuously run the program until the exit flag turns to False
    while programExitFlag:
        try:
            currDate = util.get_date_time_formatted("%Y-%m-%d")


            # Verify whether the connection to MySQL database is open
            cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)

            cycleStartTime = util.get_system_time()                 
            sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
            currentTime = util.get_date_time_formatted("%H%M")

            if (Config.TESTING_FLAG or (int(currentTime) <= int(sysSettings['SYSTEM_UPDATE_SVC_END_TIME']))):
                start=datetime.datetime.now()
                
                activeAccountsList =  util.get_active_trade_accounts(mySQLCursor, entryOrExit = 'EXIT')
                
                start = time.perf_counter()
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:

                    results = executor.map(process_updates, activeAccountsList)
                
                end = time.perf_counter()

                print (f"Completed in {end - start}")

            else:                    
                util.add_logs(cnx, mySQLCursor, 'INFO',  'System end time reached; exiting the program now' , sysDict)
                programExitFlag = False
            
            util.update_program_running_status(cnx, mySQLCursor, programName, 'ACTIVE')
            
            cnx.commit()
        
        except Exception as e:
            alertMsg = 'Live trade update service failed (main block): '+ str(e)
            util.add_logs(cnx, mySQLCursor, 'ERROR',  alertMsg, sysDict)

    
    util.disconnect_db(cnx, mySQLCursor)