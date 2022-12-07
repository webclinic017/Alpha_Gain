from utils import util_functions as util
from utils import broker_api_functions as baf
import time
import logging
import datetime
import requests
import re
from config import Config
import os
import mibian
import concurrent.futures
from utils import prostocks_api_helper as prostocksApi


def bsm_options_pricing_new(brokerApi, instToken, strikePrice, expiry, closePrice, tradeSymbol, instrumentType):
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
    
         # quoteData = prostocksApi.get_quotes(brokerApi, instToken)
        quoteData = prostocksApi.get_quotes(brokerApi, instToken, exchange='NFO')

        fnoLastPrice = float(quoteData['lp'])

        bsmDataDict = {}
        if (float(fnoLastPrice) > 0):
            # bsmDataDict['OI_VALUE'] = float(quoteData[instToken]['oi']) * float(closePrice)
            bsmDataDict['VOLUME_VALUE']= float(quoteData['v']) * float(closePrice)
            if (instrumentType == 'PUT'):
                iv = mibian.BS([closePrice, strikePrice, interestRate, daysToExpiry1], putPrice=fnoLastPrice)                
                c = mibian.BS([closePrice, strikePrice, interestRate, daysToExpiry1], volatility=float("{:.2f}".format(iv.impliedVolatility)))
                bsmDataDict['delta'] = float("{:.2f}".format(c.putDelta))
                bsmDataDict['theta'] = float("{:.2f}".format(c.putTheta))
            else:
                iv = mibian.BS([closePrice, strikePrice, interestRate, daysToExpiry1], callPrice=fnoLastPrice)
                c = mibian.BS([closePrice, strikePrice, interestRate, daysToExpiry1], volatility=float("{:.2f}".format(iv.impliedVolatility)))
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
        bsmDataDict['status'] = 'error'
        bsmDataDict['remarks'] = "Errored while getting the BSM Options Pricing for " + tradeSymbol + ": "+ str(e)
        
    return bsmDataDict

def get_strike_by_delta(mySQLCursor, brokerApi, closePrice, expiry, tradeSymbol, instrumentType, testedDelta, recalledCnt = 0):
    try:

        if ( instrumentType == 'CALL' ):
            selectStatment = f"SELECT instrument_token, strike, tradingsymbol, tick_size, lot_size \
                FROM INSTRUMENTS WHERE name='{tradeSymbol}' and expiry='{expiry}' AND exchange='NFO' AND instrument_type='CE' AND strike <= '{closePrice + 1000}' ORDER BY strike DESC LIMIT {15 + recalledCnt}"
        else:
            selectStatment = f"SELECT instrument_token, strike, tradingsymbol, tick_size, lot_size \
                FROM INSTRUMENTS WHERE name='{tradeSymbol}' and expiry='{expiry}' AND exchange='NFO' AND instrument_type='PE' AND strike >= '{closePrice - 1000}' ORDER BY strike ASC LIMIT {15 + recalledCnt}"


        fDate = datetime.datetime.strptime(str(util.get_date_time_formatted("%Y-%m-%d")), "%Y-%m-%d")
        lDate = datetime.datetime.strptime(str(expiry), "%Y-%m-%d")

        daysToExpiry = lDate - fDate
        
        if (daysToExpiry.days == 0):
            daysToExpiry = 1
        else:
            daysToExpiry = int(daysToExpiry.days)

        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()

        # impliedVolCallList = []
        callPriceBSMList = []
        deltaCallList = []
        thetaCallList = []
        gammaList = []
        vegaList = []
        strikePriceList = []

        impliedVolList = []
        interestRate = 6
        fnoTradingSymbolList = []
        fnoInstrumentTokenList = []



        for row in results:
        
            oInstrumentToken = str(row[0])
            oStrikePrice = row[1]
            fnoTradingSymbol = row[2]
            
            strikePriceList.append(oStrikePrice)
            fnoTradingSymbolList.append(fnoTradingSymbol)
            fnoInstrumentTokenList.append(oInstrumentToken)

            if (instrumentType == 'PUT'):
                bsmDataDict = bsm_options_pricing_new(brokerApi, oInstrumentToken, oStrikePrice, expiry, closePrice, fnoTradingSymbol, 'PUT')
                
            else:                    
                bsmDataDict = bsm_options_pricing_new(brokerApi, oInstrumentToken, oStrikePrice, expiry, closePrice, fnoTradingSymbol, 'CALL')
            
            
            impliedVolList.append(bsmDataDict['IMPLIED_VOLATILITY'])
            deltaCallList.append(bsmDataDict['delta'])
            thetaCallList.append(bsmDataDict['theta'])
            gammaList.append(bsmDataDict['gamma'])
            vegaList.append(bsmDataDict['vega'])


        if (len(strikePriceList) > 0):
            # Get the closest delta value to 0.40 (40)
            if (instrumentType == 'PUT'):
                closestStrikeIndex = min(range(len(deltaCallList)), key=lambda i: abs(deltaCallList[i] + abs(testedDelta)))
            else:
                closestStrikeIndex = min(range(len(deltaCallList)), key=lambda i: abs(deltaCallList[i] - abs(testedDelta)))

            # Recursive call to recalculate the delta as the tested strike did not have close match.
            if (abs(abs(deltaCallList[closestStrikeIndex]) - (abs(testedDelta)) > 0.3)):
                _tmpResp = get_strike_by_delta(mySQLCursor, brokerApi, closePrice, expiry, tradeSymbol, instrumentType, testedDelta, recalledCnt = recalledCnt + 5)

            logging.info(f"deltaCallList: {deltaCallList}")
            selectedDelta = deltaCallList[closestStrikeIndex]
            selectedStrikePrice = strikePriceList[closestStrikeIndex]
            selectedImpliedVol = impliedVolList[closestStrikeIndex]
            selectedTradingSymbol =  fnoTradingSymbolList[closestStrikeIndex]
            selectedInsturmentToken = fnoInstrumentTokenList[closestStrikeIndex]

            response = {}   
            response['status'] = 'success'
            response['remarks'] = 'success'              
            response['futInstToken'] = selectedInsturmentToken            
            response['futTradeSymbol'] = selectedTradingSymbol            
            response['selectedImpliedVol'] = selectedImpliedVol
            response['selectedStrikePrice'] = selectedStrikePrice
            response['selectedDelta'] = selectedDelta

            logging.info(f"Selected DELTA response : {response}")
  
        else:  
            response = {}   
            response['status'] = 'failed'
            response['remarks'] = 'Strike Price List doesn\'t have any value for ' + tradeSymbol
            

    except Exception as e:            
        response = {}   
        response['status'] = 'error'
        response['remarks'] = "Errored while getting the BSM CALL Options Pricing for " + tradeSymbol + ": "+ str(e)
        return response
    
    return response

def get_exit_positions_list(mySQLCursor):
    currDate = util.get_date_time_formatted('%Y-%m-%d')
    
    selectStatment = "SELECT TT.AUTO_ID, TT.TRADE_ACCOUNT, UTA.BROKER, TT.TRADE_SYMBOL, TT.STOCK_NAME, TT.QUANTITY, TT.PRODUCT_TYPE, TT.EXCHANGE, INSTRUMENT_TOKEN  FROM TRADE_TRANSACTIONS TT\
         LEFT JOIN USR_TRADE_ACCOUNTS UTA ON TT.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT WHERE (TT.EXIT_SIGNAL_STATUS = 'EXIT SIGNAL' AND TT.TRADE_STATUS = 'OPEN' AND (TT.SELL_ORDER_STATUS NOT IN ('PENDING', 'ABANDONED', 'TIMED OUT', 'REJECTED', 'COMPLETE'))) OR (TT.EXIT_SIGNAL_STATUS = 'EXIT SIGNAL' AND TT.TRADE_STATUS = 'OPEN' AND (TT.SELL_ORDER_DATE IS NULL OR DATE(TT.SELL_ORDER_DATE) <> '"+str(currDate)+"'))"
    
    mySQLCursor.execute(selectStatment)
    return mySQLCursor.fetchall()

def place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict, dummyExitFlag = 'N'):
    try:
        currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S') 
        orderId = 0        
        existingOrderFromBroker, proStocksOrderId, proStocksTransType, proStocksQty = baf.get_prostocks_orders(brokerApi, tradeDataDict['futTradeSymbol'], inTransType=tradeDataDict['futTransactionType'])
        existingOrderFromDB = util.check_existing_exit_positions(mySQLCursor, tradeDataDict)

        if (existingOrderFromBroker or existingOrderFromDB):
            alertMsg = f"Existing order fround for {tradeDataDict['futTradeSymbol']}"
            util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, tradeDataDict)


        # Modify the existing orders
        else:
            existingPositionFound, existingOrderQuantity  = baf.get_prostocks_positions(brokerApi, tradeDataDict['futTradeSymbol'])            

            if (existingPositionFound):            
                tradeDataDict['futTriggerPrice'] = util.get_best_trigger_price(brokerApi, tradeDataDict['exchangeToken'], 'NFO', tradeDataDict['futTransactionType'])

                if (tradeDataDict['futTriggerPrice'] != None):
                    tradeDataDict['futBuyValue'] = tradeDataDict['quantity'] * float(tradeDataDict['futTriggerPrice'])  
                    tradeDataDict['futLastPrice'] = tradeDataDict['futTriggerPrice']
                    profitPercent, profitAmount = util.get_profit(float(tradeDataDict['entryPrice']), float(tradeDataDict['futTriggerPrice']), tradeDataDict['quantity'])

                    alertMsg = f"Exiting {tradeDataDict['futTradeSymbol']}"
                    util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, tradeDataDict)

                    if (dummyExitFlag == 'N'):
                        orderId, orderRemarks  = baf.place_future_buy_order(brokerApi, tradeDataDict)
                    else:
                        orderId = 66666
                        orderRemarks = 'DUMMY EXIT'
                    
                    if (isinstance(orderId, list) and orderId[0] > 0):
                        orderId = orderId[0]                          

                    if (int(orderId) > 0):                    
                        
                        alertMsg = f"Exit {str(tradeDataDict['futTransactionType'])} with order id {str(orderId)}"
                        util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, tradeDataDict)
                        updateQuery = "UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_DATE='"+str(currDateTime)+"', UPDATED_ON='"+str(currDateTime)+"', SELL_ORDER_STATUS='PENDING', \
                                        SELL_ORDER_ID='"+str(orderId)+"' WHERE AUTO_ID="+str(tradeDataDict['autoId'])
                        mySQLCursor.execute(updateQuery) 
                        cnx.commit() 

                        # Update the USR_STRATEGY_SUBSCRIPTIONS that the strategy can take new positions as it just exited from a positions
                        updateQuery = (f"UPDATE USR_STRATEGY_SUBSCRIPTIONS SET ALLOCATED_CASH_EXCEEDED_FLAG='N' WHERE STRATEGY_ID ='{tradeDataDict['strategyId']}' AND TRADE_ACCOUNT='{tradeDataDict['accountId']}'")
                        mySQLCursor.execute(updateQuery)
                        cnx.commit()

                        alertMsg = f"Strategy: {tradeDataDict['strategyId']} \nTrade:  Exit {tradeDataDict['futTransactionType']} \nInstrument Name: {tradeDataDict['futTradeSymbol']} \nExit Price: {str('%.2f' % tradeDataDict['futLastPrice'])} \nProfit/Loss:  {str('%.2f' % profitAmount)} \nTraded Value:  {str('%.2f' % tradeDataDict['futBuyValue'])} \nTrade Initialized At: {tradeDataDict['signalDate']}"
                        util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)                        

    except Exception as e:    
        alertMsg = f"Exceptions occured place_fno_exit_order: {str(e)}"        
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, tradeDataDict)

def pl_based_exit(cnx, mySQLCursor, brokerApi, tradeDataDict):
    try:
        selectStatment = f"""SELECT AUTO_ID,
                            TRADE_ACCOUNT,                           
                            STRATEGY_ID,
                            TRADE_SYMBOL,
                            INSTRUMENT_TOKEN,
                            BASE_INSTRUMENT_TOKEN,
                            QUANTITY,
                            TRADE_DIRECTION,
                            INSTRUMENT_TYPE,
                            BUY_ORDER_PRICE,
                            CURR_SL_PCT,
                            TGT_STOP_LOSS_PCT,
                            TGT_PROFIT_PCT,
                            TRAILING_THRESHOLD_PCT,
                            TRADE_STATUS,                            
                            EXCHANGE_TOKEN

                        FROM TRADE_TRANSACTIONS TT WHERE EXIT_STRATEGY_ID='{tradeDataDict['exitStrategy']}' 
                            AND TRADE_ACCOUNT='{tradeDataDict['accountId']}' 
                            AND (TRADE_STATUS IN ('OPEN','P-OPEN') AND (EXIT_SIGNAL_STATUS IS NULL OR EXIT_SIGNAL_STATUS = '')
                                AND (SELL_ORDER_STATUS IS NULL OR SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE')))"""

        mySQLCursor.execute(selectStatment)
        rowCount = mySQLCursor.rowcount
        currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
        updateArrayValues = []
        updateQuery = "UPDATE TRADE_TRANSACTIONS SET UPDATED_ON = %s, CURRENT_MKT_PRICE=%s, PROFIT_PERCENT=%s, PROFIT_AMOUNT=%s, CURR_SL_PCT=%s WHERE AUTO_ID= %s"

        if (rowCount > 0):
            results = mySQLCursor.fetchall()
            tmpCnt = 0
            for row in results: 

                tradeDataDict['autoId'] = row[0]
                tradeDataDict['accountId'] = row[1]            
                tradeDataDict['tradeSymbol']  = row[2]
                tradeDataDict['futTradeSymbol'] = row[3]
                tradeDataDict['futInstToken'] = str(row[4])
                tradeDataDict['instrumentToken']  = row[5]
                tradeDataDict['quantity'] = row[6]
                tradeDataDict['tradeDirection'] = row[7]            
                tradeDataDict['instrumentType'] = row[8]
                tradeDataDict['entryPrice'] = row[9]                
                oldSlPercent = row[10]
                tgtStopLossPct = row[11]
                tgtProfitPct = row[12]
                trailingPct = row[13]
                tradeStatus = row[14]                                             
                tradeDataDict['exchangeToken'] = row[15]                 
                entryPrice = tradeDataDict['entryPrice'] 
                
                if (tradeDataDict['tradeDirection'] == 'SELL'):
                    tradeDataDict['futTransactionType'] = 'BUY'
                else:
                    tradeDataDict['futTransactionType'] = 'SELL'

                newSLPercent = 0        
        
                ltpPrice =  prostocksApi.get_last_traded_price(brokerApi, tradeDataDict['exchangeToken'], exchange='NFO')
                
                # get profit for given qty
                profitPercent, profitAmount = util.get_profit(float(entryPrice), float(ltpPrice), tradeDataDict['quantity'])

                if (oldSlPercent is None or oldSlPercent == 0):
                    oldSlPercent = tgtStopLossPct
                
                exitSignalFlag, slUpdateFlag, newSLPercent = util.get_trailing_tgt_trigger(float(profitPercent), float(tgtProfitPct), float(oldSlPercent), float(trailingPct))
                # exitSignalFlag, slUpdateFlag, newSLPercent = util.get_mixed_trailing_sl(profitPercent, oldSlPercent, tgtStopLossPct, tgtProfitPct, trailingPct)

                if (not(slUpdateFlag) or exitSignalFlag):
                    newSLPercent = oldSlPercent

                if(exitSignalFlag and float(profitPercent) != 0 and tradeStatus != 'P-OPEN'):                                    
                    alertMsg = f"Profit/Loss conditions met for {tradeDataDict['futTradeSymbol']} in strategy {tradeDataDict['strategyId']} in account {tradeDataDict['accountId']}"
                    util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, tradeDataDict)

                    place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)        

                updateVal = []
                updateVal.insert(0, str(currDateTime))                
                updateVal.insert(1, str(ltpPrice))
                updateVal.insert(2, str(profitPercent))
                updateVal.insert(3, str(profitAmount))                       
                updateVal.insert(4, str(newSLPercent))                                                         
                updateVal.insert(5, str(tradeDataDict['autoId']))
                updateArrayValues.insert(tmpCnt, updateVal)               
                
                tmpCnt += 1  
        
            if (tmpCnt > 0):       
                mySQLCursor.executemany(updateQuery, updateArrayValues)
                cnx.commit()    
    except Exception as e:    
        alertMsg = f"Exceptions occured check_pl_based_exit_conditions: {str(e)}"
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, tradeDataDict)

def technical_based_exit(cnx, mySQLCursor, brokerApi, tradeDataDict):
    try:
        if ((tradeDataDict['exitBuyCondition'] != None and tradeDataDict['exitBuyCondition'] != '') or (tradeDataDict['exitSellCondition'] != None and tradeDataDict['exitSellCondition'] != '')):

            selectStatment = f"""SELECT AUTO_ID,
				TRADE_SYMBOL,
				INSTRUMENT_TOKEN,				
				QUANTITY,
				TRADE_DIRECTION,
				INSTRUMENT_TYPE,
				BUY_ORDER_PRICE,
				POSITION_DIRECTION,
                BASE_EXCHANGE_TOKEN,
                EXCHANGE_TOKEN
			FROM TRADE_TRANSACTIONS TT WHERE STRATEGY_ID='{tradeDataDict['strategyId']}' AND TRADE_ACCOUNT='{tradeDataDict['accountId']}' 
                AND (TRADE_STATUS IN ('OPEN') AND (EXIT_SIGNAL_STATUS IS NULL OR EXIT_SIGNAL_STATUS = '') 
                AND (SELL_ORDER_STATUS IS NULL OR SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE')))"""
            
            
            mySQLCursor.execute(selectStatment)
            rowCount = mySQLCursor.rowcount

            if (rowCount > 0):
                results = mySQLCursor.fetchall()
                
                for row in results:
                    
                    tradeDataDict['autoId'] = row[0]            
                    tradeDataDict['tradeSymbol']  = row[1]                    
                    tradeDataDict['futInstToken'] = row[2]                    
                    tradeDataDict['quantity'] = row[3]
                    tradeDataDict['tradeDirection'] = row[4]            
                    tradeDataDict['instrumentType'] = row[5]
                    tradeDataDict['entryPrice'] = row[6]
                    tradeDataDict['posDirection'] = row[7]
                    tradeDataDict['baseExchangeToken'] = row[8]
                    tradeDataDict['exchangeToken'] = row[8]
                    

                    if (str(tradeDataDict['tradeDirection']).upper() == 'BUY'):        
                        tradeDataDict['futTransactionType'] = "SELL"
                    elif (str(tradeDataDict['tradeDirection']).upper() == 'SELL'):        
                        tradeDataDict['futTransactionType'] = "BUY"  

                    # Check the reveral patterns only when it is needed
                    if (tradeDataDict['revPatternCheckFlag'] == 'Y'):
                        tradeDataDict = util.get_all_pattern_signals(cnx, mySQLCursor, brokerApi, tradeDataDict, interval=tradeDataDict['exitInterval'])
                  
                    exitLongFlag, exitShortFlag = is_technical_exit_met(cnx, mySQLCursor, brokerApi, tradeDataDict)

                    if (exitLongFlag):
                        alertMsg = f"Exit technical conditions (BUY) met for {tradeDataDict['futTradeSymbol']} in strategy {tradeDataDict['strategyId']} in account {tradeDataDict['accountId']}"
                        util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, tradeDataDict)
                        tradeDataDict['futTransactionType'] = "SELL"
                        place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)

                    elif (exitShortFlag):
                        alertMsg = f"Exit technical conditions (SELL) met for {tradeDataDict['futTradeSymbol']} in strategy {tradeDataDict['strategyId']} in account {tradeDataDict['accountId']}"
                        util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, tradeDataDict)
                        tradeDataDict['futTransactionType'] = "BUY"
                        place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)
                
    
    except Exception as e:    
        alertMsg = f"Exceptions occured technical_indicator_based_exit: {str(e)}"
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, tradeDataDict)       

def ti_options_sell_with_hedge_exit(cnx, mySQLCursor, brokerApi, tradeDataDict):    
    
    selectStatment = f"""SELECT C.AUTO_ID
                        , C.INSTRUMENT_TOKEN
                        , C.BUY_ORDER_PRICE
                        , C.QUANTITY
                        , P.AUTO_ID
                        , P.INSTRUMENT_TOKEN
                        , P.BUY_ORDER_PRICE
                        , P.QUANTITY
                        , A.BASE_INSTRUMENT_TOKEN
                        , A.TGT_PROFIT_PCT
                        , C.TRADE_DIRECTION
                        , P.TRADE_DIRECTION
                        , C.TRADE_SYMBOL
                        , P.TRADE_SYMBOL
                        , A.POSITION_DIRECTION
                        , A.BASE_EXCHANGE_TOKEN
                        FROM PROD.TRADE_TRANSACTIONS AS A LEFT JOIN TRADE_TRANSACTIONS AS C ON C.POSITIONS_GROUP_ID = A.POSITIONS_GROUP_ID AND C.TRADE_DIRECTION='BUY' \
                        LEFT JOIN TRADE_TRANSACTIONS AS P ON P.POSITIONS_GROUP_ID = A.POSITIONS_GROUP_ID  AND P.TRADE_DIRECTION='SELL' \
                        WHERE  (A.EXIT_STRATEGY_ID= '{tradeDataDict['exitStrategy']}' AND (A.TRADE_STATUS IN ('OPEN','P-OPEN') AND (A.EXIT_SIGNAL_STATUS IS NULL OR A.EXIT_SIGNAL_STATUS = '')) OR (A.TRADE_STATUS = 'OPEN' AND A.EXIT_SIGNAL_STATUS NOT IN ('EXIT SIGNAL') AND A.SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE')) OR (A.TRADE_STATUS = 'OPEN' AND A.EXIT_SIGNAL_STATUS IN ('EXIT SIGNAL') AND A.SELL_ORDER_STATUS IN ('ABANDONED'))) \
                        GROUP BY A.POSITIONS_GROUP_ID"""

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()

    for row in results:
        callAutoId = row[0]
        callInstToken = str(row[1])
        callEntryPrice = row[2]    
        callQuantity = row[3]  
        putAutoId = row[4]
        putInstToken = str(row[5])
        putEntryPrice = row[6] 
        putQuantity = row[7]
        tradeDataDict['instrumentToken'] = row[8]        
        tgtProfitPct = row[9]
        callTradeDirection = row[10]
        putTradeDirection = row[11]
        callTradeSymbol = row[12]
        putTradeSymbol = row[13]
        tradeDataDict['posDirection'] = row[14]
        tradeDataDict['baseExchangeToken'] = row[15]
        
        if (callTradeDirection == 'SELL'):
            ltpPrice =  prostocksApi.get_last_traded_price(brokerApi, tradeDataDict['exchangeToken'], exchange=tradeDataDict['exchange'])            
            # get profit for given qty
            profitPercent, profitAmount = util.get_profit(float(callEntryPrice), float(ltpPrice), callQuantity)
        elif (putTradeDirection == 'SELL'):
            ltpPrice =  prostocksApi.get_last_traded_price(brokerApi, tradeDataDict['exchangeToken'], exchange=tradeDataDict['exchange'])
            # get profit for given qty
            profitPercent, profitAmount = util.get_profit(float(putEntryPrice), float(ltpPrice), putQuantity)
        
        exitOptionPositionFlag = False
        
        # Check whether the profit for SELL positions reached it's target
        if(float(profitPercent) >= float(tgtProfitPct)):
            exitOptionPositionFlag = True
            tradeDataDict['quantity'] = callQuantity   
            tradeDataDict['futInstToken'] = callInstToken 
            tradeDataDict['autoId'] = callAutoId                                   
            tradeDataDict['futTradeSymbol'] = callTradeSymbol
            tradeDataDict['entryPrice'] = callEntryPrice

            if (callTradeDirection == 'SELL'):
                tradeDataDict['futTransactionType'] = 'BUY'
            else:
                tradeDataDict['futTransactionType'] = 'SELL'
            
            
            place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)

            tradeDataDict['quantity'] = putQuantity   
            tradeDataDict['futInstToken'] = putInstToken 
            tradeDataDict['autoId'] = putAutoId                                   
            tradeDataDict['futTradeSymbol'] = putTradeSymbol
            tradeDataDict['entryPrice'] = putEntryPrice

            if (putTradeDirection == 'SELL'):
                tradeDataDict['futTransactionType'] = 'BUY'
            else:
                tradeDataDict['futTransactionType'] = 'SELL'            

            place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)
            
        # check ehter we receive any techical indicator for exit
        elif (not(exitOptionPositionFlag)):
                
            exitLongFlag, exitShortFlag = is_technical_exit_met(cnx, mySQLCursor, brokerApi, tradeDataDict)
        
            if (exitLongFlag):        
                
                tradeDataDict['quantity'] = callQuantity   
                tradeDataDict['futInstToken'] = callInstToken 
                tradeDataDict['autoId'] = callAutoId                                   
                tradeDataDict['futTradeSymbol'] = callTradeSymbol
                tradeDataDict['entryPrice'] = callEntryPrice
                if (callTradeDirection == 'SELL'):
                    tradeDataDict['futTransactionType'] = 'BUY'
                else:
                    tradeDataDict['futTransactionType'] = 'SELL'
                place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)

                # BUY this next
                tradeDataDict['quantity'] = putQuantity   
                tradeDataDict['futInstToken'] = putInstToken 
                tradeDataDict['autoId'] = putAutoId                                   
                tradeDataDict['futTradeSymbol'] = putTradeSymbol
                tradeDataDict['entryPrice'] = putEntryPrice
                
                if (putTradeDirection == 'SELL'):
                    tradeDataDict['futTransactionType'] = 'BUY'
                else:
                    tradeDataDict['futTransactionType'] = 'SELL'

                place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)

            elif (exitShortFlag):
                
                tradeDataDict['quantity'] = callQuantity   
                tradeDataDict['futInstToken'] = callInstToken 
                tradeDataDict['autoId'] = callAutoId                                   
                tradeDataDict['futTradeSymbol'] = callTradeSymbol
                tradeDataDict['entryPrice'] = callEntryPrice
                
                if (callTradeDirection == 'SELL'):
                    tradeDataDict['futTransactionType'] = 'BUY'
                else:
                    tradeDataDict['futTransactionType'] = 'SELL'
                place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)

                # BUY this next
                tradeDataDict['quantity'] = putQuantity   
                tradeDataDict['futInstToken'] = putInstToken 
                tradeDataDict['autoId'] = putAutoId                                   
                tradeDataDict['futTradeSymbol'] = putTradeSymbol
                tradeDataDict['entryPrice'] = putEntryPrice
                
                if (putTradeDirection == 'SELL'):
                    tradeDataDict['futTransactionType'] = 'BUY'
                else:
                    tradeDataDict['futTransactionType'] = 'SELL'
                place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)



def exit_pl_based_trade_service(cnx, mySQLCursor, brokerApi): 
    try:

        oldAccountId = None
        brokerApi = None
        isApiConnected = False
        # Get the list of all stocks. It will check whether existing order is available; proceed to order only when no existing order present in mysql database
        for row in get_exit_positions_list(mySQLCursor):  
            tradeDataDict = {}      
            autoId = row[0]
            accountId = row[1]
            broker = row[2]
            tradeSymbol = row[3]
            stockName = row[4]
            quantity = row[5]
            exchange = row[7]
            instrumentToken = row[8]
    
            if (oldAccountId is None or oldAccountId != accountId):
                brokerApi, isApiConnected = prostocksApi.connect_broker_api(cnx, mySQLCursor, accountId, broker)
                oldAccountId = accountId

            if (isApiConnected):            
                if (exchange == 'NFO'):
                    tradeDataDict['broker'] = 'PROSTOCKS'
                    tradeDataDict['quantity'] = quantity
                    tradeDataDict['accountId'] = accountId
                    tradeDataDict['futTradeSymbol'] = tradeSymbol           
                    tradeDataDict['futOrderType'] = 'LMT'                
                    if (int(quantity) > 0):                        
                        tradeDataDict['futTransactionType'] = "SELL"      
                    else: 
                        tradeDataDict['futTransactionType'] = "BUY"      

                    existingOrderFound, proStocksOrderId, proStocksTransType, proStocksQty = baf.get_prostocks_orders(brokerApi, tradeDataDict['futTradeSymbol'], inTransType=tradeDataDict['futTransactionType'])
                    existingPositionFound, existingOrderQuantity  = baf.get_prostocks_positions(brokerApi, tradeDataDict['futTradeSymbol']) 
                    
                    if (existingPositionFound and not(existingOrderFound)):
                        tradeDataDict['futTriggerPrice'] = util.get_best_trigger_price(brokerApi, instrumentToken, exchange, tradeDataDict['futTransactionType'])                

                        if (tradeDataDict['futTriggerPrice'] != None):
                    
                            tradeDataDict['futLastPrice'] = tradeDataDict['futTriggerPrice']                        
                            orderId = 0
                            orderRemarks = ''
                            
                            
                            orderId, orderRemarks  = baf.place_future_buy_order(brokerApi, tradeDataDict)  
                            
                            currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')

                            if (isinstance(orderId, list) and orderId[0] > 0):
                                orderId = orderId[0]

                            if (int(orderId) > 0):                    
                                try:
                                    logging.info("Sell order is placed for " + tradeSymbol +" with order Id: "+ str(orderId))    
                                    updateQuery = "UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_DATE='"+str(currDateTime)+"', UPDATED_ON='"+str(currDateTime)+"', SELL_ORDER_STATUS='PENDING', \
                                                    SELL_ORDER_ID='"+str(orderId)+"' WHERE AUTO_ID="+str(autoId)
                                
                                    mySQLCursor.execute(updateQuery) 
                                    cnx.commit() 
                                except Exception as e:
                                    logging.info('Unable to update the exit status in database (in > 0): ' + str(e))
                            elif (int(orderId) == -1):                    
                                try:
                                    updateQuery = "UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_REMARKS='"+str(orderRemarks)+"', SELL_ORDER_DATE='"+str(currDateTime)+"', UPDATED_ON='"+str(currDateTime)+"', SELL_ORDER_STATUS='ABANDONED' WHERE AUTO_ID="+str(autoId)
                                    mySQLCursor.execute(updateQuery) 
                                    cnx.commit()

                                    alertMsg = "Errored during sell of "+ stockName.replace('&', ' and ') +" in trade account "+ str(accountId) +" with error: "+str(orderRemarks)
                                    # util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)    

                                except Exception as e:
                                    logging.info('Unable to update the exit status in database (in -1): ' + str(e))
                            
                            elif (int(orderId) == -2):                    
                                try:
                                    updateQuery = "UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_REMARKS='"+str(orderRemarks)+"', SELL_ORDER_DATE='"+str(currDateTime)+"', UPDATED_ON='"+str(currDateTime)+"', SELL_ORDER_STATUS='TIMED OUT' WHERE AUTO_ID="+str(autoId)
                                    mySQLCursor.execute(updateQuery) 
                                    cnx.commit() 

                                    alertMsg = "Timed out during sell of "+ stockName.replace('&', ' and ') +" in trade account "+ str(accountId) +" with error: "+str(orderRemarks)
                                    # util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)    

                                except Exception as e:
                                    logging.info('Unable to update the exit status in database (in -2): ' + str(e))
                            
                            elif (int(orderId) == -3):                   
                                alertMsg = "Session timed out during sell of "+ stockName.replace('&', ' and ') +" in trade account "+ str(accountId) +" with error: "+ str(orderRemarks) + "\nTry Again."
                                # util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)    

                        else:
                            logging.info(orderRemarks)
                    else:
                        logging.info(f"Existing position is not found or existing order found for {tradeDataDict['futTradeSymbol']}")
    
    except Exception as e:
        alertMsg = f"Exception occured in exit_pl_based_trade_service: {str(e)}"
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, sysDict)

def place_options_adjustment_order(mySQLCursor, brokerApi, adjustmentInstType, tradeDataDict):
    try:
        userList = util.get_strategy_setttings(mySQLCursor, tradeDataDict['strategyId'], tradeAccount = tradeDataDict['accountId'])

        for user in userList:                        
            tradeDataDict['tgtProfitPct'] = user['TGT_PROFIT_PCT']    
            tradeDataDict['tgtStopLossPct'] = user['TGT_STOP_LOSS_PCT']    
            tradeDataDict['trailingThresholdPct'] = user['TRAILING_THRESHOLD_PCT']  
            tradeDataDict['exitStrategyId'] = user['EXIT_STRATEGY_ID']                                   
            tradeDataDict['availableMargin'] = user['AVAILABLE_MARGIN']
            tradeDataDict['capitalAllocation'] = user['CAPITAL_ALLOCATION']
            tradeDataDict['entryBuyCondition'] = user['ENTRY_BUY_CONDITION']
            tradeDataDict['entrySellCondition'] = user['ENTRY_SELL_CONDITION']            
            tradeDataDict['strategyType'] = user['STRATEGY_TYPE']
            tradeDataDict['entryDirection'] = user['ENTRY_DIRECTION']
            
            tradeDataDict['contractType'] = user['CONTRACT_TYPE']
            tradeDataDict['productType'] = user['PRODUCT_TYPE']
            tradeDataDict['lotSizeMultiplier'] = int(user['LOT_SIZE_MULTIPLIER'])
            tradeDataDict['rollOverFlag'] = user['ROLL_OVER_FLAG']
            tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")    
            tradeDataDict['futTransactionType'] = 'SELL'
            tradeDataDict['strikeSelectByPercent'] = user['STRIKE_SELECT_BY_PERCENT']            
            tradeDataDict['strikeType'] = user['STRIKE_TYPE']                                
                                
            tradeDataDict['adjustmentOrder'] = "Y"
            tradeDataDict['quantity'] = abs(tradeDataDict['quantity'])
            
            expDate = tradeDataDict['expiryDate'].strftime("%d%b%y").upper()


            if (adjustmentInstType == 'PUT'):
                # Get the delta of CALL (Un-Tested side) options
                bsmDataDict = bsm_options_pricing_new(brokerApi, tradeDataDict['callInstToken'], tradeDataDict['callStrikePrice'], tradeDataDict['expiryDate'], tradeDataDict['baseLtpPrice'], tradeDataDict['callTradeSymbol'], 'CALL')
                logging.info(f"bsmDataDict : {bsmDataDict}")

                tradeDataDict['instrumentType'] = 'PUT'
                
                if (bsmDataDict['status'] == 'success'):
                    # Get the closest strike price that matches delta of untested options
                    bsmSelectedDict = get_strike_by_delta(mySQLCursor, brokerApi, tradeDataDict['CLOSE-PRICE'], tradeDataDict['expiryDate'], tradeDataDict['baseTradeSymbol'], 'PUT', bsmDataDict['delta'])
                    
                    tradeDataDict['futTradeSymbol'] = str(tradeDataDict['tradeSymbol']).replace('&', '%26') + expDate + 'P' + str(bsmSelectedDict['selectedStrikePrice']).replace('.0','')
                    tradeDataDict['futInstToken'] = bsmSelectedDict['futInstToken']                    
                    tradeDataDict['strikePrice'] = bsmSelectedDict['selectedStrikePrice']

                else:
                    tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=0, adjustOrderFlag=True)                
                    
                
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)            
                
                
                alertMsg = f"Placing adjustment PUT ({tradeDataDict['futTradeSymbol']}) order for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']}"
                util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)
            
                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
            else:                
                # Get the delta of PUT (Un-Tested side) options
                bsmDataDict = bsm_options_pricing_new(brokerApi, tradeDataDict['putInstToken'], tradeDataDict['putStrikePrice'], tradeDataDict['expiryDate'], tradeDataDict['baseLtpPrice'], tradeDataDict['putTradeSymbol'], 'PUT')
                logging.info(f"bsmDataDict : {bsmDataDict}")

                tradeDataDict['instrumentType'] = 'CALL'
                
                # If the delta is found of un-tested side, get the closest delta of adjustment trade, else continue with traditional options selection process
                if (bsmDataDict['status'] == 'success'):
                    # Get the closest strike price that matches delta of untested options
                    bsmSelectedDict = get_strike_by_delta(mySQLCursor, brokerApi, tradeDataDict['CLOSE-PRICE'], tradeDataDict['expiryDate'], tradeDataDict['baseTradeSymbol'], 'CALL', bsmDataDict['delta'])
                    
                    tradeDataDict['futTradeSymbol'] = str(tradeDataDict['tradeSymbol']).replace('&', '%26') + expDate + 'C' + str(bsmSelectedDict['selectedStrikePrice']).replace('.0','')
                    tradeDataDict['futInstToken'] = bsmSelectedDict['futInstToken']                    
                    tradeDataDict['strikePrice'] = bsmSelectedDict['selectedStrikePrice']

                else:
                    tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=0, adjustOrderFlag=True)      
                
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)

                alertMsg = f"Placing adjustment CALL ({tradeDataDict['futTradeSymbol']}) order for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']}"
                util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)

                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
    
    except Exception as e:
        alertMsg = f"Exceptions occured place_options_adjustment_order: {str(e)}"
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, tradeDataDict)

def short_strangle_exit(cnx, mySQLCursor, brokerApi, tradeDataDict):
    try:
        selectStatment = f"""SELECT 
                    C.AUTO_ID,
                    C.INSTRUMENT_TOKEN,
                    C.BUY_ORDER_PRICE,
                    C.QUANTITY,
                    C.OPTION_STRIKE_PRICE,
                    C.TRADE_SYMBOL,
                    P.AUTO_ID,
                    P.INSTRUMENT_TOKEN,
                    P.BUY_ORDER_PRICE,
                    P.QUANTITY,
                    P.OPTION_STRIKE_PRICE,
                    P.TRADE_SYMBOL,
                    A.BASE_INSTRUMENT_TOKEN,
                    A.POSITIONS_GROUP_ID,
                    A.TGT_PROFIT_PCT,
                    A.BASE_STOCK_ENTRY_PRICE,
                    A.BASE_TRADE_SYMBOL,
                    A.TRADE_SEQUENCE,
                    A.EXPIRY_DATE,
                    A.TGT_STOP_LOSS_PCT        
                FROM TRADE_TRANSACTIONS AS A 
                LEFT JOIN TRADE_TRANSACTIONS AS C ON C.POSITIONS_GROUP_ID = A.POSITIONS_GROUP_ID AND C.TRADE_DIRECTION='SELL' AND C.INSTRUMENT_TYPE='CALL' AND C.TRADE_STATUS = 'OPEN' AND ((C.EXIT_SIGNAL_STATUS IS NULL OR C.EXIT_SIGNAL_STATUS NOT IN ('EXIT SIGNAL')) AND (C.SELL_ORDER_STATUS IS NULL OR C.SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE')))
                LEFT JOIN TRADE_TRANSACTIONS AS P ON P.POSITIONS_GROUP_ID = A.POSITIONS_GROUP_ID AND P.TRADE_DIRECTION='SELL' AND P.INSTRUMENT_TYPE='PUT' AND P.TRADE_STATUS = 'OPEN' AND ((P.EXIT_SIGNAL_STATUS IS NULL OR P.EXIT_SIGNAL_STATUS NOT IN ('EXIT SIGNAL')) AND (P.SELL_ORDER_STATUS IS NULL OR P.SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE')))
                WHERE A.TRADE_ACCOUNT= '{tradeDataDict['accountId']}' AND A.EXIT_STRATEGY_ID= '{tradeDataDict['exitStrategy']}' AND A.STRATEGY_ID='{tradeDataDict['strategyId']}' AND A.TRADE_STATUS ='OPEN'
                GROUP BY A.POSITIONS_GROUP_ID"""
                
    
        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()                

        for row in results:
            callAutoId = row[0]
            callInstToken = str(row[1])
            callEntryPrice = row[2]    
            callQuantity = row[3]
            callStrikePrice = row[4]
            callTradeSymbol = row[5]

            putAutoId = row[6]
            putInstToken = str(row[7])
            putEntryPrice = row[8]            
            putQuantity = row[9]        
            putStrikePrice = row[10]            
            putTradeSymbol = row[11]
            
            baseInstToken = str(row[12])
            positionGroupId = row[13]
            tgtProfitPct = row[14]
            baseStockEntryPrice = row[15]
            baseTradeSymbol = row[16]
            tradeSequence = row[17]            
            tradeDataDict['expiryDate'] = row[18]
            tradeDataDict['rawExpiry'] = row[18]
            tgtStopLossPct = row[19]
            
            tradeDataDict['positionGroupId'] = positionGroupId            
            tradeDataDict['baseStockEntryPrice'] = baseStockEntryPrice      
            tradeDataDict['instrumentToken'] = baseInstToken            
            tradeDataDict['tradeSymbol'] = baseTradeSymbol
            tradeDataDict['baseTradeSymbol'] = baseTradeSymbol
            
            tradeDataDict['callInstToken'] = callInstToken
            tradeDataDict['callStrikePrice'] = callStrikePrice
            tradeDataDict['callTradeSymbol'] = callTradeSymbol
            tradeDataDict['putInstToken'] = putInstToken
            tradeDataDict['putStrikePrice'] = putStrikePrice
            tradeDataDict['putTradeSymbol'] = putTradeSymbol

            try:
                tradeDataDict['tradeSequence']  = 'DELTA_ADJUST_' + str(int(re.findall(r'\d+', tradeSequence)[0]) + 1)
            except:
                tradeDataDict['tradeSequence'] = tradeSequence
        
            putOptionRowFlag = False
            callOptionRowFlag = False

            if (callInstToken != 'None' and callInstToken != ''):
                callOptionRowFlag = True             

            if (putInstToken != 'None' and putInstToken != ''):
                putOptionRowFlag = True
            
            # Calculate cumlative target profit or target loss, and exit when it reaches either profit or loss

            if(callOptionRowFlag and putOptionRowFlag):
                baseLtpPrice =  prostocksApi.get_last_traded_price(brokerApi, baseInstToken, exchange='NFO')

                tradeDataDict['CLOSE-PRICE'] = baseLtpPrice
                tradeDataDict['baseLtpPrice'] = baseLtpPrice
                

                callDelta = None
                putDelta = None
                # Get the delta of CALL options
                bsmDataDict = bsm_options_pricing_new(brokerApi, tradeDataDict['callInstToken'], tradeDataDict['callStrikePrice'], tradeDataDict['expiryDate'], tradeDataDict['baseLtpPrice'], tradeDataDict['callTradeSymbol'], 'CALL')
                if (bsmDataDict['status'] == 'success'):
                    callDelta = abs(float(bsmDataDict['delta']))
                
                # Get the delta of PUT options
                bsmDataDict = bsm_options_pricing_new(brokerApi, tradeDataDict['putInstToken'], tradeDataDict['putStrikePrice'], tradeDataDict['expiryDate'], tradeDataDict['baseLtpPrice'], tradeDataDict['putTradeSymbol'], 'PUT')                    
                if (bsmDataDict['status'] == 'success'):
                    putDelta = abs(float(bsmDataDict['delta']))

                profitAmount, profitPercent = get_cumulative_pnl(cnx, mySQLCursor, positionGroupId)

                if (float(profitPercent) >= float(tgtProfitPct) or float(profitPercent) <= float(tgtStopLossPct) or abs(putDelta) > 0.60 or abs(callDelta) > 0.60):
                    alertMsg = f"Target or Stoploss reached for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']} with profit of {profitPercent} %. Exiting both {callTradeSymbol} and {putTradeSymbol}"
                    util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)
                    if (callOptionRowFlag):
                        tradeDataDict['quantity'] = callQuantity   
                        tradeDataDict['futInstToken'] = callInstToken 
                        tradeDataDict['autoId'] = callAutoId                                   
                        tradeDataDict['futTradeSymbol'] = callTradeSymbol            
                        tradeDataDict['futTransactionType'] = 'BUY'                    
                        tradeDataDict['entryPrice'] = callEntryPrice

                        place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)

                    if (putOptionRowFlag):
                        tradeDataDict['quantity'] = putQuantity   
                        tradeDataDict['futInstToken'] = putInstToken 
                        tradeDataDict['autoId'] = putAutoId                                   
                        tradeDataDict['futTradeSymbol'] = putTradeSymbol            
                        tradeDataDict['futTransactionType'] = 'BUY'
                        tradeDataDict['entryPrice'] = putEntryPrice

                        place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)

                else:
                    # CALL STRIKE TESTED, EXIT THE PUT AND PLACE THE ADJUSTMENT TRADE ON PUT       
                    if (callDelta != None and putDelta != None and (callDelta > putDelta) and (callDelta - putDelta) >= 0.10):
                        tradeDataDict['instrumentType'] = 'PUT'
                        alertMsg = f"CALL strike tested for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']}. Exiting {putTradeSymbol}, and Base Instrument LTP is now at {baseLtpPrice}"
                        util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)
                        
                        tradeDataDict['quantity'] = putQuantity   
                        tradeDataDict['futInstToken'] = putInstToken 
                        tradeDataDict['autoId'] = putAutoId
                        tradeDataDict['futTradeSymbol'] = putTradeSymbol
                        tradeDataDict['futTransactionType'] = 'BUY'
                        tradeDataDict['entryPrice'] = putEntryPrice
                        place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)
                        
                        # Mark that we have adjustment order; this will make sure that it will only insert the record in database, and no data will be sent to exchange

                        place_options_adjustment_order(mySQLCursor, brokerApi, 'PUT', tradeDataDict)

                    # PUT STRIKE TESTED, EXIT THE CALL AND PLACE THE ADJUSTMENT TRADE ON CALL
                    elif (callDelta != None and putDelta != None and (putDelta > callDelta) and (putDelta - callDelta) >= 0.10):
                        alertMsg = f"PUT strike tested for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']}. Exiting {callTradeSymbol}, and LTP is now at {baseLtpPrice}"
                        util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)    

                        tradeDataDict['quantity'] = callQuantity
                        tradeDataDict['futInstToken'] = callInstToken
                        tradeDataDict['autoId'] = callAutoId
                        tradeDataDict['futTradeSymbol'] = callTradeSymbol
                        tradeDataDict['futTransactionType'] = 'BUY'
                        tradeDataDict['entryPrice'] = callEntryPrice
                        place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)
                        
                        # Mark that we have adjustment order; this will make sure that it will only insert the record in database, and no data will be sent to exchange
                        place_options_adjustment_order(mySQLCursor, brokerApi, 'CALL', tradeDataDict)

                    # # CALL STRIKE TESTED, EXIT THE PUT AND PLACE THE ADJUSTMENT TRADE ON PUT                
                    # if (baseLtpPrice >= callStrikePrice):                    
                    #     tradeDataDict['instrumentType'] = 'PUT'
                    #     tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=0)
                        
                    #     if (tradeDataDict['strikePrice']  >= (putStrikePrice + tradeDataDict['strikeAdjust'])):

                    #         alertMsg = f"CALL strike tested for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']}. Exiting {putTradeSymbol}, and Base Instrument LTP is now at {baseLtpPrice}"
                    #         util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)
                            
                    #         tradeDataDict['quantity'] = putQuantity   
                    #         tradeDataDict['futInstToken'] = putInstToken 
                    #         tradeDataDict['autoId'] = putAutoId
                    #         tradeDataDict['futTradeSymbol'] = putTradeSymbol
                    #         tradeDataDict['futTransactionType'] = 'BUY'
                    #         tradeDataDict['entryPrice'] = putEntryPrice
                    #         place_fno_exit_order(cnx, mySQLCursor, kite, tradeDataDict, brokerAPI)
                            
                    #         # Mark that we have adjustment order; this will make sure that it will only insert the record in database, and no data will be sent to exchange

                    #         place_options_adjustment_order(mySQLCursor, kite, brokerAPI, 'PUT', tradeDataDict)
                    
                    # # PUT STRIKE TESTED, EXIT THE CALL AND PLACE THE ADJUSTMENT TRADE ON CALL
                    # elif (baseLtpPrice <= putStrikePrice):                    
                    #     tradeDataDict['instrumentType'] = 'CALL'

                    #     tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=0)
                    #     if (tradeDataDict['strikePrice']  <= (callStrikePrice - tradeDataDict['strikeAdjust'])):
                    #         alertMsg = f"PUT strike tested for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']}. Exiting {callTradeSymbol}, and LTP is now at {baseLtpPrice}"
                    #         util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)    

                    #         tradeDataDict['quantity'] = callQuantity
                    #         tradeDataDict['futInstToken'] = callInstToken
                    #         tradeDataDict['autoId'] = callAutoId
                    #         tradeDataDict['futTradeSymbol'] = callTradeSymbol
                    #         tradeDataDict['futTransactionType'] = 'BUY'
                    #         tradeDataDict['entryPrice'] = callEntryPrice
                    #         place_fno_exit_order(cnx, mySQLCursor, kite, tradeDataDict, brokerAPI)
                            
                    #         # Mark that we have adjustment order; this will make sure that it will only insert the record in database, and no data will be sent to exchange
                    #         place_options_adjustment_order(mySQLCursor, kite, brokerAPI, 'CALL', tradeDataDict)
            
            
    except Exception as e:    
        alertMsg = f"Exceptions occured check_short_strangle_exit_conditions: {str(e)}"
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, tradeDataDict)

def get_cumulative_pnl(cnx, mySQLCursor, positionGroupId):
    selectStatment = f"SELECT SUM(ABS(BUY_VALUE)), SUM(PROFIT_AMOUNT), SUM(PROFIT_AMOUNT) / SUM(ABS(BUY_VALUE)) * 100 \
                FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS IN ('OPEN', 'EXITED') AND POSITIONS_GROUP_ID = '{positionGroupId}'"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    profitAmount = 0
    profitPercent = 0
    for row in results:            
        profitAmount = row[1]
        profitPercent = row[2]        
        try:
            updateQuery = f"UPDATE TRADE_TRANSACTIONS SET POSITION_TOTAL_PROFIT='{profitAmount}' WHERE POSITIONS_GROUP_ID = '{positionGroupId}'"
            mySQLCursor.execute(updateQuery) 
            cnx.commit()                 
        except:
            pass

    return profitAmount, profitPercent

def is_technical_exit_met(cnx, mySQLCursor, brokerApi, tradeDataDict):
    exitBuyCondition = tradeDataDict['exitBuyCondition']
    exitSellCondition = tradeDataDict['exitSellCondition']
    interval = tradeDataDict['exitInterval']
    posDirection = None

    if ('posDirection'  in tradeDataDict):
        posDirection = tradeDataDict['posDirection']
    # elif ('instrumentType'  in tradeDataDict): 
    #     if (tradeDataDict['instrumentType'] == 'CALL'):                
    #         posDirection = "LONG"
    #     elif (tradeDataDict['instrumentType'] == 'PUT'):                
    #         posDirection = "SHORT"     
    # elif ('tradeDirection'  in tradeDataDict):    
    #     if (tradeDataDict['tradeDirection'] == 'BUY'):
    #         posDirection = "LONG"
    #     elif (tradeDataDict['tradeDirection'] == 'SELL'):
    #         posDirection = "SHORT"


    exitLongFlag =  False
    exitShortFlag =  False

    if ((exitBuyCondition != None and exitBuyCondition != '') or (exitSellCondition != None and exitSellCondition != '')):

        tradeDataDict, techIndResponse = util.techinical_indicator_in_dict(brokerApi, tradeDataDict, interval=interval)
        if (techIndResponse['status'] == 'success'):
            if (posDirection == 'LONG' and exitBuyCondition != None and exitBuyCondition != '' and eval(exitBuyCondition)):                    
                exitLongFlag = True
            elif (posDirection == 'SHORT' and exitSellCondition != None and exitSellCondition != '' and eval(exitSellCondition)):                            
                exitShortFlag = True
        else:                  
            util.add_logs(cnx, mySQLCursor, 'ERROR', techIndResponse['remarks'], tradeDataDict)

    return exitLongFlag, exitShortFlag

def hedged_futures_exit(cnx, mySQLCursor, brokerApi, tradeDataDict):
    try:
        
        selectStatment = f"""SELECT 
                    O.AUTO_ID,
                    O.INSTRUMENT_TOKEN,
                    O.BUY_ORDER_PRICE,
                    O.QUANTITY,
                    O.TRADE_SYMBOL,
                    O.TRADE_DIRECTION,
                    F.AUTO_ID,
                    F.INSTRUMENT_TOKEN,
                    F.BUY_ORDER_PRICE,
                    F.QUANTITY,
                    F.TRADE_SYMBOL,
                    F.TRADE_DIRECTION,
                    A.BASE_INSTRUMENT_TOKEN,
                    A.POSITIONS_GROUP_ID,
                    A.TGT_PROFIT_PCT,
                    A.BASE_STOCK_ENTRY_PRICE,
                    A.BASE_TRADE_SYMBOL,
                    A.TGT_STOP_LOSS_PCT,
                    A.POSITION_DIRECTION,
                    A.BASE_EXCHANGE_TOKEN, 
                    O.EXCHANGE_TOKEN,
                    F.EXCHANGE_TOKEN


                FROM TRADE_TRANSACTIONS AS A \
                LEFT JOIN TRADE_TRANSACTIONS AS O ON O.POSITIONS_GROUP_ID = A.POSITIONS_GROUP_ID AND (O.INSTRUMENT_TYPE='CALL' OR O.INSTRUMENT_TYPE='PUT')  AND O.TRADE_STATUS = 'OPEN' AND ((O.EXIT_SIGNAL_STATUS IS NULL OR O.EXIT_SIGNAL_STATUS NOT IN ('EXIT SIGNAL')) AND (O.SELL_ORDER_STATUS IS NULL OR O.SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE'))) \
                LEFT JOIN TRADE_TRANSACTIONS AS F ON F.POSITIONS_GROUP_ID = A.POSITIONS_GROUP_ID AND F.INSTRUMENT_TYPE='FUT' AND F.TRADE_STATUS = 'OPEN' AND ((F.EXIT_SIGNAL_STATUS IS NULL OR F.EXIT_SIGNAL_STATUS NOT IN ('EXIT SIGNAL')) AND (F.SELL_ORDER_STATUS IS NULL OR F.SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE'))) \
                WHERE A.TRADE_ACCOUNT= '{tradeDataDict['accountId']}' AND A.EXIT_STRATEGY_ID= '{tradeDataDict['exitStrategy']}' AND A.STRATEGY_ID='{tradeDataDict['strategyId']}' AND A.TRADE_STATUS ='OPEN' \
                GROUP BY A.POSITIONS_GROUP_ID"""
                
        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()                

        for row in results:
            optionAutoId = row[0]
            optionInstToken = str(row[1])
            optionEntryPrice = row[2]    
            optionQuantity = row[3]            
            optionTradeSymbol = row[4]  
            optionTradeDirection = row[5]
            
            futAutoId = row[6]
            futInstToken = str(row[7])
            futEntryPrice = row[8]            
            futQuantity = row[9]               
            futTradeSymbol = row[10]                           
            futTradeDirection = row[11]

            baseInstToken = str(row[12])
            positionGroupId = row[13]
            tgtProfitPct = row[14]
            baseStockEntryPrice = row[15]
            baseTradeSymbol = row[16]            
            tgtStopLossPct = row[17]
            tradeDataDict['posDirection'] = row[18]
            tradeDataDict['baseExchangeToken'] = row[19]
            optionExchangeToken = row[20]
            futExchangeToken = row[21]
       

            tradeDataDict['positionGroupId'] = positionGroupId            
            tradeDataDict['baseStockEntryPrice'] = baseStockEntryPrice      
            tradeDataDict['instrumentToken'] = baseInstToken
            tradeDataDict['tradeSymbol'] = baseTradeSymbol
            tradeDataDict['baseTradeSymbol'] = baseTradeSymbol

         
            profitAmount, profitPercent = get_cumulative_pnl(cnx, mySQLCursor, positionGroupId)

            optionRowFlag = False
            futRowFlag = False
            if (optionInstToken != 'None' and optionInstToken != ''):
                optionRowFlag = True                  
            if (futInstToken != 'None' and futInstToken != ''):
                futRowFlag = True

            exitOptionPositionFlag = False
            alertMsg = ""
            if (optionRowFlag and futRowFlag):
                if (float(profitPercent) >= float(tgtProfitPct) or float(profitPercent) <= float(tgtStopLossPct)):   
                    alertMsg = f"Target or Stoploss reached for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']} with profit of {profitPercent} %. Exiting both {optionTradeSymbol} and {futTradeSymbol}"                
                    exitOptionPositionFlag = True
                else:
                    exitLongFlag, exitShortFlag = is_technical_exit_met(cnx, mySQLCursor, brokerApi, tradeDataDict)
                    
                    if (exitLongFlag):
                        alertMsg = f"Technical exit (BUY) conditions met for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']} with profit of {profitPercent} %. Exiting both {optionTradeSymbol} and {futTradeSymbol}"
                        exitOptionPositionFlag = True
                    elif (exitShortFlag):
                        alertMsg = f"Technical exit (SELL) conditions met for strategy {tradeDataDict['strategyId']} in {tradeDataDict['accountId']} with profit of {profitPercent} %. Exiting both {optionTradeSymbol} and {futTradeSymbol}"                            
                        exitOptionPositionFlag = True
                
                if (exitOptionPositionFlag):
                    util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)
                    # Exit the futures first
                    if (futRowFlag):
                        tradeDataDict['quantity'] = futQuantity   
                        tradeDataDict['futInstToken'] = futInstToken 
                        tradeDataDict['autoId'] = futAutoId                                   
                        tradeDataDict['futTradeSymbol'] = futTradeSymbol            
                        tradeDataDict['entryPrice'] = futEntryPrice                                                
                        tradeDataDict['futTransactionType'] = 'SELL'
                        tradeDataDict['exchangeToken'] = futExchangeToken

                        if (futTradeDirection == 'SELL'):
                            tradeDataDict['futTransactionType'] = 'BUY'
                        place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)
                    
                    # Exit the options last, but just entry will be added as exit. This will not exit until the future exit order gets completed
                    if (optionRowFlag):
                        tradeDataDict['quantity'] = optionQuantity   
                        tradeDataDict['futInstToken'] = optionInstToken 
                        tradeDataDict['autoId'] = optionAutoId                                   
                        tradeDataDict['futTradeSymbol'] = optionTradeSymbol            
                        tradeDataDict['futTransactionType'] = 'SELL'
                        tradeDataDict['exchangeToken'] = optionExchangeToken
                        if (optionTradeDirection == 'SELL'):
                            tradeDataDict['futTransactionType'] = 'BUY'
                        tradeDataDict['entryPrice'] = optionEntryPrice                        
                        
                        place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict, dummyExitFlag = 'Y')

                    

            
    except Exception as e:    
        alertMsg = f"Exceptions occured hedged_futures_exit: {str(e)}"
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, tradeDataDict)

def eod_exit_positions(cnx, mySQLCursor, strategyId, accountId, broker, brokerApi):
    response = {}
    try:
        # Check if the sell is having a corresponding adjustment order                                    
        selectStatment = f"SELECT TRADE_DIRECTION, ABS(QUANTITY), TRADE_SYMBOL, INSTRUMENT_TOKEN, AUTO_ID, BUY_ORDER_PRICE \
            FROM TRADE_TRANSACTIONS WHERE STRATEGY_ID = '{strategyId}' AND TRADE_ACCOUNT='{accountId}' AND TRADE_STATUS IN ('OPEN','P-OPEN')\
                AND (SELL_ORDER_STATUS IS NULL OR SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE'))"

        mySQLCursor.execute(selectStatment)                                                                                    
        rowCount = mySQLCursor.rowcount
        if ( rowCount > 0 ):
            tradeDataDict = {}
            tradeDataDict['accountId'] = accountId
            tradeDataDict['broker'] = broker                                       
            tradeDataDict['exchange'] = 'NFO'
            tradeDataDict['futOrderType'] = 'LMT'
            tradeDataDict['strategyId'] = strategyId
            tradeDataDict['signalDate'] = util.get_date_time_formatted('%d-%m-%Y %H:%M:%S')
            results = mySQLCursor.fetchall() 
            for row in results:                                            
                tradeDataDict['futTransactionType'] = row[0]
                tradeDataDict['quantity'] = int(row[1])
                tradeDataDict['futTradeSymbol'] = row[2]
                tradeDataDict['futInstToken'] = row[3]
                tradeDataDict['autoId'] = row[4]
                tradeDataDict['entryPrice'] = row[5]
                autoId = row[4]
                
                if (tradeDataDict['futTransactionType'] == 'SELL'):
                    tradeDataDict['futTransactionType'] = 'BUY'                    
                else:
                    tradeDataDict['futTransactionType'] = 'SELL'

                place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)
       
        response['status'] = 'success'     
    
    except Exception as e:                                 
        response['status'] = 'failed'
        response['remarks'] = 'Exception occurred while eod_exit_positions. The error is, ' + str(e)    

    return response

def process_exits(tradeAccounts):
    try:
        accountId = tradeAccounts[0]
        broker = tradeAccounts[1]
        global sysDict
        cnx, mySQLCursor = util.connect_mysql_db()
        userList = util.get_strategy_setttings(mySQLCursor, accountId, entryOrExit='EXIT')

        if (len(userList) > 0):
            brokerApi, isApiConnected = prostocksApi.connect_broker_api(cnx, mySQLCursor, accountId, broker)                        
            for userData in userList:
                strategyId = userData['STRATEGY_ID']
                tradeDataDict = util.copy_user_data_to_dict(strategyId, userData)
                
                if (tradeDataDict['exitStrategy'] == 'OPTIONS_PL_AND_INDICATOR_EXIT'):                                    
                    pl_based_exit(cnx, mySQLCursor, brokerApi, tradeDataDict)
                    technical_based_exit(cnx, mySQLCursor, brokerApi, tradeDataDict)

                elif (tradeDataDict['exitStrategy'] == 'OPTIONS_INDICATOR_BASED_EXIT'):
                    technical_based_exit(cnx, mySQLCursor, brokerApi, tradeDataDict)
                
                elif (tradeDataDict['exitStrategy'] == 'FUT_PATTERN_BASED_EXIT'):
                    
                    pl_based_exit(cnx, mySQLCursor, brokerApi, tradeDataDict)                    
                    technical_based_exit(cnx, mySQLCursor, brokerApi, tradeDataDict)
                    
                elif (tradeDataDict['exitStrategy'] == 'OPTIONS_SELL_WITH_HEDGE'):
                    ti_options_sell_with_hedge_exit(cnx, mySQLCursor, brokerApi, tradeDataDict)
                
                elif (tradeDataDict['exitStrategy'] == 'OPTIONS_SHORT_STRANGLE_EXIT'):                                    
                    short_strangle_exit(cnx, mySQLCursor, brokerApi, tradeDataDict)
                
                elif (tradeDataDict['exitStrategy'] == 'CUMMLATIVE_PL_AND_INDICATOR_EXIT'):
                    hedged_futures_exit(cnx, mySQLCursor, brokerApi, tradeDataDict)               

            if (tradeDataDict['eodExitFlag'] == 'Y' and int(currentTime) > 1525):
                response = eod_exit_positions(cnx, mySQLCursor, strategyId, tradeDataDict['accountId'], tradeDataDict['broker'], brokerApi)
                if (response['status'] == 'failed'):                                  
                    util.add_logs(cnx, mySQLCursor, 'ERROR',  response['remarks'], sysDict)
            
            exit_pl_based_trade_service(cnx, mySQLCursor, brokerApi) 

            cnx.commit()        

    except Exception as e:
        alertMsg = f"Exception occured in the main exit (inside) program: {str(e)}"
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, sysDict)

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
        # Verify whether the connection to MySQL database is open
        cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)
            
        sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
        currentTime = int(util.get_date_time_formatted("%H%M"))
        
        if (Config.TESTING_FLAG or (int(currentTime) < int(sysSettings['SYSTEM_END_TIME']))):

            activeAccountsList =  util.get_active_trade_accounts(mySQLCursor, entryOrExit='EXIT')
            
            start = time.perf_counter()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                results = executor.map(process_exits, activeAccountsList)
            
            end = time.perf_counter()

            print (f"Completed in {end - start}")


        elif (int(currentTime) > int(sysSettings['SYSTEM_END_TIME'])):
            alertMsg = 'System end time reached; exiting the program now' 
            util.add_logs(cnx, mySQLCursor, 'ALERT', alertMsg, sysDict)
            programExitFlag = False
        else:
            time.sleep(10)
        
        util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
        util.disconnect_db(cnx, mySQLCursor)

    else:        
        alertMsg = f"Unable to connect admin trade account from main exit program"
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, sysDict)