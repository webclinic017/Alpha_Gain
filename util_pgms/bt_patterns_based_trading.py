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
from utils import chart_patterns as cp
from utils import trade_scoring_copy as tsc



def insert_pattern_analysis(cnx, mySQLCursor, configDict, insertPaternAnalysis):
       
    # Insert pattern analysis data
    try:   
        cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
        insertQuery = "INSERT INTO BT_PATTERN_ANALYSIS \
                (UPDATED_ON, INSTRUMENT_TOKEN, TRADE_SYMBOL, STOCK_NAME, CURRENT_MKT_PRICE, TRENDLINE_PRICE, REVERSAL_BUY_SELL, BUY_FAILED_SWING, \
                    BUY_DOUBLE_BOTTOM, BUY_NON_FAILURE_SWING, BUY_RISING_CHANNEL, BUY_HORIZONDAL_CHANNEL, HEAD_AND_SHOULDER, \
                    INVERTED_HEAD_AND_SHOULDER, BROADENING_TOP, BROADENING_BOTTOM, TRIANGLE_TOP, TRIANGLE_BOTTOM, RECTANGLE_TOP, \
                    RECTANGLE_BOTTOM, CUP_AND_HANDLE, BULLISH_REVERSAL_SCORE, BULLISH_REVERSAL_SCORE_PCT, INTERVAL_TIME,\
                    HT_CYCLE_SCORE, CDL_MAX_SCORE, CDL_TOTAL_SCORE, PRECEDING_TREND_SCORE, TREND_LEVEL_SCORE , OVER_EXTENSION_SCORE, \
                    RETRACEMENT_LEVEL_SCORE, VOLUME_OPEN_INTERST_SCORE, SELLING_CLIMAX_SCORE, SIGNAL_DATE_TIME, STACKED_EMA_SCORE, \
                    TTMS_SCORE, SUPPORT_SCORE, BULLISH_DIVERGENCE_SCORE, TTMS_LENGTH, STACKED_EMA_SELL_SCORE, BELOW_BUY_PRICE, ABOVE_SELL_PRICE, \
                    ATR_BASED_BUY_EXIT, ATR_BASED_SELL_EXIT,EMAS_LENGTH, ACTUAL_BUY_SELL, PROFIT_PCT, TRADE_VALUE) \
                    VALUES(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            
       

        mySQLCursor.executemany(insertQuery, insertPaternAnalysis)
        cnx.commit()
    except Exception as e:
        logging.info('ERROR: Unable to insert pattern analysis data: ' + str(e))
    logging.info("--------------------------------------------------")

def get_patterns_signal(kite, instrumentToken, interval, stockName, tradeSymbol, fromDate, toDate):


    patternSignal = ""

    histRecords = baf.get_historical_data(kite, instrumentToken, fromDate, toDate, interval)
    df = pd.DataFrame(histRecords)
    extrema, prices, smooth_extrema, smooth_prices = cp.find_extrema(
        df, bw="cv_ls"
    )

    lastMktPrice = prices.tail(1).values[0]
    
    signalDate = df['date'].tail(1).values[0]
    currDateTime = signalDate

    patterns, trendLinePrice  = cp.find_patterns(extrema, prices, max_bars=100)
    reversalScoreDict = tsc.get_bullish_reversal_score(df, extrema, prices, interval, stockName)
    insertVal = []     
    if (len(reversalScoreDict) > 0):
        REVERSAL_BUY_SELL = ''
        BUY_FAILED_SWING = ''
        BUY_DOUBLE_BOTTOM = ''
        BUY_NON_FAILURE_SWING = ''
        BUY_RISING_CHANNEL = ''
        BUY_HORIZONDAL_CHANNEL = ''
        HEAD_AND_SHOULDER = ''
        INVERTED_HEAD_AND_SHOULDER = ''
        BROADENING_TOP = ''
        BROADENING_BOTTOM = ''
        TRIANGLE_TOP = ''
        TRIANGLE_BOTTOM = ''
        RECTANGLE_TOP = ''
        RECTANGLE_BOTTOM = ''
        CUP_AND_HANDLE = ''
        
        
        for name, pattern_periods in patterns.items():
            
            if (name == 'Buy' or name == 'Sell'):
                REVERSAL_BUY_SELL = name
                patternSignal = name      
            elif (name == 'Buy Failed Swing'):
                BUY_FAILED_SWING = name
            elif (name == 'Buy Double Bottom'):   
                BUY_DOUBLE_BOTTOM = name     
            elif (name == 'Buy Non-Failure Swing'):
                BUY_NON_FAILURE_SWING = name
            elif (name == 'Buy Rising Channel'):
                BUY_RISING_CHANNEL = name
            elif (name == 'Buy Horizondal Channel'):
                BUY_HORIZONDAL_CHANNEL = name
            elif (name == 'HS'):
                HEAD_AND_SHOULDER = name
            elif (name == 'IHS'):
                INVERTED_HEAD_AND_SHOULDER = name
            elif (name == 'BTOP'):
                BROADENING_TOP = name
            elif (name == 'BBOT'):
                BROADENING_BOTTOM = name
            elif (name == 'TTOP'):
                TRIANGLE_TOP = name
            elif (name == 'TBOT'):
                TRIANGLE_BOTTOM = name
            elif (name == 'RTOP'):
                RECTANGLE_TOP = name
            elif (name == 'RBOT'):
                RECTANGLE_BOTTOM = name
            elif (name == 'Buy Cup and Handle'):
                CUP_AND_HANDLE = name

        
        logging.info(f"Call: {str(patternSignal)} at {str(currDateTime)} and the current price {str(lastMktPrice)} ; interval: {str(interval)}; bullishReversalScore: {str(reversalScoreDict['netScore'])}")
        reversalScoreDict['signalDate'] = signalDate
        updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")

        
        insertVal.insert(len(insertVal), str(updatedOn))              
        insertVal.insert(len(insertVal), str(instrumentToken))
        insertVal.insert(len(insertVal), str(tradeSymbol))
        insertVal.insert(len(insertVal), str(stockName))
        insertVal.insert(len(insertVal), str(lastMktPrice))               
        insertVal.insert(len(insertVal), str(trendLinePrice))
        insertVal.insert(len(insertVal), str(REVERSAL_BUY_SELL))    
        insertVal.insert(len(insertVal), str(BUY_FAILED_SWING))            
        insertVal.insert(len(insertVal), str(BUY_DOUBLE_BOTTOM))            
        insertVal.insert(len(insertVal), str(BUY_NON_FAILURE_SWING))            
        insertVal.insert(len(insertVal), str(BUY_RISING_CHANNEL)) 
        insertVal.insert(len(insertVal), str(BUY_HORIZONDAL_CHANNEL))
        insertVal.insert(len(insertVal), str(HEAD_AND_SHOULDER))
        insertVal.insert(len(insertVal), str(INVERTED_HEAD_AND_SHOULDER))
        insertVal.insert(len(insertVal), str(BROADENING_TOP))
        insertVal.insert(len(insertVal), str(BROADENING_BOTTOM))
        insertVal.insert(len(insertVal), str(TRIANGLE_TOP))
        insertVal.insert(len(insertVal), str(TRIANGLE_BOTTOM))
        insertVal.insert(len(insertVal), str(RECTANGLE_TOP))
        insertVal.insert(len(insertVal), str(RECTANGLE_BOTTOM))
        insertVal.insert(len(insertVal), str(CUP_AND_HANDLE))
        insertVal.insert(len(insertVal), str(reversalScoreDict['netScore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['netScorePct']))
        insertVal.insert(len(insertVal), str(interval))
        insertVal.insert(len(insertVal), str(reversalScoreDict['htCycleScore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['cdlMaxScore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['cdlTotalScore'] ))
        insertVal.insert(len(insertVal), str(reversalScoreDict['precedingtrendscore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['trendLevelScore'] ))
        insertVal.insert(len(insertVal), str(reversalScoreDict['overExtensionScore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['retracementLevelScore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['volumeOpenInterstScore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['sellingClimaxScore']))
        insertVal.insert(len(insertVal), str(signalDate))
        insertVal.insert(len(insertVal), str(reversalScoreDict['stackedEMAScore']))    
        insertVal.insert(len(insertVal), str(reversalScoreDict['TTMSscore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['supportScore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['bullishDivergenceScore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['TTMSLengthVal']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['stackedEMASellScore']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['belowBuyPrice']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['aboveSellPrice']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['ATRbasedBuyExit']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['ATRbasedSellExit']))
        insertVal.insert(len(insertVal), str(reversalScoreDict['EMASLength']))
        


        return patternSignal, lastMktPrice, insertVal, reversalScoreDict
    else:
        return "", 0, insertVal, reversalScoreDict


def insert_bt_trade(cnx, mySQLCursor, interval, overNightPosFlag, minPriceChange, SIGNAL_DATE_TIME, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, buy_sell):
    currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')     
    sqlQuery = f"INSERT INTO BT_TRADES (INTERVAL_TIME, OVER_NIGHT_POS_FLG, MIN_PRICE_CHANGE, SYS_TIME, BUY_DATE, BUY_PRICE, STOCK_NAME, STRATEGY, ORDER_STATUS, QUANTITY, BUY_VALUE, BUY_SELL) \
        VALUES ('{str(interval)}',  '{str(overNightPosFlag)}', '{str(minPriceChange)}', '{str(currDateTime)}', '{str(SIGNAL_DATE_TIME)}', {str(lastMktPrice)},'{str(futTradeSymbol)}','{str(strategyId)}','ACTIVE',\
            {str(quantity)},{str(futBuyValue)},'{str(buy_sell)}');"
    mySQLCursor.execute(sqlQuery)
    # commit records
    cnx.commit()   

def update_bt_trade(cnx, mySQLCursor, sellValue, SIGNAL_DATE_TIME, lastMktPrice, profitAmount, profitPercent, tradeNo):
    sqlQuery = f"UPDATE BT_TRADES set SELL_VALUE= {str(sellValue)}, ORDER_STATUS='CLOSED', SELL_DATE= '{str(SIGNAL_DATE_TIME)}', SELL_PRICE = {str(lastMktPrice)},\
                    PROFIT_AMOUNT ={str(profitAmount)}, PROFIT_PERCENT ={str(profitPercent)} WHERE TRADE_NO= {str(tradeNo)}"
        
    mySQLCursor.execute(sqlQuery)
    # commit records
    cnx.commit()   



def eod_exit_pattern_based_positions(cnx, mySQLCursor, strategyId, accountId, brokerAPI):
    data = "jData={\"uid\":\"R0428\", \"actid\":\"R0428\"}&jKey=" + brokerAPI
    response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PositionBook', data=data)
    orderData = response.json()
    currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
    exitAllPosFlag = False
    if (isinstance(orderData, list) and len(orderData) > 0 and orderData[0]['stat'] == 'Ok'):
        for orders in orderData:
            exch = orders["exch"]
            posTradeSymbol = orders["tsym"]    
            brokerQuantity = int(orders["netqty"])
            if (exch == "NFO" and int(brokerQuantity) != 0):
                selectStatment = "SELECT AUTO_ID, CURRENT_MKT_PRICE  FROM TRADE_TRANSACTIONS WHERE TRADE_ACCOUNT= '"+ str(accountId) + "' \
                                    AND STRATEGY_ID= '"+ str(strategyId) + "' AND (TRADE_STATUS ='OPEN' AND (SELL_ORDER_STATUS IS NULL OR SELL_ORDER_STATUS= '' OR SELL_ORDER_STATUS ='ABANDONED')) AND TRADE_SYMBOL = '"+ str(posTradeSymbol) + "'"
                mySQLCursor.execute(selectStatment)
                results = mySQLCursor.fetchall()
                # gets the number of rows affected by the command executed                                        
                for row in results:                        
                    autoId = row[0]
                    lastMktPrice = row[1]
                    tradeDataDict = {}
                    tradeDataDict['exchange'] = 'NFO'
                    tradeDataDict['broker'] = 'PROSTOCKS'
                    tradeDataDict['futOrderType'] = 'MKT' 
                    tradeDataDict['quantity'] = abs(brokerQuantity)
                    if (brokerQuantity > 0):  
                        tradeDataDict['futTransactionType'] = "SELL"
                    else:
                        tradeDataDict['futTransactionType'] = "BUY"

                    tradeDataDict['accountId'] = 'R0428'  
                    tradeDataDict['futTriggerPrice'] = lastMktPrice     
                    tradeDataDict['futLastPrice'] = lastMktPrice  
                    # Frame the trade symbol that matches PROSTOCKS instruments name
                    tradeDataDict['futTradeSymbol']= posTradeSymbol

                    orderId, orderRemarks  = baf.place_future_buy_order(brokerAPI, tradeDataDict)
                    
                    if (isinstance(orderId, list) and orderId[0] > 0):
                        orderId = orderId[0]                          

                    if (int(orderId) > 0):                    
                        try:                                            
                            updateQuery = "UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_DATE='"+str(currDateTime)+"', UPDATED_ON='"+str(currDateTime)+"', SELL_ORDER_STATUS='PENDING', \
                                            SELL_ORDER_ID='"+str(orderId)+"' WHERE AUTO_ID="+str(autoId)
                        
                            mySQLCursor.execute(updateQuery) 
                            cnx.commit()
                            alertMsg = f"*Stock Name:  {str(posTradeSymbol).replace('&', ' and ')} *\n*Call:  Sell*\nExit Price: {str(lastMktPrice)}\nReason: Sold as part of exit all postions intructions"
                            util.send_usr_alerts(logEnableFlag, cnx, mySQLCursor, 'LIVE', alertMsg, userName=tradeDataDict['accountId'], programName=programName, strategyId='1', stockName=posTradeSymbol, telgUpdateFlag='Y', chatId=TELG_ADMIN_ID)
                        except Exception as e:
                            logging.info('Unable to update the exit status in database (in > 0): ' + str(e))
        
        exitAllPosFlag = True

    return exitAllPosFlag


def get_pattern_analysis (cnx, mySQLCursor, kite):
    fromDate = util.get_lookup_date(400)
    toDate = util.get_lookup_date(0)
    instList = util.get_test_inst_list(mySQLCursor)
    for instRow in instList:
        instrumentToken = instRow[0]
        tradeSymbol = instRow[1]                            
        stockName = instRow[2]       
        print("Checking Insturment: " + stockName)        
        # patternSignal, lastMktPrice, bullishReversalScore = get_patterns_signal(kite, instrumentToken, fromDate, toDate, 'day', stockName, tradeSymbol)


def meeting_entry_conditions(strategyId, patternSignal, reversalScoreDict1):
    entryConditionsMetFlag = False
    if (strategyId == 'ALPHA_9' and 
        ((patternSignal == 'Buy' and float(reversalScoreDict1['stackedEMAScore']) >= 10 and
            (float(reversalScoreDict1['bullishCandlePct']) >= 0.80 )) or 
                (patternSignal == 'Sell' and float(reversalScoreDict1['stackedEMASellScore']) >= 10) and 
                    (float(reversalScoreDict1['bearishCandlePct']) >= 0.80 ))):
        
        entryConditionsMetFlag = True
    
    elif(strategyId == 'ALPHA_10' and 
        (float(reversalScoreDict1['EMASLength']) >= 1) and ((patternSignal == 'Buy' and float(reversalScoreDict1['stackedEMAScore']) >= 10 
            and float(reversalScoreDict1['belowBuyPrice']) == 1) or (patternSignal == 'Sell' and 
                float(reversalScoreDict1['stackedEMASellScore']) >= 10 and float(reversalScoreDict1['aboveSellPrice']) == 1))):
        
        entryConditionsMetFlag = True
    
    elif(strategyId == 'ALPHA_2' and 
        (float(reversalScoreDict1['EMASLength']) >= 1) and ((patternSignal == 'Buy' and float(reversalScoreDict1['stackedEMAScore']) >= 10 
            and float(reversalScoreDict1['belowBuyPrice']) == 1) or (patternSignal == 'Sell' and 
                float(reversalScoreDict1['stackedEMASellScore']) >= 10 and float(reversalScoreDict1['aboveSellPrice']) == 1))):
        
        entryConditionsMetFlag = True

    elif(strategyId == 'ALPHA_1' and 
        (float(reversalScoreDict1['EMASLength']) >= 1) and ((patternSignal == 'Buy' and float(reversalScoreDict1['stackedEMAScore']) >= 10 
            and float(reversalScoreDict1['belowBuyPrice']) == 1) or (patternSignal == 'Sell' 
                and float(reversalScoreDict1['stackedEMASellScore']) >= 10 and float(reversalScoreDict1['aboveSellPrice']) == 1))):
        
        entryConditionsMetFlag = True

    return entryConditionsMetFlag
                
  
def check_existing_bt_order(cursor, stockName, strategyId):
    selectStatment = "SELECT TRADE_NO, BUY_DATE, BUY_PRICE, QUANTITY FROM BT_TRADES WHERE STOCK_NAME=\"" + str(stockName) + "\" AND ORDER_STATUS ='ACTIVE' AND STRATEGY='"+strategyId+"'"
    cursor.execute(selectStatment)
    # gets the number of rows affected by the command executed
    rowCount = cursor.rowcount
    buyDate = ""
    buyPrice = ""
    quantity = 0
    buyIndex = 0
    tradeNo = 0

    if rowCount == 0:
        return False, buyPrice, quantity, tradeNo, buyDate
    else:
        results = cursor.fetchall()
        for row in results:
            tradeNo = row[0]
            buyDate = row[1]
            buyPrice = row[2]
            quantity = row[3]

        return True, buyPrice, quantity, tradeNo, buyDate

def patterns_based_entry_check(cnx, mySQLCursor, kite, strategyId, accountId, instList, reportStart):
    insertPaternAnalysis = []
    interval = ''
    confirmInterval1 = ''
    confirmInterval2 = ''
    cutOffStartFlag = False
    broker = 'PROSTOCKS'   
    overNightPosFlag = False
    
    if (strategyId == 'ALPHA_1'):      
        exitProfitPercent =  1
        interval = '2hour'
        confirmInterval1 = '3hour'
        confirmInterval2 = 'day'
        
        fromDate1 = datetime.datetime.strptime(get_from_date_based_interval(interval, dateNow=reportStart), '%Y-%m-%d')                
        fromDate2 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval1, dateNow=reportStart), '%Y-%m-%d')                
        fromDate3 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval2, dateNow=reportStart), '%Y-%m-%d')      
        toDate = datetime.datetime.strptime(util.get_lookup_date(0, dateNow=reportStart), '%Y-%m-%d')
    
    elif (strategyId == 'ALPHA_2'): 
        exitProfitPercent = 0.3     
        interval = '30minute'
        confirmInterval1 = '60minute'
        confirmInterval2 = '2hour'
        
        fromDate1 = datetime.datetime.strptime(get_from_date_based_interval(interval, dateNow=reportStart), '%Y-%m-%d')                
        fromDate2 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval1, dateNow=reportStart), '%Y-%m-%d')                
        fromDate3 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval2, dateNow=reportStart), '%Y-%m-%d')      
        toDate = datetime.datetime.strptime(util.get_lookup_date(0, dateNow=reportStart), '%Y-%m-%d')
        
    
    elif (strategyId == 'ALPHA_9'):
        exitProfitPercent =  0.5      
        interval = '15minute'
        confirmInterval1 = '30minute'
        if (int(currentTime) > int(sysSettings['PBE_CUT_OFF_START_TIME'])):
            cutOffStartFlag = True
    
        fromDate1 = datetime.datetime.strptime(get_from_date_based_interval(interval, dateNow=reportStart), '%Y-%m-%d')                
        fromDate2 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval1, dateNow=reportStart), '%Y-%m-%d')                    
        toDate = datetime.datetime.strptime(util.get_lookup_date(0, dateNow=reportStart), '%Y-%m-%d')
        
    elif (strategyId == 'ALPHA_10'):  
        exitProfitPercent = 0.3    
        interval = '30minute'
        confirmInterval1 = '60minute'
        confirmInterval2 = '2hour'

        fromDate1 = datetime.datetime.strptime(get_from_date_based_interval(interval, dateNow=reportStart), '%Y-%m-%d')                
        fromDate2 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval1, dateNow=reportStart), '%Y-%m-%d')                
        fromDate3 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval2, dateNow=reportStart), '%Y-%m-%d')      
        toDate = datetime.datetime.strptime(util.get_lookup_date(0, dateNow=reportStart), '%Y-%m-%d')
            
    
    else:   
        exitProfitPercent = 0.3  
        interval = '30minute'
        confirmInterval1 = '60minute'
        confirmInterval2 = '2hour'

        fromDate1 = datetime.datetime.strptime(get_from_date_based_interval(interval, dateNow=reportStart), '%Y-%m-%d')                
        fromDate2 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval1, dateNow=reportStart), '%Y-%m-%d')                
        fromDate3 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval2, dateNow=reportStart), '%Y-%m-%d')      
        toDate = datetime.datetime.strptime(util.get_lookup_date(0, dateNow=reportStart), '%Y-%m-%d')
            

    
    for instRow in instList:
        instrumentToken = instRow[0]
        tradeSymbol = instRow[1]                            
        stockName = instRow[2]
        reEntryFlag = instRow[3]
        patternSignal = ''
        confirmPatternSignal1 = ''
        confirmPatternSignal2 = ''        
        patternDataList1  = []
        patternDataList2 = []
        patternDataList3  = []
        tradeDataDict = {}        

        patternSignal, lastMktPrice, patternDataList1, reversalScoreDict1 = get_patterns_signal(kite, instrumentToken, interval, stockName, tradeSymbol, fromDate1, toDate)                        
        
        confirmPatternSignal1, lastMktPrice1, patternDataList2, reversalScoreDict2 = get_patterns_signal(kite, instrumentToken, confirmInterval1, stockName, tradeSymbol, fromDate2, toDate)                          

        if (strategyId == 'ALPHA_10' or strategyId == 'ALPHA_1' or strategyId == 'ALPHA_2'):  
            confirmPatternSignal2, lastMktPrice1, patternDataList3, reversalScoreDict3 = get_patterns_signal(kite, instrumentToken, confirmInterval2, stockName, tradeSymbol, fromDate3, toDate)         

        if (strategyId == 'ALPHA_2'):
            if(patternSignal == 'Buy'):
                futInstName, futInstToken, lotSize, expDate,strikePrice  = util.get_options_instruments(mySQLCursor, tradeSymbol, 'BUY', lastMktPrice)
                tradeDataDict['futTradeSymbol'] = tradeSymbol + expDate + 'C' + str(strikePrice).replace('.0','')   
                tradeDataDict['instrumentType'] = 'CALL'
            else:
                futInstName, futInstToken, lotSize, expDate,strikePrice  = util.get_options_instruments(mySQLCursor, tradeSymbol, 'SELL', lastMktPrice)
                tradeDataDict['futTradeSymbol'] = tradeSymbol + expDate + 'P' + str(strikePrice).replace('.0','')
                tradeDataDict['instrumentType'] = 'PUT'

            
            tradeDataDict['tgtProfitPct'] = 20                     
            tradeDataDict['tgtStopLossPct'] = -20            
            tradeDataDict['trailingThresholdPct'] = 80
            

        else:
            futInstName, futInstToken, lotSize, expDate = util.get_futures_instruments(mySQLCursor, tradeSymbol)
            tradeDataDict['futTradeSymbol']= tradeSymbol + expDate + 'F'
            tradeDataDict['instrumentType'] = 'FUT'
            
            if (strategyId == 'ALPHA_1'):                
                tradeDataDict['tgtProfitPct'] = 5                     
                tradeDataDict['tgtStopLossPct'] = -3
                tradeDataDict['trailingThresholdPct'] = 80
            else:
                tradeDataDict['tgtProfitPct'] = 3                     
                tradeDataDict['tgtStopLossPct'] = -1            
                tradeDataDict['trailingThresholdPct'] = 80

        tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        
        logging.info(f"futTradeSymbol: {tradeDataDict['futTradeSymbol']}")

        tradeDataDict['signalId'] = 1
        tradeDataDict['futInstToken'] = futInstToken                
        tradeDataDict['futLastPrice'] = lastMktPrice
        tradeDataDict['futTriggerPrice'] = lastMktPrice
        
        if (strategyId == 'ALPHA_2'):        
            tradeDataDict['quantity'] = int(lotSize) * 2
            tradeDataDict['futBuyValue'] = tradeDataDict['quantity'] * float(lastMktPrice)
            
        else:
            tradeDataDict['quantity'] = int(lotSize)
            tradeDataDict['futBuyValue'] = tradeDataDict['quantity'] * float(lastMktPrice)
            

        tradeDataDict['orderStatus']= 'PENDING'                
        tradeDataDict['strategyId'] = strategyId
        tradeDataDict['accountId'] = accountId
        tradeDataDict['tgtProfitAmt'] = 100000
        tradeDataDict['tgtStopLossAmt'] = 100000
        tradeDataDict['exitStrategyId'] = 'TRAILING_TGT_TRIGGER'
        tradeDataDict['instrumentToken'] = instrumentToken
        tradeDataDict['exchange'] = 'NFO'
        tradeDataDict['broker'] = broker
        tradeDataDict['futOrderType'] = 'LMT'                
        # Frame the trade symbol that matches PROSTOCKS instruments name

        tradeDataDict['orderType'] = 'REVERSAL FUTURE'        
        tradeDataDict['futTransactionType'] = "BUY"
        
        if (strategyId != 'ALPHA_2' and patternSignal == 'Sell'):
            tradeDataDict['futTransactionType'] = "SELL"
   
        tradeDataDict['isPaperTrade'] = 'NO'
        orderId = 0
        orderRemarks = ""
        
        existingPositionFound, buyOrderPrice, existingOrderQuantity, tradeNo, buyDate = check_existing_bt_order(mySQLCursor, tradeDataDict['futTradeSymbol'], strategyId)

        confirmPatternMatchFlag = util.check_confirm_pattern_match(strategyId, 'ENTRY', patternSignal, confirmPatternSignal1, confirmPatternSignal2)        
        
      
        newOrExistingPosFlag = False
        
        logging.info(f"EMASLength: {str(reversalScoreDict1['EMASLength'])}")
        logging.info(f"stackedEMAScore: {str(reversalScoreDict1['stackedEMAScore'])}")
        logging.info(f"belowBuyPrice: {str(reversalScoreDict1['belowBuyPrice'])}")
        logging.info(f"stackedEMASellScore: {str(reversalScoreDict1['stackedEMASellScore'])}")
        logging.info(f"aboveSellPrice: {str(reversalScoreDict1['aboveSellPrice'])}")
        logging.info(f"bullishCandlePct: {str(reversalScoreDict1['bullishCandlePct'])}")
        logging.info(f"bearishCandlePct: {str(reversalScoreDict1['bearishCandlePct'])}")

        entryConditionsMetFlag = meeting_entry_conditions(strategyId, patternSignal, reversalScoreDict1)
     

        if (patternSignal == 'Buy' or patternSignal == 'Sell' ):       
            # Initial Buy or Sell
            if (reEntryFlag == 'YES' and not(existingPositionFound) and \
                    existingOrderQuantity == 0 and not(cutOffStartFlag) and confirmPatternMatchFlag and entryConditionsMetFlag) :
                
                print('-----ALL OTHER BUY CONDITIONS MET------')                
                fromDate4 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval1, dateNow=reportStart), '%Y-%m-%d')                
                fromDate5 = datetime.datetime.strptime(get_from_date_based_interval(confirmInterval2, dateNow=reportStart), '%Y-%m-%d')  
                confirmPatternSignal3, lastMktPrice1, patternDataList4, reversalScoreDict4 = util.get_patterns_signal(kite, instrumentToken, 'day', stockName, tradeSymbol, fromDate4, toDate)                        
                confirmPatternSignal4, lastMktPrice1, patternDataList5, reversalScoreDict5 = util.get_patterns_signal(kite, instrumentToken, 'week', stockName, tradeSymbol, fromDate5, toDate)      
                
                # Make sure we buy/sell only the trend directional stocks only
                if ((strategyId == 'ALPHA_9' and patternSignal== confirmPatternSignal3) or 
                        (strategyId == 'ALPHA_10' and patternSignal== confirmPatternSignal3) or 
                            (patternSignal== confirmPatternSignal3 and confirmPatternSignal3 == confirmPatternSignal4)):                        
                    
                    insert_bt_trade(cnx, mySQLCursor, interval, overNightPosFlag, exitProfitPercent, reversalScoreDict1['signalDate'], lastMktPrice, tradeDataDict['futTradeSymbol'], strategyId, tradeDataDict['quantity'], tradeDataDict['futBuyValue'], patternSignal)

                    alertMsg = f"*Stock Name:  {tradeDataDict['futTradeSymbol']}*\nCall:  Buy\n*Entry Price:  {str('%.2f' % tradeDataDict['futLastPrice'])}* \nBuy Value:  {str('%.2f' % tradeDataDict['futBuyValue'])}"
                    logging.info(alertMsg)
                    newOrExistingPosFlag = True
                    profitPercent = 0

                    patternDataList1, patternDataList2, patternDataList3 = util.update_pattern_list(patternDataList1, patternDataList2, patternDataList3, tradeDataDict['futTransactionType'], profitPercent, tradeDataDict['futBuyValue'])


        logging.info("--------")

        if (not(newOrExistingPosFlag)):
            patternDataList1, patternDataList2, patternDataList3 = util.update_pattern_list(patternDataList1, patternDataList2, patternDataList3, '', 0, 0)

        # Mention that it is actual buy or sell in the pattern analysis data
        if (len(patternDataList1) > 0):
            insertPaternAnalysis.insert(len(insertPaternAnalysis), patternDataList1)    
        
        # Mention that it is actual buy or sell in the pattern analysis data
        if (len(patternDataList2) > 0):
            insertPaternAnalysis.insert(len(insertPaternAnalysis), patternDataList2)  
        
        # Mention that it is actual buy or sell in the pattern analysis data
        if (len(patternDataList3) > 0):
            insertPaternAnalysis.insert(len(insertPaternAnalysis), patternDataList3)

    # Added the collected pattern analysis data into database;
    insert_pattern_analysis(cnx, mySQLCursor, configDict, insertPaternAnalysis)       
  
    logging.info("--------------------------------------------------")



def get_from_date_based_interval(interval, dateNow):
    fromDate = ''
    if (interval == '15minute'):
        fromDate = util.get_lookup_date(24, dateNow=dateNow)
    elif (interval == '30minute'):
        fromDate = util.get_lookup_date(38 , dateNow=dateNow)
    elif (interval == '60minute'):
        fromDate = util.get_lookup_date(65, dateNow=dateNow)
    elif (interval == '2hour'):
        fromDate = util.get_lookup_date(130, dateNow=dateNow)
    elif (interval == 'day'):
        fromDate = util.get_lookup_date(400, dateNow=dateNow)
    elif (interval == 'week'):
        fromDate = util.get_lookup_date(2000, dateNow=dateNow)
    else:
        fromDate = util.get_lookup_date(200, dateNow=dateNow)

    return fromDate
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

    programName = configDict['PATTERNS_BASED_ENTRY_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['PATTERNS_BASED_ENTRY_PGM_NAME']) + '.log')

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
            # Verify whether the connection to MySQL database is open
        cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
            
        sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
        currentTime = util.get_date_time_formatted("%H%M")
            
            
        try:
            
            strategyId='ALPHA_9'
            accountId='R0428'
            broker = 'PROSTOCKS'
            
            reportStart = '2021-01-01'
            fromDate= '2021-01-01'
            reportEnd = '2021-02-01'


            instList = util.get_user_defined_inst_list(mySQLCursor, strategyId, accountId, 'ENTRY')            
            brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, accountId, broker)
            dayCnter = 0
            programExitFlag == 'N' 
            while programExitFlag != 'Y':                        
                logging.info("--------------------ALPHA9 STRATEGY START  - DAY TRADING--------------------")
                patterns_based_entry_check(cnx, mySQLCursor, kite, strategyId, accountId, instList, fromDate)
                
                # logging.info("--------------------ALPHA10 STRATEGY STRAT - BUY TODAY SELL TOMMORROW--------------------")
                # patterns_based_entry_check(cnx, mySQLCursor, kite, sysSettings, strategyId='ALPHA_10', accountId='R0428')
                
                # logging.info("--------------------ALPHA1 STRATEGY STRAT -  SWING TRADING--------------------")
                # patterns_based_entry_check(cnx, mySQLCursor, kite, sysSettings, strategyId='ALPHA_1', accountId='R0428')                    
                
                # logging.info("--------------------ALPHA2 STRATEGY STRAT - OPTIONS TRADING--------------------")
                # patterns_based_entry_check(cnx, mySQLCursor, kite, sysSettings, strategyId='ALPHA_2', accountId='R0428')    
                
                dayCnter += 1                
                fromDate = (datetime.datetime.strptime(reportStart, "%Y-%m-%d") + datetime.timedelta(days=dayCnter)).strftime("%Y-%m-%d")
                
                if (reportStart == reportEnd):
                    programExitFlag == 'Y'
            # logging.info("--------------------ALPHA1 PAPER TRADE--------------------")
            # patterns_based_entry_check(cnx, mySQLCursor, kite, sysSettings, strategyId='ALPHA_1', accountId='R0428', isPaperTrade=True)


        except Exception as e:
            alertMsg = "ERROR: Live trade service failed (main block): " + str(e)
            logging.info(alertMsg)

         
            
        # util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
        util.disconnect_db(cnx, mySQLCursor)


    util.logger(logEnableFlag, "info", "Program ended")
