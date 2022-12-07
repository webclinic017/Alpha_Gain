from importlib.resources import path
import matplotlib.pyplot as plt
import pandas as pd
from scipy.signal import argrelextrema
import live_trades_service as lts
from utils import util_functions as util
from utils import trade_scoring_copy as tsc
from utils import broker_api_functions as baf
from utils import bt_chart_patterns as cp
from scipy.stats import linregress
import logging
import datetime


def get_fixed_trailing_sl(profitPercent, oldSlPercent, slPercent):
    exitSignalFlag = False
    slUpdateFlag = False
    newSLPercent = 0

    if (profitPercent <= oldSlPercent):
        # Stop loss hit, send exit signal
        exitSignalFlag = True

    elif (profitPercent > oldSlPercent and profitPercent > 0):
        if (oldSlPercent > slPercent):
            newSLPercent = slPercent + profitPercent 
        else:
            newSLPercent = oldSlPercent + profitPercent 

        if(newSLPercent > oldSlPercent):            
            slUpdateFlag = True

    return exitSignalFlag, slUpdateFlag, newSLPercent
 


def get_patterns_signal(df, instrumentToken, interval, stockName, tradeSymbol, inputParams):
    currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')    
    patternSignal = ""
    extrema, prices, smooth_extrema, smooth_prices = cp.find_extrema(df, inputParams, bw="cv_ls")

    lastMktPrice = prices.tail(1).values[0]
    signalDate = df['date'].tail(1).values[0]

    patterns, trendLinePrice, stopLossPct  = cp.find_patterns(extrema, prices, max_bars=100)
    reversalScoreDict = tsc.get_bullish_reversal_score(df, extrema, prices, interval, stockName)
    # netScore, totalBuyScore, totalSellScore = tsc.getBuySellScore(df)
    insertVal = []     
    if (len(reversalScoreDict) > 0):
        reversalScoreDict['stopLossPct'] = stopLossPct
        reversalScoreDict['signalDate'] = signalDate
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

        
        # logging.info(f"Call: {str(patternSignal)} at {str(signalDate)} and the current price {str(lastMktPrice)} ; interval: {str(interval)}; bullishReversalScore: {str(reversalScoreDict['netScore'])}")

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
        return patternSignal, lastMktPrice, insertVal, reversalScoreDict, signalDate
    else:
        return "", 0, insertVal, reversalScoreDict, signalDate

def insert_pattern_analysis(cnx, mySQLCursor, configDict, insertPaternAnalysis, strategyVersion):
       
    # Insert pattern analysis data
    try:   
        cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
        sqlQuery = f"UPDATE BT_TRADES SET ORDER_STATUS='F-CLOSED' WHERE ORDER_STATUS= 'ACTIVE' AND STRATEGY_VERSION = '{str(strategyVersion)}'"
        
        mySQLCursor.execute(sqlQuery)
        # commit records
        cnx.commit()    

        sqlQuery = f"UPDATE BT_INPUT_PARAMETERS SET RERUN_FLAG='NO' WHERE BT_VERSION = '{str(strategyVersion)}'"        
        mySQLCursor.execute(sqlQuery)
        # commit records
        cnx.commit()    

        if (len(insertPaternAnalysis) > 0):  
            insertQuery = "INSERT INTO BT_PATTERN_ANALYSIS \
                    (UPDATED_ON, INSTRUMENT_TOKEN, TRADE_SYMBOL, STOCK_NAME, CURRENT_MKT_PRICE, TRENDLINE_PRICE, REVERSAL_BUY_SELL, BUY_FAILED_SWING, \
                        BUY_DOUBLE_BOTTOM, BUY_NON_FAILURE_SWING, BUY_RISING_CHANNEL, BUY_HORIZONDAL_CHANNEL, HEAD_AND_SHOULDER, \
                        INVERTED_HEAD_AND_SHOULDER, BROADENING_TOP, BROADENING_BOTTOM, TRIANGLE_TOP, TRIANGLE_BOTTOM, RECTANGLE_TOP, \
                        RECTANGLE_BOTTOM, CUP_AND_HANDLE, BULLISH_REVERSAL_SCORE, BULLISH_REVERSAL_SCORE_PCT, INTERVAL_TIME,\
                        HT_CYCLE_SCORE, CDL_MAX_SCORE, CDL_TOTAL_SCORE, PRECEDING_TREND_SCORE, TREND_LEVEL_SCORE , OVER_EXTENSION_SCORE, \
                        RETRACEMENT_LEVEL_SCORE, VOLUME_OPEN_INTERST_SCORE, SELLING_CLIMAX_SCORE, SIGNAL_DATE_TIME, STACKED_EMA_SCORE, \
                        TTMS_SCORE, SUPPORT_SCORE, BULLISH_DIVERGENCE_SCORE, TTMS_LENGTH, STACKED_EMA_SELL_SCORE, BELOW_BUY_PRICE, ABOVE_SELL_PRICE, \
                        ATR_BASED_BUY_EXIT, ATR_BASED_SELL_EXIT,EMAS_LENGTH, ACTUAL_BUY_SELL, PROFIT_PCT, TRADE_VALUE, TRADE_TRANSACTIONS_ID) \
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                
        

            mySQLCursor.executemany(insertQuery, insertPaternAnalysis)
            cnx.commit()    
        
    except Exception as e:
        logging.info('ERROR: Unable to insert pattern analysis data: ' + str(e))
    logging.info("--------------------------------------------------")

def check_existing_bt_order(cursor, stockName, strategyId, strategyVersion):

    selectStatment = f"SELECT TRADE_NO, BUY_DATE, BUY_PRICE, QUANTITY, MAX_PROFIT_PERCENT, MAX_PROFIT_AMOUNT, TGT_STOP_LOSS_PCT, \
         TGT_PROFIT_PCT, CURR_SL_PCT, MAX_LOSS_PERCENT, MAX_LOSS_AMOUNT FROM BT_TRADES WHERE STOCK_NAME='{str(stockName)}' AND ORDER_STATUS ='ACTIVE' AND STRATEGY='{str(strategyId)}' AND STRATEGY_VERSION = '{str(strategyVersion)}'"
    cursor.execute(selectStatment)
    # gets the number of rows affected by the command executed
    rowCount = cursor.rowcount
    buyDate = ""
    buyPrice = ""
    quantity = 0
    buyIndex = 0
    tradeNo = 0
    existingProfitPercent = 0
    existingProfitAmount = 0
    tgtStopLossPct = 0
    tgtProfitPct = 0
    oldSlPct = 0
    existingLossPercent = 0
    existingLossAmount = 0

    if rowCount == 0:
        return False, buyPrice, quantity, tradeNo, buyDate, existingProfitPercent, existingProfitAmount, tgtStopLossPct, tgtProfitPct, oldSlPct, existingLossPercent, existingLossAmount
    else:
        results = cursor.fetchall()
        for row in results:
            tradeNo = row[0]
            buyDate = row[1]
            buyPrice = row[2]
            quantity = row[3]
            existingProfitPercent = row[4]
            existingProfitAmount = row[5]
            tgtStopLossPct = row[6]
            tgtProfitPct = row[7]
            oldSlPct = row[8]
            existingLossPercent = row[9]
            existingLossAmount = row[10]

        return True, buyPrice, quantity, tradeNo, buyDate, existingProfitPercent, existingProfitAmount, tgtStopLossPct, tgtProfitPct, oldSlPct, existingLossPercent, existingLossAmount

def insert_bt_trade(cnx, mySQLCursor, reversalScoreDict, interval, overNightPosFlag, minPriceChange, SIGNAL_DATE_TIME, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, buy_sell, inputParams):
    currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')     
    sqlQuery = f"INSERT INTO BT_TRADES (INTERVAL_TIME, OVER_NIGHT_POS_FLG, MIN_PRICE_CHANGE, SYS_TIME, BUY_DATE, BUY_PRICE, STOCK_NAME, STRATEGY, ORDER_STATUS, QUANTITY, \
        BUY_VALUE, BUY_SELL, STRATEGY_VERSION, TGT_STOP_LOSS_PCT, TGT_PROFIT_PCT, CURR_SL_PCT)  VALUES ('{str(interval)}',  '{str(overNightPosFlag)}', '{str(minPriceChange)}', '{str(currDateTime)}', \
            '{str(SIGNAL_DATE_TIME)}', {str(lastMktPrice)},'{str(futTradeSymbol)}','{str(strategyId)}','ACTIVE',\
                {str(quantity)},{str(futBuyValue)},'{str(buy_sell)}','{str(inputParams['btVersion'])}', {str(reversalScoreDict['stopLossPct'])}, {str(reversalScoreDict['target_pct'])}, {str(reversalScoreDict['stopLossPct'])} );"


    mySQLCursor.execute(sqlQuery)
    # commit records
    rowId = mySQLCursor.lastrowid
    cnx.commit()       
    return rowId

def update_bt_trade(cnx, mySQLCursor, sellValue, SIGNAL_DATE_TIME, lastMktPrice, profitAmount, profitPercent, tradeNo):
    sqlQuery = f"UPDATE BT_TRADES set SELL_VALUE= {str(sellValue)}, ORDER_STATUS='CLOSED', SELL_DATE= '{str(SIGNAL_DATE_TIME)}', SELL_PRICE = {str(lastMktPrice)},\
                    PROFIT_AMOUNT ={str(profitAmount)}, PROFIT_PERCENT ={str(profitPercent)} WHERE TRADE_NO= {str(tradeNo)}"
        
    mySQLCursor.execute(sqlQuery)
    # commit records
    cnx.commit()                  


def meeting_entry_conditions(strategyId, patternSignal, reversalScoreDict1):

    entryConditionsMetFlag = False
    if (strategyId == 'ALPHA_9' and 
            (patternSignal == 'Buy' or 
                patternSignal == 'Sell' )):
        # ((patternSignal == 'Buy' and float(reversalScoreDict1['stackedEMAScore']) >= 20) or \
        #     (patternSignal == 'Sell' and float(reversalScoreDict1['stackedEMASellScore']) >= 10))):
        # ((patternSignal == 'Buy' and float(reversalScoreDict1['bullishCandlePct']) >= 0.80 and float(reversalScoreDict1['buyCandleRange']) > (0.5 * float(reversalScoreDict1['buyATRVal']))) or 
        #     (patternSignal == 'Sell' and float(reversalScoreDict1['bearishCandlePct']) >= 0.80 and float(reversalScoreDict1['sellCandleRange']) > 0.5 * float(reversalScoreDict1['sellATRVal'])))):
        
        # (patternSignal == 'Buy'  or patternSignal == 'Sell')):
        entryConditionsMetFlag = True

   


    if (strategyId == 'ALPHA_9A' and 
        ((patternSignal == 'Buy' and float(reversalScoreDict1['bullishCandlePct']) >= 0.80 and float(reversalScoreDict1['supportScore']) >= 10 and float(reversalScoreDict1['volumeOpenInterstScore']) >= 10 and float(reversalScoreDict1['sellingClimaxScore']) >= 10) or 
        (patternSignal == 'Sell' and float(reversalScoreDict1['bearishCandlePct']) >= 0.80 and float(reversalScoreDict1['supportScore']) >= 10 and float(reversalScoreDict1['volumeOpenInterstScore']) >= 10 and float(reversalScoreDict1['sellingClimaxScore']) >= 10))):
        entryConditionsMetFlag = True
        print(entryConditionsMetFlag)
        # if (strategyId == 'ALPHA_9' and 
        # ((patternSignal == 'Buy' and float(reversalScoreDict1['stackedEMAScore']) >= 10 and
        #     (float(reversalScoreDict1['bullishCandlePct']) >= 0.80 )) or 
        #         (patternSignal == 'Sell' and float(reversalScoreDict1['stackedEMASellScore']) >= 10) and 
        #             (float(reversalScoreDict1['bearishCandlePct']) >= 0.80 ))):
        
        # entryConditionsMetFlag = True

    if(strategyId == 'ALPHA_10' and 
        ((patternSignal == 'Buy' and float(reversalScoreDict1['overExtensionScore']) >= 10  ) or \
            (patternSignal == 'Sell' and float(reversalScoreDict1['overExtensionSellScore']) >= 10 ))):
        entryConditionsMetFlag = True
    
    if(strategyId == 'ALPHA_10A' and pd.to_datetime(reversalScoreDict1['signalDate']).time() >= datetime.datetime.strptime('09:15', '%H:%M').time() and
        (reversalScoreDict1['RSISellSignal'] == 'Sell' )):
        entryConditionsMetFlag = True

    elif(strategyId == 'ALPHA_10C' and 
        (float(reversalScoreDict1['EMASLength']) >= 1) and ((patternSignal == 'Buy' and float(reversalScoreDict1['stackedEMAScore']) >= 10) or \
            (patternSignal == 'Sell' and float(reversalScoreDict1['stackedEMASellScore']) >= 10))):
        
        entryConditionsMetFlag = True

    elif(strategyId == 'ALPHA_10B' and 
            ((reversalScoreDict1['buySellSignal'] == 'Buy' and patternSignal == 'Buy') or
                (reversalScoreDict1['buySellSignal'] == 'Sell' and patternSignal == 'Sell'))):
        
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


def get_mixed_trailing_sl(profitPercent, oldSlPercent, slPercent, tgtProfitPct, trailingPct):
    exitSignalFlag = False
    slUpdateFlag = False
    newSLPercent = 0

    if (profitPercent <= oldSlPercent):
        # Stop loss hit, send exit signal
        exitSignalFlag = True

    elif (profitPercent > oldSlPercent and profitPercent >= float(tgtProfitPct)):
        newSLPercent = profitPercent * (trailingPct / 100)
        if( oldSlPercent < newSLPercent ):            
            slUpdateFlag = True

    elif (profitPercent > oldSlPercent and profitPercent > 0):
        if (oldSlPercent > slPercent):
            newSLPercent = slPercent + profitPercent 
        else:
            newSLPercent = oldSlPercent + profitPercent 

        if(newSLPercent > oldSlPercent):            
            slUpdateFlag = True


    return exitSignalFlag, slUpdateFlag, newSLPercent

def get_trailing_tgt_trigger(profitPercent, tgtProfitPct, oldSlPercent, trailingPct):
    exitSignalFlag = False
    slUpdateFlag = False
    newSLPercent = 0

    # Exiting is the current profit percentatge hit target stop loss percentage
    if (profitPercent <= oldSlPercent):
        exitSignalFlag = True        

    elif (profitPercent > oldSlPercent and profitPercent >= float(tgtProfitPct)):
        newSLPercent = profitPercent * (trailingPct / 100)
        if( oldSlPercent < newSLPercent ):            
            slUpdateFlag = True
    
    return exitSignalFlag, slUpdateFlag, newSLPercent

def patterns_based_entry_check(cnx, mySQLCursor, df, overNightPosFlag, tradeSymbol, stockName, strategyId, interval, exitProfitPercent, inputParams):

    patternSignal, lastMktPrice,  patternDataList, reversalScoreDict, signalDate= get_patterns_signal(df, instrumentToken, interval, stockName, tradeSymbol, inputParams)
    
    # futInstName, futInstToken, lotSize, expDate = util.get_futures_instruments(mySQLCursor, tradeSymbol)

    futBuyValue = int(inputParams['lotSize']) * float(lastMktPrice)
    quantity = inputParams['lotSize']
 
    # Frame the trade symbol that matches PROSTOCKS instruments name
    futTradeSymbol= tradeSymbol + inputParams['expDate'] + 'F'

    existingPositionFound, buyOrderPrice, existingOrderQuantity, tradeNo, buyDate, existingProfitPercent, existingProfitAmount, tgtStopLossPct, tgtProfitPct, oldSlPct, existingLossPercent, existingLossAmount = check_existing_bt_order(mySQLCursor, futTradeSymbol, strategyId, inputParams['btVersion'])
    
    noTradeFlag = False
    profitPercent = 0 
    patternListUpdatedFlag = False
    if (existingPositionFound and not(overNightPosFlag) and str(pd.to_datetime(signalDate).time()) == '15:15:00'):
        profitPercent, profitAmount = util.get_profit(buyOrderPrice, lastMktPrice, existingOrderQuantity)
        sellValue= abs(existingOrderQuantity) * float(lastMktPrice)                    
        update_bt_trade(cnx, mySQLCursor, sellValue, signalDate, lastMktPrice, profitAmount, profitPercent, tradeNo)
        
        noTradeFlag =True
        existingPositionFound = False
        existingOrderQuantity = 0
        patternListUpdatedFlag = True
    
    sellValue = 0
    exitSignalFlag =  False
    if (existingPositionFound):

        profitPercent, profitAmount = util.get_profit(buyOrderPrice, lastMktPrice, existingOrderQuantity)

        if (inputParams['exitStrategy'] == 'TTSL'):
            exitSignalFlag, slUpdateFlag, newSLPercent = get_trailing_tgt_trigger(float(profitPercent), float(tgtProfitPct), float(oldSlPct), trailingPct=80)
        
        elif(inputParams['exitStrategy'] == 'MTSL'):
            exitSignalFlag, slUpdateFlag, newSLPercent = get_mixed_trailing_sl(float(profitPercent), float(oldSlPct), float(tgtStopLossPct), float(tgtProfitPct), trailingPct=80)
        
        else:
            exitSignalFlag, slUpdateFlag, newSLPercent  = get_fixed_trailing_sl(float(profitPercent), float(oldSlPct), float(tgtStopLossPct))

        if (not(slUpdateFlag) or exitSignalFlag):
            newSLPercent = oldSlPct

        if (float(profitPercent) > float(existingProfitPercent)):
            existingProfitPercent = profitPercent
            existingProfitAmount = profitAmount

        if (float(profitPercent) < float(existingLossPercent)):
            existingLossPercent = profitPercent
            existingLossAmount = profitAmount

        sqlQuery = f"UPDATE BT_TRADES set CURR_SL_PCT= {str(newSLPercent)}, MAX_PROFIT_PERCENT = {str(existingProfitPercent)}, MAX_PROFIT_AMOUNT = {str(existingProfitAmount)}, MAX_LOSS_PERCENT = {str(existingLossPercent)}, MAX_LOSS_AMOUNT = {str(existingLossAmount)}, MARKET_PRICE= {str(lastMktPrice)}, PROFIT_AMOUNT ={str(profitAmount)}, PROFIT_PERCENT ={str(profitPercent)} WHERE TRADE_NO= {str(tradeNo)}"
        mySQLCursor.execute(sqlQuery)
        cnx.commit()

        sellValue= abs(existingOrderQuantity) * float(lastMktPrice) 
    
    # logging.info(f"EMASLength: {str(patternSignal)}")
    # logging.info(f"EMASLength: {str(reversalScoreDict['EMASLength'])}")
    # logging.info(f"stackedEMAScore: {str(reversalScoreDict['stackedEMAScore'])}")
    # logging.info(f"belowBuyPrice: {str(reversalScoreDict['belowBuyPrice'])}")
    # logging.info(f"stackedEMASellScore: {str(reversalScoreDict['stackedEMASellScore'])}")
    # logging.info(f"aboveSellPrice: {str(reversalScoreDict['aboveSellPrice'])}")
    # logging.info(f"bullishCandlePct: {str(reversalScoreDict['bullishCandlePct'])}")
    # logging.info(f"bearishCandlePct: {str(reversalScoreDict['bearishCandlePct'])}")
    
    reversalScoreDict['target_pct'] = inputParams['exitTgtPct']
    reversalScoreDict['stopLossPct'] = inputParams['exitSlPct']
    
    if (not(noTradeFlag)):
        entryConditionsMetFlag = meeting_entry_conditions(strategyId, patternSignal, reversalScoreDict)
     
        if (patternSignal == 'Buy' and not(existingPositionFound) and existingOrderQuantity == 0 and entryConditionsMetFlag):
            
            tradeNo = insert_bt_trade(cnx, mySQLCursor, reversalScoreDict, interval, overNightPosFlag, exitProfitPercent, signalDate, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, 'BUY', inputParams)
            # Mention that it is actual buy or sell in the pattern analysis data
            
            if (len(patternDataList) > 0):
                patternDataList.insert(len(patternDataList), str('BUY'))
                patternDataList.insert(len(patternDataList), str(profitPercent))
                patternDataList.insert(len(patternDataList), str(futBuyValue))
                patternDataList.insert(len(patternDataList), str(tradeNo))

                patternListUpdatedFlag = True
        
        # Short covering order 
        # elif (patternSignal == 'Buy' and existingPositionFound and existingOrderQuantity < 0 and (profitPercent > exitProfitPercent or profitPercent < (exitProfitPercent * -1))):              
   
        #     update_bt_trade(cnx, mySQLCursor, sellValue, signalDate, lastMktPrice, profitAmount, profitPercent, tradeNo)                    
        #     # insert_bt_trade(cnx, mySQLCursor, interval, overNightPosFlag, exitProfitPercent, signalDate, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, 'BUY')
        #     if (len(patternDataList) > 0):
        #         patternDataList.insert(len(patternDataList), str('EXIT BUY'))
        #         patternDataList.insert(len(patternDataList), str(profitPercent))
        #         patternDataList.insert(len(patternDataList), str(sellValue))
        #         patternDataList.insert(len(patternDataList), str(tradeNo))
            
        #         patternListUpdatedFlag = True

        elif (existingPositionFound and existingOrderQuantity < 0 and exitSignalFlag):
   
            update_bt_trade(cnx, mySQLCursor, sellValue, signalDate, lastMktPrice, profitAmount, profitPercent, tradeNo)                    
            # insert_bt_trade(cnx, mySQLCursor, interval, overNightPosFlag, exitProfitPercent, signalDate, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, 'BUY')
            if (len(patternDataList) > 0):
                patternDataList.insert(len(patternDataList), str('EXIT BUY'))
                patternDataList.insert(len(patternDataList), str(profitPercent))
                patternDataList.insert(len(patternDataList), str(sellValue))
                patternDataList.insert(len(patternDataList), str(tradeNo))
            
                patternListUpdatedFlag = True

        # New Sell order, having no existing orders            
        elif (patternSignal == 'Sell' and not(existingPositionFound) and existingOrderQuantity == 0 and entryConditionsMetFlag):
 
            tradeNo = insert_bt_trade(cnx, mySQLCursor, reversalScoreDict, interval, overNightPosFlag, exitProfitPercent, signalDate, lastMktPrice, futTradeSymbol, strategyId, int(quantity) * -1, futBuyValue, 'SELL', inputParams)
            if (len(patternDataList) > 0):
                patternDataList.insert(len(patternDataList), str('SELL'))
                patternDataList.insert(len(patternDataList), str(profitPercent))
                patternDataList.insert(len(patternDataList), str(futBuyValue))
                patternDataList.insert(len(patternDataList), str(tradeNo))
            
                patternListUpdatedFlag = True
            
        # Short covering order 
        # elif (existingPositionFound and existingOrderQuantity > 0 and (profitPercent <= tgtStopLossPct or profitPercent >= tgtProfitPct)):                                    
        elif (existingPositionFound and existingOrderQuantity > 0 and exitSignalFlag):                                    
            
            update_bt_trade(cnx, mySQLCursor, sellValue, signalDate, lastMktPrice, profitAmount, profitPercent, tradeNo)                    
            # insert_bt_trade(cnx, mySQLCursor, interval, overNightPosFlag, exitProfitPercent, signalDate, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, 'SELL')
            if (len(patternDataList) > 0):
                patternDataList.insert(len(patternDataList), str('EXIT SELL'))
                patternDataList.insert(len(patternDataList), str(profitPercent))
                patternDataList.insert(len(patternDataList), str(sellValue))
                patternDataList.insert(len(patternDataList), str(tradeNo))
                patternListUpdatedFlag = True
    #     # Short covering order 
    #     elif (existingPositionFound and existingOrderQuantity > 0):   
    #         # insert_bt_trade(cnx, mySQLCursor, interval, overNightPosFlag, exitProfitPercent, signalDate, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, 'SELL')
    #         if (len(patternDataList) > 0):
    #             patternDataList.insert(len(patternDataList), str('EXISTING BUY'))
    #             patternDataList.insert(len(patternDataList), str(profitPercent))
    #             patternDataList.insert(len(patternDataList), str(sellValue))
    #             patternDataList.insert(len(patternDataList), str(tradeNo))
        
    #     elif (existingPositionFound and existingOrderQuantity < 0):                                                                               
    #         # insert_bt_trade(cnx, mySQLCursor, interval, overNightPosFlag, exitProfitPercent, signalDate, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, 'SELL')
    #         if (len(patternDataList) > 0):
    #             patternDataList.insert(len(patternDataList), str('EXISTING SELL'))
    #             patternDataList.insert(len(patternDataList), str(profitPercent))
    #             patternDataList.insert(len(patternDataList), str(sellValue))
    #             patternDataList.insert(len(patternDataList), str(tradeNo))
    #     else:                                                 
    #         # insert_bt_trade(cnx, mySQLCursor, interval, overNightPosFlag, exitProfitPercent, signalDate, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, 'SELL')
    #         if (len(patternDataList) > 0):
    #             patternDataList.insert(len(patternDataList), str(''))
    #             patternDataList.insert(len(patternDataList), str(0))
    #             patternDataList.insert(len(patternDataList), str(0))
    #             patternDataList.insert(len(patternDataList), str(0))
    # else:
    #     # insert_bt_trade(cnx, mySQLCursor, interval, overNightPosFlag, exitProfitPercent, signalDate, lastMktPrice, futTradeSymbol, strategyId, quantity, futBuyValue, 'SELL')
    #     if (len(patternDataList) > 0):
    #         patternDataList.insert(len(patternDataList), str(''))
    #         patternDataList.insert(len(patternDataList), str(0))
    #         patternDataList.insert(len(patternDataList), str(0))
    #         patternDataList.insert(len(patternDataList), str(0))
    
    if (patternListUpdatedFlag):
        return patternDataList
    else:
        patternDataList = []
        return patternDataList



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
            exitProfitPercent = 0
            overNightPosFlag = False
            reportDate = util.get_system_date()

            if (strategyId == 'ALPHA_1'):      
                interval = '2hour'
                confirmInterval1 = '3hour'
                confirmInterval2 = 'day'
                exitProfitPercent =  1

            elif (strategyId == 'ALPHA_2'):      
                interval = '30minute'
                confirmInterval1 = '60minute'
                confirmInterval2 = '2hour'
                exitProfitPercent =  0.3

            elif (strategyId == 'ALPHA_9'):      
                interval = '15minute'
                confirmInterval1 = '30minute'
                exitProfitPercent =  0.01
                # if (int(currentTime) > int(sysSettings['PBE_CUT_OFF_START_TIME'])):
                #     cutOffStartFlag = True

            elif (strategyId == 'ALPHA_10'):      
                interval = '30minute'
                exitProfitPercent =  0.3
                confirmInterval1 = '60minute'
                confirmInterval2 = '2hour'
              
            else:     
                interval = '30minute'
                exitProfitPercent =  0.3
                confirmInterval1 = '60minute'
                confirmInterval2 = '2hour'

            instList = util.get_all_fno_inst_list(mySQLCursor, limitCnt=20)
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

            inputParamQuery = f"SELECT BT_VERSION, ENTRY_CONDITIONS, REVERSAL_BW_VALUE, EXIT_STRATEGY, EXIT_STOP_LOSS_PCT, EXIT_TARGET_PCT, \
                                RERUN_FLAG, STRATEGY_NAME FROM BT_INPUT_PARAMETERS WHERE RERUN_FLAG='YES'" 

            mySQLCursor.execute(inputParamQuery)
            btInputParams = mySQLCursor.fetchall()            

            for btInput in btInputParams: 
                inputParams = {}
                inputParams['btVersion'] = btInput[0]
                inputParams['entryCondtion'] = btInput[1]
                inputParams['revBWValue'] = btInput[2]
                inputParams['exitStrategy'] = btInput[3]
                inputParams['exitSlPct'] = btInput[4]
                inputParams['exitTgtPct'] = btInput[5]
                inputParams['reRunFlag'] = btInput[6]
                
                strategyId = btInput[7]
               
                if (inputParams['reRunFlag'] == 'YES'):
                    sqlQuery = f"DELETE FROM BT_TRADES WHERE STRATEGY_VERSION = '{str(inputParams['btVersion'])}'"
                    mySQLCursor.execute(sqlQuery)
                    # commit records
                    cnx.commit()

                for instRow in instListLocal:                
                    startTime=datetime.datetime.now()
                    instrumentToken = instRow['instrumentToken']
                    tradeSymbol = instRow['tradeSymbol']                            
                    stockName = instRow['stockName']      
                    inputParams['lotSize'] = instRow['lotSize']
                    inputParams['expDate'] = instRow['expDate']          
                    
                    logging.info(f"Starting with {stockName}")

                    # histRecords = baf.get_historical_data(kite, instrumentToken, fromDate, toDate, interval)
                    # df = pd.DataFrame(histRecords)
                    # df.date = df.date + pd.Timedelta('05:30:00')
                    df = instRow['df']
                    dfStart = 200
                    dfEnd = 461
                    dfLoopCnt = 0
                    patternDataList = []
                    insertPaternAnalysis = []
                    while dfEnd < df.shape[0]:                                       
                        dfTemp = df.iloc[dfStart:dfEnd]
                        patternDataList  = patterns_based_entry_check(cnx, mySQLCursor, dfTemp, overNightPosFlag, tradeSymbol, stockName, strategyId, interval, exitProfitPercent, inputParams)
                        
                        if (len(patternDataList) > 0): 
                            insertPaternAnalysis.insert(len(insertPaternAnalysis), patternDataList)

                        dfStart += 1
                        dfEnd += 1
                        
                                        
                    # Added the collected pattern analysis data into database;
                   
                    insert_pattern_analysis(cnx, mySQLCursor, configDict, insertPaternAnalysis, inputParams['btVersion'])
                    
                    endTime = datetime.datetime.now()                    

                    logging.info(f"Finished {stockName} in {endTime - startTime}")
                    logging.info(f"---------------------")

        except Exception as e:
            alertMsg = "Live trade service failed (main block): " + str(e)
            logging.info(alertMsg)

        
        # util.update_program_running_status(cnx, mySQLCursor,programN---`ame, 'ACTIVE')
        util.disconnect_db(cnx, mySQLCursor)


    util.logger(logEnableFlag, "info", "Program ended")
