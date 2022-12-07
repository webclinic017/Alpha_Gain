import datetime

from requests.models import Response
from utils import util_functions as util
from utils import broker_api_functions as baf
import mibian
import time
from utils import trade_scoring as ts

def insert_options_data(cnx, mySQLCursor, cashSecuredPutDataList):
    try:                 
        insertQuery = "REPLACE INTO CASH_SECURED_PUT_WATCHLIST (TRADE_SYMBOL, CMP_STOCK, LOT_SIZE, NOTIONAL_VALUE, MARGIN_REQUIRED, OPTION_INSTRUMENT, \
                        EXPIRY_DATE, PUT_OPTION_STRIKE, PUT_OPTION_PREMIUM, OPTION_VALUE, PREMIUM_PCT, \
                        DELTA, PROBABILITY_PROFIT, BREAK_EVEN_PRICE, PRICE_PROTECTION, IMPLIED_VOLATILITY, VOLUME, DAYS_TO_EXPIRY, OPEN_INTEREST, REPORT_DATE, OPTION_INSTRUMENT_TOKEN, OPTION_TYPE, UPDATED_ON, BASE_INSTRUMENT_TOKEN) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)"

        mySQLCursor.execute(insertQuery, cashSecuredPutDataList)
        cnx.commit()
    except Exception as e:
        print (e)



def bsm_call_options_pricing(cnx, mySQLCursor, kite, stockClosePrice, tradeSymbol, baseInstToken):
    try:
        expiryMonth = util.get_fno_expiry_month() 
    
        instSearchString = str(tradeSymbol) + str(expiryMonth)        

        selectStatment = "SELECT instrument_token, strike, expiry,tradingsymbol,tick_size,lot_size FROM INSTRUMENTS WHERE tradingsymbol LIKE '" + \
            instSearchString+"%' AND exchange='NFO' AND instrument_type='CE' AND strike >= " + str(stockClosePrice) +" ORDER BY strike LIMIT 5"

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

            
            updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
            reportDate = util.get_date_time_formatted("%Y-%m-%d")
            dataList = []
            dataList.insert(0, str(tradeSymbol))      
            dataList.insert(1, str(stockClosePrice))
            dataList.insert(2, str(fnoLotSize))
            dataList.insert(3, str(cashValue))
            dataList.insert(4, str(marginRequired))
            dataList.insert(5, str(selectedTradingSymbol))
            dataList.insert(6, str(lDate))
            dataList.insert(7, str(selectedStrikePrice))
            dataList.insert(8, str(fnoBuyPrice))
            dataList.insert(9, str(fnoBuyValue))
            dataList.insert(10, str(premiumValuePct))
            dataList.insert(11, str(selectedDelta))
            dataList.insert(12, str(probabilityProfit))
            dataList.insert(13, str(breakEvenPrice))
            dataList.insert(14, str(priceProtection))
            dataList.insert(15, str(selectedImpliedVol))
            dataList.insert(16, str(selectedVolume))
            dataList.insert(17, str(daysToExpiry.days))
            dataList.insert(18, str(selectedOpenIntrest))
            dataList.insert(19, str(reportDate))
            dataList.insert(20, str(selectedInsturmentToken))
            dataList.insert(21, str('CALL'))            
            dataList.insert(22, str(updatedOn))
            dataList.insert(23, str(baseInstToken))
            

            insert_options_data(cnx, mySQLCursor, dataList)

            response = {}   
            response['status'] = 'success'
            response['remarks'] = 'success'              
            response['selectedInsturmentToken'] = selectedInsturmentToken            
            response['selectedTradingSymbol'] = selectedTradingSymbol
            response['fnoBuyPrice'] = fnoBuyPrice
            response['fnoLotSize'] = fnoLotSize
            response['selectedLastPrice'] = selectedLastPrice
            response['premiumValuePct'] = premiumValuePct                        


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

def bsm_put_options_pricing(cnx, mySQLCursor, kite, stockClosePrice, tradeSymbol, baseInstToken):
    try:
        expiryMonth = util.get_fno_expiry_month()    
        instSearchString = str(tradeSymbol) + str(expiryMonth)        

        selectStatment = "SELECT instrument_token, strike, expiry, tradingsymbol, tick_size, lot_size FROM INSTRUMENTS WHERE tradingsymbol LIKE '" + \
            instSearchString+"%' AND exchange='NFO' AND instrument_type='PE' AND strike <= " + str(stockClosePrice) +" ORDER BY strike DESC LIMIT 5"

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
            selectedBuyPrice = fnoBuyPriceList[closestStrikeIndex]
            selectedSellPrice = fnoSellPriceList[closestStrikeIndex]
            selectedLastPrice = fnoLastPriceList[closestStrikeIndex]
            selectedVega = vegaList[closestStrikeIndex]
            selectedTradingSymbol =  fnoTradingSymbolList[closestStrikeIndex]
            selectedInsturmentToken = fnoInstrumentTokenList[closestStrikeIndex]
            spreadBuySell = float(selectedSellPrice) - float(selectedBuyPrice)

            fnoBuyPrice = float(selectedSellPrice) + float(fnoTickSize)
            fnoBuyValue = fnoBuyPrice * fnoLotSize
            
            cashValue = fnoLotSize * float(selectedStrikePrice)
            marginRequired = cashValue * 0.25
            premiumValuePct = (fnoBuyValue / cashValue ) * 100
            breakEvenPrice = float(selectedStrikePrice) - float(selectedSellPrice)            
            priceProtection =  ((breakEvenPrice / float(stockClosePrice)) - 1) * 100
            probabilityProfit = 1 + float(selectedDelta)

            
            updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
            reportDate = util.get_date_time_formatted("%Y-%m-%d")
            dataList = []
            dataList.insert(0, str(tradeSymbol))      
            dataList.insert(1, str(stockClosePrice))
            dataList.insert(2, str(fnoLotSize))
            dataList.insert(3, str(cashValue))
            dataList.insert(4, str(marginRequired))
            dataList.insert(5, str(selectedTradingSymbol))
            dataList.insert(6, str(lDate))
            dataList.insert(7, str(selectedStrikePrice))
            dataList.insert(8, str(fnoBuyPrice))
            dataList.insert(9, str(fnoBuyValue))
            dataList.insert(10, str(premiumValuePct))
            dataList.insert(11, str(selectedDelta))
            dataList.insert(12, str(probabilityProfit))
            dataList.insert(13, str(breakEvenPrice))
            dataList.insert(14, str(priceProtection))
            dataList.insert(15, str(selectedImpliedVol))
            dataList.insert(16, str(selectedVolume))
            dataList.insert(17, str(daysToExpiry.days))
            dataList.insert(18, str(selectedOpenIntrest))
            dataList.insert(19, str(reportDate))
            dataList.insert(20, str(selectedInsturmentToken))
            dataList.insert(21, str('PUT'))            
            dataList.insert(22, str(updatedOn))
            dataList.insert(23, str(baseInstToken))

            insert_options_data(cnx, mySQLCursor, dataList)
            response = {}   
            response['status'] = 'success'
            response['remarks'] = 'success'              
            response['selectedInsturmentToken'] = selectedInsturmentToken            
            response['selectedTradingSymbol'] = selectedTradingSymbol
            response['fnoBuyPrice'] = fnoBuyPrice
            response['fnoLotSize'] = fnoLotSize
            response['selectedLastPrice'] = selectedLastPrice
            response['premiumValuePct'] = premiumValuePct                        
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


def add_many_records_into_db(cnx, mySQLCursor, configDict, query, arrayVals):
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

    mySQLCursor.executemany(query, arrayVals)
    cnx.commit()


def get_market_updates(cnx, mySQLCursor, kite, configDict, niftyHistRecords, category, toDate):
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

    updateQuery = "UPDATE MARKET_PERFORMANCE_TBL SET UPDATED_ON = %s, SMA3_KST_RATIO = %s, KST = %s, KST_NIFTY= %s, KST_RATIO= %s, \
                    SMA9_KST_RATIO= %s, ROC1= %s, VOLUME= %s, CLOSE_PRICE= %s, HIGH_252_FLAG= %s, LOW_252_FLAG= %s, BETA= %s, PRICE_MOMEMTUM_26W= %s, OBV_MOMEMTUM_26W= %s,\
                    OBVM_PMM_RATIO= %s, TTM_SQUEEZE= %s, TTMS_LENGTH= %s, UP_IN_3_DAYS= %s, DOWN_IN_3_DAYS= %s, UD_RATIO_22_DAYS= %s, SMA_3_UDR= %s, SMA_9_UDR= %s, \
                    UD_VOLUME_RATIO= %s, SMA_3_UDVR= %s, SMA_9_UDVR= %s, BUY_SELL_NET_SCORE= %s, BUY_SCORE= %s, \
                    SELL_SCORE= %s WHERE DATE=%s  AND INSTRUMENT_TOKEN=%s"

    
    # Fetch list of all instruments from stock universe for the provided category
    selectStatment = "SELECT INSTRUMENT_TOKEN, TRADINGSYMBOL, BENCHMARK_INDEX, MARKET_CAP, SECTOR, INDUSTRY, FNO, NAME FROM \
                        STOCK_UNIVERSE WHERE CATEGORY='"+category+"' ORDER BY TRADINGSYMBOL"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()   
    
    insertArrayValues = []
    tmpCnt = 0
    lastRecord = ''
    histRecords = ''
    for row in results:
        instrumentToken = row[0]
        tradeSymbol = row[1]        
        FNO = row[6]
        STOCK_NAME = row[7]
        print("STOCK_NAME: "+ str(STOCK_NAME))
        try:
            # Get historical data of given instrument token
            histRecords = baf.get_historical_data(kite, instrumentToken, fromDate, toDate, interval, broker=None)     
              
            df, indSuccssFlag = util.get_indicators(histRecords, niftyHistRecords, STOCK_NAME,category=category)
          
            if (indSuccssFlag):          
                lastRecord = df.tail(1)
                if (FNO == 'FNO'):
                    closePrice = lastRecord['close'].values[0] 
                    
                    response = bsm_put_options_pricing(cnx, mySQLCursor, kite, closePrice, tradeSymbol, instrumentToken)    
                    if (response['status'] == 'error'): 
                        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', response['remarks'], telgUpdateFlag='N', programName=programName)

                    response = bsm_call_options_pricing(cnx, mySQLCursor, kite, closePrice, tradeSymbol, instrumentToken)    
                    if (response['status'] == 'error'): 
                        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', response['remarks'], telgUpdateFlag='N', programName=programName)

                buySellScore, totalBuyScore, totalSellScore = ts.getBuySellScore(df)

                SMA3_KST_RATIO = lastRecord['SMA3_KST_RATIO'].values[0]
                SMA9_KST_RATIO = lastRecord['SMA9_KST_RATIO'].values[0]
                ROC1 = lastRecord['ROC1'].values[0]
                KST = lastRecord['KST'].values[0]
                KST_NIFTY = lastRecord['KST_NIFTY'].values[0]
                KST_RATIO = lastRecord['KST_RATIO'].values[0]
                
                CLOSE_PRICE = lastRecord['close'].values[0]
                HIGH252 = lastRecord['HIGH200'].values[0]
                LOW252 = lastRecord['LOW200'].values[0]
                BETA = lastRecord['BETA'].values[0]
                high252Flag = 'N'
                low252Flag = 'N'

                if (HIGH252 is None or str(HIGH252).upper == 'NAN'):
                    high252Flag = 'NA'
                elif (CLOSE_PRICE > HIGH252):
                    high252Flag = 'Y'

                if (LOW252 is None or str(LOW252).upper == 'NAN'):
                    low252Flag = 'NA'           
                elif (CLOSE_PRICE < LOW252):
                    low252Flag = 'Y'


                if (category == "Market Cap" or category == "Sector"):
                    VOLUME = 0
                    OBV_MOMEMTUM_26W = 0
                    OBVM_PMM_RATIO = 0
                    UD_VOLUME_RATIO = 0
                    SMA_3_UDVR = 0
                    SMA_9_UDVR = 0
                else:
                    VOLUME = lastRecord['volume'].values[0]
                    OBV_MOMEMTUM_26W = lastRecord['OBV_Momentum_26w'].values[0]
                    OBVM_PMM_RATIO = lastRecord['OBVMM/PMM'].values[0]
                    UD_VOLUME_RATIO = lastRecord['UD_Volume_Ratio'].values[0]
                    SMA_3_UDVR = lastRecord['SMA3_UD_Volume_Ratio'].values[0]
                    SMA_9_UDVR = lastRecord['SMA9_UD_Volume_Ratio'].values[0]
                
                PRICE_MOMEMTUM_26W = lastRecord['Price_Momentum_26w'].values[0]
                TTM_SQUEEZE = lastRecord['TTMSqueeze'].values[0]
                TTMS_LENGTH = lastRecord['TTMSLength'].values[0]
                UP_IN_3_DAYS = lastRecord['UP_IN_3_DAYS'].values[0]
                DOWN_IN_3_DAYS = lastRecord['DOWN_IN_3_DAYS'].values[0]
                UD_RATIO_22_DAYS = lastRecord['UD_Days_Ratio_22'].values[0]
                SMA_3_UDR = lastRecord['SMA3_UD_Days_Ratio'].values[0]
                SMA_9_UDR = lastRecord['SMA9_UD_Days_Ratio'].values[0]
                BUY_SELL_NET_SCORE = buySellScore
                BUY_SCORE = totalBuyScore
                SELL_SCORE = totalSellScore
              
                updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")

                insertVal = []  
                insertVal.insert(0, str(updatedOn))              
                insertVal.insert(1, str(SMA3_KST_RATIO))
                insertVal.insert(2, str(KST))
                insertVal.insert(3, str(KST_NIFTY))
                insertVal.insert(4, str(KST_RATIO))               
                insertVal.insert(5, str(SMA9_KST_RATIO))
                insertVal.insert(6, str(ROC1))
                insertVal.insert(7, str(VOLUME))
                insertVal.insert(8, str(CLOSE_PRICE))            
                insertVal.insert(9, str(high252Flag))            
                insertVal.insert(10, str(low252Flag))            
                insertVal.insert(11, str(BETA)) 
                insertVal.insert(12, str(PRICE_MOMEMTUM_26W))
                insertVal.insert(13, str(OBV_MOMEMTUM_26W))
                insertVal.insert(14, str(OBVM_PMM_RATIO))
                insertVal.insert(15, str(TTM_SQUEEZE))
                insertVal.insert(16, str(TTMS_LENGTH))
                insertVal.insert(17, str(UP_IN_3_DAYS))
                insertVal.insert(18, str(DOWN_IN_3_DAYS))
                insertVal.insert(19, str(UD_RATIO_22_DAYS))
                insertVal.insert(20, str(SMA_3_UDR))
                insertVal.insert(21, str(SMA_9_UDR))
                insertVal.insert(22, str(UD_VOLUME_RATIO))
                insertVal.insert(23, str(SMA_3_UDVR))
                insertVal.insert(24, str(SMA_9_UDVR))
                insertVal.insert(25, str(BUY_SELL_NET_SCORE))
                insertVal.insert(26, str(BUY_SCORE))
                insertVal.insert(27, str(SELL_SCORE))                
                insertVal.insert(28, str(toDate))
                insertVal.insert(29, str(instrumentToken))

                insertArrayValues.insert(tmpCnt, insertVal)
                tmpCnt += 1

                if (tmpCnt == 50):                                                
                    add_many_records_into_db(cnx, mySQLCursor, configDict, updateQuery, insertArrayValues)                                                         
                    insertArrayValues = []
                    tmpCnt = 0   
                                     
        except Exception as e:    
            print("Errored while updating the market updates: " + str(e))
            pass
    if (tmpCnt > 0):               
        # insertMarketPerformance(cnx, mySQLCursor,insertVal)
        add_many_records_into_db(cnx, mySQLCursor, configDict, updateQuery, insertArrayValues)


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

    programName = configDict['MARKET_UPD_SVC_PGM_NAME']
    # Initialized the log files 
    util.initialize_logs(str(configDict['MARKET_UPD_SVC_PGM_NAME']) + '.log')

    programExitFlag = 'N'    

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)
    
    adminTradeAccount = configDict['ADMIN_TRADE_ACCOUNT']
    adminTradeBroker = configDict['ADMIN_TRADE_BROKER'] 

    # Connect to Kite ST
    apiConnObj, isAPIConnected  = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = adminTradeBroker)    
    
    # Dates between which we need historical data
    fromDate = util.get_lookup_date(360)    
    toDate = util.get_lookup_date(0)
    interval = "day"

    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if (isAPIConnected):           
        
        alertMsg = 'The program (' + programName.replace('_','\_') + ')  started at ' + str(util.get_date_time_formatted("%d-%m-%Y %H:%M:%S"))
        
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)
        
        # Continuously run the program until the exit flag turns to Y
        while programExitFlag != 'Y': 
            try:

                cycleStartTime = util.get_system_time()                 
                
                # Verify whether the connection to MySQL database is open
                cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
                
                sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
                currentTime = util.get_date_time_formatted("%H%M")
                if (testingFlag or (int(currentTime) <= int(sysSettings['SYSTEM_END_TIME']))):      
                    day = (datetime.datetime.strptime(toDate, '%Y-%m-%d')).strftime("%A")
                    # Update performance data for Market Cap, Sector, and Stocks
                    if (day != 'Saturday' and day != 'Sunday'):
                        try:
                            # # Get historical data of given instrument token; the number 268041 is for nifty500 instrument            
                            niftyHistRecords = baf.get_historical_data(apiConnObj, '268041', fromDate, toDate, interval, broker=adminTradeBroker)
                            # startTime = time.time()                   
                            # get_market_updates(cnx, mySQLCursor, apiConnObj, configDict, niftyHistRecords, "Market Cap", toDate)                    
                            # endTime = time.time()

                            # alertMsg="Market updates have been completed for Market Cap"                     
                            # util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    
                            
                            # startTime = time.time()                   
                            # get_market_updates(cnx, mySQLCursor, apiConnObj, configDict, niftyHistRecords, "Sector",toDate)    
                            # endTime = time.time()

                            # alertMsg="Market updates have been completed for Sector" 
                            # util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    

                            startTime = time.time()                   
                            get_market_updates(cnx, mySQLCursor, apiConnObj, configDict, niftyHistRecords, "Stocks",toDate)                    
                            endTime = time.time()


                            alertMsg="Market updates have been completed for Stocks" 
                            util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    
                            
                        except Exception as e:
                            alertMsg="Unable to complete the market updates" + str(e) 
                            util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)                            

                else:
                    alertMsg = 'System end time reached; exiting the program now'                  
                    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)    
                    programExitFlag = 'Y'

                util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
                util.disconnect_db(cnx, mySQLCursor)
                
                cycleEndTime = util.get_system_time()
                cycleDiff = cycleEndTime - cycleStartTime  
                util.logger(logEnableFlag, 'info', "Cycle ended in " + str(cycleDiff) + " minutes") 
            
            except Exception as e:
                alertMsg = 'Market updates and BSM PUT Options serivce failed (main block): '+ str(e)
                util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='N', programName=programName)
            
    else:

        alertMsg = 'Unable to connect admin trade account from signal service. The singal records will not be updated for today until the issue is fixed.'
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='Y', programName=programName)    

    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

    util.update_program_running_status(cnx, mySQLCursor,programName, 'INACTIVE')
    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', 'Program ended', telgUpdateFlag='N', programName=programName)    
    util.disconnect_db(cnx, mySQLCursor)