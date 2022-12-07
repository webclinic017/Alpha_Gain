# def check_expiring_contracts(mySQLCursor, kite):
#     currDate = util.get_date_time_formatted('%Y-%m-%d')
#     selectStatment = f"SELECT TT.AUTO_ID, TT.TRADE_ACCOUNT, TT.BROKER, TT.TRADE_SYMBOL, TT.INSTRUMENT_TOKEN, TT.STRATEGY_ID,  \
#         TT.QUANTITY, TT.TRADE_DIRECTION, TT.EXPIRY_DATE, TT.UAT_FLAG FROM TRADE_TRANSACTIONS TT WHERE TT.EXPIRY_DATE='{currDate}' AND (TT.TRADE_STATUS ='OPEN' AND (TT.EXIT_SIGNAL_STATUS IS NULL OR TT.EXIT_SIGNAL_STATUS = ''))"

#     mySQLCursor.execute(selectStatment)
#     rowCount = mySQLCursor.rowcount

#     if (rowCount > 0):
#         results = mySQLCursor.fetchall()
        
#         for row in results:
#             tradeDataDict = {}
#             tradeDataDict['autoId'] = row[0]
#             tradeDataDict['accountId'] = row[1]            
#             tradeDataDict['broker'] = row[2]                        
#             tradeDataDict['futTradeSymbol'] = row[3]
#             tradeDataDict['futInstToken'] = row[4]
#             tradeDataDict['strategyId'] = row[5]            
#             tradeDataDict['quantity'] = row[6]
#             tradeDataDict['tradeDirection'] = row[7]            
#             tradeDataDict['expiryDate'] = row[8]
#             tradeDataDict['futOrderType']  = 'LMT'
#             tradeDataDict['uatFlag']  = True if row[9] == 'Y' else False

#             if (str(tradeDataDict['tradeDirection']).upper() == 'BUY'):        
#                 tradeDataDict['futTransactionType'] = "SELL"
#             elif (str(tradeDataDict['tradeDirection']).upper() == 'SELL'):        
#                 tradeDataDict['futTransactionType'] = "BUY"        
          
            
#             brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'])
#             place_fno_exit_order(cnx, mySQLCursor, kite, tradeDataDict, brokerAPI)
    
#     return rowCount

#     # Exit plan for close follow, and guranteed 3% up or down
# def get_fixed_trailing_sl(profitPercent, initialSLPercent, oldSlPercent):
#     exitSignalFlag = False
#     slUpdateFlag = False
#     newSLPercent = 0

#     if (profitPercent < oldSlPercent):
#         # Stop loss hit, send exit signal
#         exitSignalFlag = True

#     elif (profitPercent > 0):
#         newSLPercent = initialSLPercent + profitPercent
#         if( oldSlPercent < newSLPercent ):            
#             slUpdateFlag = True

#     return exitSignalFlag, slUpdateFlag, newSLPercent

# def patterns_based_exit(cnx, mySQLCursor, tradeDataDict, brokerApi):    
    
#     try:
#         if ((tradeDataDict['exitBuyCondition'] != None and tradeDataDict['exitBuyCondition'] != '') or (tradeDataDict['exitSellCondition'] != None and tradeDataDict['exitSellCondition'] != '')):

#             selectStatment = f"SELECT BASE_INSTRUMENT_TOKEN, BASE_TRADE_SYMBOL, STOCK_NAME, TRADE_SYMBOL, INSTRUMENT_TOKEN, QUANTITY, INSTRUMENT_TYPE, AUTO_ID, TRADE_DIRECTION, EXIT_STRATEGY_ID, BUY_ORDER_PRICE FROM TRADE_TRANSACTIONS \
#                 WHERE STRATEGY_ID='{tradeDataDict['strategyId']}' AND TRADE_ACCOUNT='{tradeDataDict['accountId']}' AND (TRADE_STATUS IN ('OPEN') AND (EXIT_SIGNAL_STATUS IS NULL OR EXIT_SIGNAL_STATUS = '') AND (SELL_ORDER_STATUS IS NULL OR SELL_ORDER_STATUS NOT IN ('PENDING', 'COMPLETE')))"

#             mySQLCursor.execute(selectStatment)
#             rowCount = mySQLCursor.rowcount

#             if (rowCount > 0):
#                 results = mySQLCursor.fetchall()

#                 for instRow in results:
#                     tradeDataDict['instrumentToken']  = instRow[0]
#                     tradeDataDict['tradeSymbol']  = instRow[1]                            
#                     tradeDataDict['stockName'] = instRow[2]
#                     tradeDataDict['futTradeSymbol'] = instRow[3]
#                     tradeDataDict['futInstToken'] = instRow[4]        
#                     tradeDataDict['quantity'] = instRow[5]
#                     tradeDataDict['existingOptionsInstType'] = instRow[6]
#                     tradeDataDict['autoId'] = instRow[7]
#                     tradeDataDict['tradeDirection'] = instRow[8]
#                     tradeDataDict['exitStrategy'] = instRow[9]
#                     tradeDataDict['entryPrice'] = instRow[10]

#                     exitLongFlag, exitShortFlag = is_technical_exit_met(cnx, mySQLCursor, brokerApi, tradeDataDict)

#                     if (exitLongFlag):                            
#                         tradeDataDict['futTransactionType'] = "SELL"                    
#                         place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)

#                     elif (exitShortFlag):                                
#                         tradeDataDict['futTransactionType'] = "BUY"
#                         place_fno_exit_order(cnx, mySQLCursor, brokerApi, tradeDataDict)
    
#     except Exception as e:    
#         alertMsg = f"Exceptions occured patterns_based_exit: {str(e)}"
#         util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, tradeDataDict)       

# def check_rollover_of_expired_contracts(mySQLCursor, brokerApi):
#     currDate = util.get_date_time_formatted('%Y-%m-%d')
#     selectStatment = f"SELECT STRATEGY_ID, TRADE_ACCOUNT, BROKER, EXCHANGE, BASE_INSTRUMENT_TOKEN, BASE_TRADE_SYMBOL, QUANTITY, \
#                         TGT_PROFIT_PCT, TGT_STOP_LOSS_PCT, EXIT_STRATEGY_ID, TRAILING_THRESHOLD_PCT, INSTRUMENT_TYPE, \
# 						TRADE_DIRECTION, OPTION_STRIKE_PRICE, UAT_FLAG, EXPIRY_DATE, POSITION_DIRECTION, AUTO_ID, CONTRACT_TYPE FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS ='EXITED' AND DATE(EXPIRY_DATE) = '{currDate}' AND ROLL_OVER_FLAG = 'Y' AND ROLL_OVER_COMPLETED_FLAG = 'N'"

#     mySQLCursor.execute(selectStatment)
#     rowCount = mySQLCursor.rowcount

#     if (rowCount > 0):
#         results = mySQLCursor.fetchall()
        
#         for row in results:
#             tradeDataDict = {}            
#             tradeDataDict['strategyId'] =  row[0]
#             tradeDataDict['accountId'] = row[1]
#             tradeDataDict['broker'] = row[2]
#             tradeDataDict['exchange'] = row[3]
#             tradeDataDict['instrumentToken'] = row[4]
#             tradeDataDict['baseTradeSymbol'] = row[5]
#             tradeDataDict['quantity'] = row[6]
#             tradeDataDict['tgtProfitPct'] =  row[7]                      
#             tradeDataDict['tgtStopLossPct'] = row[8]
#             tradeDataDict['exitStrategyId'] = row[9]	
#             tradeDataDict['trailingThresholdPct'] = row[10]	
#             tradeDataDict['instrumentType'] = row[11]
#             tradeDataDict['tradeDirection'] = row[12]
#             tradeDataDict['futTransactionType'] = row[12]
#             tradeDataDict['strikePrice'] = row[13]
#             tradeDataDict['uatFlag'] = True if row[14] == 'Y' else False
#             tradeDataDict['rawExpiry'] = row[15]
#             tradeDataDict['positionDirection'] = row[16]
#             autoId= row[17]
#             tradeDataDict['contractType']  = row[18]
            
#             tradeDataDict['programName']=programName
            
#             tradeDataDict['signalDate'] = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
#             tradeDataDict['futOrderType']  = 'LMT'
#             tradeDataDict['orderStatus']  = 'PENDING'
#             tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')

#             if (tradeDataDict['instrumentType'] == 'FUT'):
#                 futInstName, futInstToken, lotSize, expDate, rawExpiry = util.get_futures_instruments(mySQLCursor, tradeDataDict['baseTradeSymbol'])
#                 tradeDataDict['futTradeSymbol']= tradeDataDict['baseTradeSymbol']  + expDate + 'F'
#                 tradeDataDict['futInstToken'] = futInstToken
#                 tradeDataDict['rawExpiry'] = rawExpiry
#             else:
#                 if (tradeDataDict['contractType'] == 'WEEKLY'):
#                     futInstName, futInstToken, lotSize, expDate, strikePrice, rawExpiry  = util.get_rollover_options_instruments(mySQLCursor, tradeDataDict['baseTradeSymbol'], tradeDataDict['instrumentType'], tradeDataDict['strikePrice'])            
#                     if (tradeDataDict['instrumentType'] == 'PUT'):
#                         tradeDataDict['futTradeSymbol'] = tradeDataDict['baseTradeSymbol'] + expDate + 'P' + str(strikePrice).replace('.0','')            
#                     else:
#                         tradeDataDict['futTradeSymbol'] = tradeDataDict['baseTradeSymbol'] + expDate + 'C' + str(strikePrice).replace('.0','')

#                     tradeDataDict['futInstToken'] = futInstToken
#                     tradeDataDict['rawExpiry'] = rawExpiry
                        
            
            
#             orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
            
#             if (orderSuccessFlag):
#                 updateQuery = (f"UPDATE TRADE_TRANSACTIONS SET ROLL_OVER_COMPLETED_FLAG='Y' WHERE AUTO_ID = '{autoId}'")
#                 mySQLCursor.execute(updateQuery)
#                 cnx.commit()
           
#     return rowCount

# def get_options_price_change (kite, instToken, entryPrice, quantity, maxProfitPercent, maxProfitAmount, prevClosePrice):
#     getLTPData =  baf.get_ltp(kite, instToken)
#     ltpPrice = getLTPData[instToken]['last_price']
#     profitPercent, profitAmount = util.get_profit(float(entryPrice), float(ltpPrice), quantity)

#     if (float(profitPercent) > float(maxProfitPercent)):
#         maxProfitPercent = profitPercent
#         maxProfitAmount = profitAmount

#     if (float(prevClosePrice) == 0):
#         dayChageprofitPercent, dayChangeprofitAmount = util.get_profit(float(entryPrice), float(ltpPrice), quantity)
#     else:
#         dayChageprofitPercent, dayChangeprofitAmount = util.get_profit(float(prevClosePrice), float(ltpPrice), quantity)

#     return profitPercent, profitAmount, maxProfitPercent, maxProfitAmount, dayChageprofitPercent, dayChangeprofitAmount, ltpPrice

    
# # def get_pattern_analysis (cnx, mySQLCursor, kite):
# #     fromDate = util.get_lookup_date(400)
# #     toDate = util.get_lookup_date(0)
# #     instList = util.get_test_inst_list(mySQLCursor)
# #     for instRow in instList:
# #         instrumentToken = instRow[0]
# #         tradeSymbol = instRow[1]                            
# #         stockName = instRow[2]       
# #         print("Checking Insturment: " + stockName)        
# #         # patternSignal, lastMktPrice, bullishReversalScore = get_patterns_signal(kite, instrumentToken, fromDate, toDate, 'day', stockName, tradeSymbol)

# def is_account_under_limit (mySQLCursor, tradeDataDict):
#     strategyId = tradeDataDict['strategyId']
#     accountId = tradeDataDict['accountId']                                        
#     availableMargin = float(tradeDataDict['availableMargin'])
#     capitalAllocation = float(tradeDataDict['capitalAllocation'])    
    
#     utilizeCaptial = util.get_utilized_capital(mySQLCursor, accountId, strategyId=strategyId)

#     isUtilzedCapitalLimited =  (utilizeCaptial <= capitalAllocation)

#     limitExeededFlag = False

#     if (isUtilzedCapitalLimited):
#         futBuyValue = tradeDataDict['futBuyValue']
#         isMarginAvailable = (availableMargin >= (futBuyValue * 0.25))

#         if (isMarginAvailable):  
#             limitExeededFlag = True
                    
#     return limitExeededFlag
# def add_buy_hedging(cnx, mySQLCursor, kite, tradeDataDict, instrumentType, brokerAPI):
#     response = {}
#     try:
#         tradeDataDict['futTransactionType'] = 'BUY'
#         tradeDataDict['instrumentType'] = instrumentType
#         tradeDataDict['isItHedgePos'] = 'Y'
#         tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'], hedgingFlag=tradeDataDict['hedgingRequiredFlag'])        
#         tradeDataDict = util.bsm_options_pricing(kite, tradeDataDict)
#         orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, kite, brokerAPI, tradeDataDict)
        
#         if (orderSuccessFlag):
#             response['status'] = 'success'
#             response['remarks'] = 'Sucessfully completed the hedging'
#         else:
#             response['status'] = 'failed'
#             response['remarks'] = 'Unable to complete the order placement for hedging'

#     except Exception as e:
#         response['status'] = 'failed'
#         response['remarks'] = f"Exception occured in options_short_strangle_entry: {str(e)}"
   
#     return response
