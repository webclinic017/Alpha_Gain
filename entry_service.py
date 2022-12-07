from utils import util_functions as util
from utils import broker_api_functions as baf
import logging
import requests
import datetime
import os
import pandas as pd
from config import Config
from utils import chart_patterns as cp
import concurrent.futures
import time
from utils import prostocks_api_helper as prostocksApi


def options_bull_credit_spread_entry(cnx, mySQLCursor, brokerApi, tradeDataDict):
    response = {}
    try:
        tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")         

        if (tradeDataDict['entryBuyConditionFlag']):
              
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Buy')):
                # Buy this first to hedge to put option sell
                tradeDataDict['positionDirection'] = 'LONG'
                tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')
                tradeDataDict['instrumentType'] = 'PUT'
                tradeDataDict['futTransactionType'] = 'BUY'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])
                
                brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                
                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

                # Sell in the money option when we get buy signal 
                tradeDataDict['instrumentType'] = 'PUT' 
                tradeDataDict['futTransactionType'] = 'SELL'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])            

                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

        elif (tradeDataDict['entrySellConditionFlag']):
              
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Sell')):

                tradeDataDict['positionDirection'] = 'SHORT'
                tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')
                # Buy this first to hedge the call option sell
                tradeDataDict['instrumentType'] = 'CALL'
                tradeDataDict['futTransactionType'] = 'BUY'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=1)             
                brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                
                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

                # Sell in the money call option when we get sell signal
                tradeDataDict['instrumentType'] = 'CALL'
                tradeDataDict['futTransactionType'] = 'SELL'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=1) 

                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
        response['status'] = 'success'
        response['remarks'] = 'options_bull_credit_spread_entry has been successfully completed'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = f"Exception occured in options_bull_credit_spread_entry: {str(e)}"
   
    return response

def options_bear_debt_spread_entry(cnx, mySQLCursor, brokerApi, tradeDataDict):
    response = {}
    try:
        tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")         

        if (tradeDataDict['entryBuyConditionFlag']):
              
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Buy')):
                # Buy this first to hedge to put option sell
                entryCondtionsMetFlag = True
                tradeDataDict['positionDirection'] = 'LONG'
                tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')
                tradeDataDict['instrumentType'] = 'PUT'
                tradeDataDict['futTransactionType'] = 'SELL'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=1)

                brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                
                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

                # Sell in the money option when we get buy signal 
                tradeDataDict['instrumentType'] = 'PUT' 
                tradeDataDict['futTransactionType'] = 'BUY'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=1)

                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

        elif (tradeDataDict['entrySellConditionFlag']):
              
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Sell')):
                tradeDataDict['positionDirection'] = 'SHORT'
                tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')
                # Buy this first to hedge the call option sell
                tradeDataDict['instrumentType'] = 'CALL'
                tradeDataDict['futTransactionType'] = 'SELL'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=1)
                
                brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                
                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

                # Sell in the money call option when we get sell signal
                tradeDataDict['instrumentType'] = 'CALL'
                tradeDataDict['futTransactionType'] = 'BUY'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=1)            

                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
        
        response['status'] = 'success'
        response['remarks'] = 'options_bear_debt_spread_entry has been successfully completed'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = f"Exception occured in options_bear_debt_spread_entry: {str(e)}"
   
    return response

def options_sell_entry(cnx, mySQLCursor, brokerApi, tradeDataDict):
    response = {}
    try:
        tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")         
        tradeDataDict['futTransactionType'] = 'SELL'
        
        if (tradeDataDict['entryBuyConditionFlag']):
              
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Buy')):
                # Sell in the money option when we get buy signal 
                tradeDataDict['instrumentType'] = 'PUT'             
                tradeDataDict['positionDirection'] = 'LONG'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=1)
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)
                if (tradeDataDict['status'] == 'success' and tradeDataDict['optionsEligibilityConditions'] != None and eval(tradeDataDict['optionsEligibilityConditions'])):                    
                    brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
                elif(tradeDataDict['optionsEligibilityConditions'] == None):
                    brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

        elif (tradeDataDict['entrySellConditionFlag']):
              
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Sell')):
                tradeDataDict['instrumentType'] = 'CALL'            
                tradeDataDict['positionDirection'] = 'SHORT'            
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=1)                
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)                
                if (tradeDataDict['status'] == 'success' and tradeDataDict['optionsEligibilityConditions'] != None and eval(tradeDataDict['optionsEligibilityConditions'])):                    
                    brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
                elif(tradeDataDict['optionsEligibilityConditions'] == None):
                    brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
                    
        response['status'] = 'success'
        response['remarks'] = 'options_sell_entry has been successfully completed'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = f"Exception occured in options_sell_entry: {str(e)}"
   
    return response
    
# Options buy check
def options_buy_entry(cnx, mySQLCursor, brokerApi, tradeDataDict):
    response = {}
    try:
        tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")    
        tradeDataDict['futTransactionType'] = 'BUY'

        if (tradeDataDict['entryBuyConditionFlag']):
              
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Buy')):
                tradeDataDict['instrumentType'] = 'CALL'
                tradeDataDict['positionDirection'] = 'LONG'            
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)
                if (tradeDataDict['status'] == 'success' and tradeDataDict['optionsEligibilityConditions'] != None and eval(tradeDataDict['optionsEligibilityConditions'])):                    
                    brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
                elif(tradeDataDict['optionsEligibilityConditions'] == None):
                    brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, brokerAPI, tradeDataDict)
        
        elif (tradeDataDict['entrySellConditionFlag']):
              
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Sell')):
                tradeDataDict['instrumentType'] = 'PUT'
                tradeDataDict['positionDirection'] = 'SHORT'
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])
                
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)
                if (tradeDataDict['status'] == 'success' and tradeDataDict['optionsEligibilityConditions'] != None and eval(tradeDataDict['optionsEligibilityConditions'])):                                        
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
                elif(tradeDataDict['optionsEligibilityConditions'] == None):                    
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
        response['status'] = 'success'
        response['remarks'] = 'options_buy_entry has been successfully completed'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = f"Exception occured in options_buy_entry - {tradeDataDict['tradeSymbol']}: {str(e)}"
    
    return response

def patterns_based_entry(cnx, mySQLCursor, brokerApi, tradeDataDict):
    response = {}
    try:
        if (tradeDataDict['entryBuyConditionFlag']):        
             
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Buy')):
                futInstName, futInstToken, lotSize, expDate, rawExpiry, exchangeToken = util.get_futures_instruments(mySQLCursor, tradeDataDict['tradeSymbol'])
                if (futInstName != ''):
                    tradeDataDict['futTradeSymbol']= tradeDataDict['tradeSymbol']  + expDate + 'F'
                    tradeDataDict['futInstToken'] = exchangeToken
                    tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")    
                    tradeDataDict['quantity'] = int(lotSize) * tradeDataDict['lotSizeMultiplier'] 
                    tradeDataDict['instrumentType'] = 'FUT'
                    tradeDataDict['exchange'] = 'NFO'
                    tradeDataDict['rawExpiry'] = rawExpiry
                    tradeDataDict['futTransactionType'] = 'BUY'
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)        

        elif (tradeDataDict['entrySellConditionFlag']):
              
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Sell')):        
                futInstName, futInstToken, lotSize, expDate, rawExpiry, exchangeToken = util.get_futures_instruments(mySQLCursor, tradeDataDict['tradeSymbol'])
                if (futInstName != ''):
                    tradeDataDict['futTradeSymbol']= tradeDataDict['tradeSymbol']  + expDate + 'F'
                    tradeDataDict['futInstToken'] = exchangeToken                
                    tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")    
                    tradeDataDict['quantity'] = int(lotSize) * tradeDataDict['lotSizeMultiplier'] 
                    tradeDataDict['instrumentType'] = 'FUT'
                    tradeDataDict['exchange'] = 'NFO'
                    tradeDataDict['rawExpiry'] = rawExpiry
                    tradeDataDict['futTransactionType'] = 'SELL'
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
        
        response['status'] = 'success'
        response['remarks'] = 'patterns_based_entry check has been successfully completed'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = f"Exception occured in patterns_based_entry - {tradeDataDict['tradeSymbol']}: {str(e)}"
   
    return response  

def options_short_strangle_entry(cnx, mySQLCursor, brokerApi, tradeDataDict):
    response = {}
    try:
       
        hedgingSuccessFlag = False
        orderSuccessFlag = False
        # The below portion of the code is to check only implied volatility and eligiblity conditions check
        
        # ------option eligiblity conditions check start------------
        if (tradeDataDict['optionsEligibilityConditions'] != None):
            tradeDataDict['instrumentType'] = 'PUT'
            tradeDataDict['futTransactionType'] = 'SELL'        
            tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])        
            tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)
        # ------option eligiblity conditions check end------------

        if (tradeDataDict['optionsEligibilityConditions'] == None or (tradeDataDict['status'] == 'success' and tradeDataDict['optionsEligibilityConditions'] != None and eval(tradeDataDict['optionsEligibilityConditions']))):
            tradeDataDict['tradeSequence'] = 'DELTA_ADJUST_0'
            tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")   
            tradeDataDict['positionDirection'] = 'NEUTRAL'

            if (tradeDataDict['positionGroupId'] == 0):
                tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')

            # Add the hedging if required field is added in user subscriptions page

            # if (tradeDataDict['hedgingRequiredFlag'] == 'Y' and tradeDataDict['placeHedgeFlag']):
            #     response = add_buy_hedging(cnx, mySQLCursor, kite, tradeDataDict, 'PUT', brokerAPI)
            #     # Put hedging is success; now proceed with call hedging
            #     if (response['status'] == 'success'):
            #         response = add_buy_hedging(cnx, mySQLCursor, kite, tradeDataDict, 'CALL', brokerAPI)
            #         if (response['status'] == 'success'):
            #             hedgingSuccessFlag = True
            
            # Continue to place the order only if hedging is successful or hedging is not required        
            # if (tradeDataDict['hedgingRequiredFlag'] == 'N' or tradeDataDict['placeExposureFlag']):
            tradeDataDict['instrumentType'] = 'PUT'
            tradeDataDict['futTransactionType'] = 'SELL'
            
            # ORDER BY DELTA CHANGE STARTS FROM HERE-------------------------------
            # Get the closest strike price that matches delta of untested options
                                                        
            bsmSelectedDict = util.get_strike_by_delta(mySQLCursor, brokerApi, tradeDataDict['CLOSE-PRICE'], tradeDataDict['baseTradeSymbol'], 'PUT', requiredDelta=0.30, minDaysToExpiry=tradeDataDict['minDaysToExpiry'], contractType=tradeDataDict['contractType'])                
            tradeDataDict['futTradeSymbol'] = str(tradeDataDict['tradeSymbol']).replace('&', '%26') + bsmSelectedDict['expiry'] + 'P' + str(bsmSelectedDict['selectedStrikePrice']).replace('.0','')
            tradeDataDict['futInstToken'] = bsmSelectedDict['selectedInstToken']
            tradeDataDict['strikePrice'] = bsmSelectedDict['selectedStrikePrice']
            tradeDataDict['rawExpiry'] = bsmSelectedDict['rawExpiry']
            tradeDataDict['quantity'] = int(bsmSelectedDict['lotSize']) * tradeDataDict['lotSizeMultiplier'] 


            # ORDER BY DELTA CHANGE ENDS HERE-------------------------------


            # tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])        
            
            # Reduent funtion, should be removed as these values are already calculated in get_strike_by_delta
            tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)

            # Place the first leg put options
            orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
            if (orderSuccessFlag):
                # Sell in the money option when we get buy signal 
                tradeDataDict['instrumentType'] = 'CALL' 
                tradeDataDict['futTransactionType'] = 'SELL'
                # tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)
                
                bsmSelectedDict = util.get_strike_by_delta(mySQLCursor, brokerApi, tradeDataDict['CLOSE-PRICE'], tradeDataDict['baseTradeSymbol'], 'CALL', requiredDelta=0.30, minDaysToExpiry=tradeDataDict['minDaysToExpiry'], contractType=tradeDataDict['contractType'])            
                tradeDataDict['futTradeSymbol'] = str(tradeDataDict['tradeSymbol']).replace('&', '%26') + bsmSelectedDict['expiry'] + 'C' + str(bsmSelectedDict['selectedStrikePrice']).replace('.0','')
                tradeDataDict['futInstToken'] = bsmSelectedDict['selectedInstToken']                    
                tradeDataDict['strikePrice'] = bsmSelectedDict['selectedStrikePrice']
                tradeDataDict['rawExpiry'] = bsmSelectedDict['rawExpiry']
                tradeDataDict['quantity'] = int(bsmSelectedDict['lotSize']) * tradeDataDict['lotSizeMultiplier']

                # Reduent funtion, should be removed as these values are already calculated in get_strike_by_delta
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)
                orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)
        
        
        response['status'] = 'success'
        response['remarks'] = 'options_short_strangle_entry check has been successfully completed'        

    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = f"Exception occured in options_short_strangle_entry: {str(e)}"
   
    return response    

def options_long_strangle_entry(cnx, mySQLCursor, brokerApi, tradeDataDict):
    response = {}
    try:
        tradeDataDict['tradeSequence'] = 'DELTA_ADJUST_0'
        tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")   
        tradeDataDict['futTransactionType'] = 'BUY'
        # Buy this first to hedge to put option sell    
        tradeDataDict['positionDirection'] = 'NEUTRAL'
        tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')
        tradeDataDict['instrumentType'] = 'PUT'        
        tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])        
        tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)
        
        brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])    
        orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

        # Sell in the money option when we get buy signal 
        tradeDataDict['instrumentType'] = 'CALL'         
        tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])
        orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

        response['status'] = 'success'
        response['remarks'] = 'options_short_strangle_entry has been successfully completed'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = f"Exception occured in options_short_strangle_entry: {str(e)}"
   
    return response

def hedged_futures_entry(cnx, mySQLCursor, brokerApi, tradeDataDict):
    response = {}
    try:
        tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")

        if (tradeDataDict['entryBuyConditionFlag']):
            
            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Buy')):
                tradeDataDict['instrumentType'] = 'PUT' 
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)
        
                alertMsg = f"The implied volatility is {tradeDataDict['IMPLIED_VOLATILITY']} for {tradeDataDict['futTradeSymbol']}"
                util.add_logs(cnx, mySQLCursor, 'INFO', alertMsg, tradeDataDict)
                
                if (tradeDataDict['status'] == 'success' and tradeDataDict['optionsEligibilityConditions'] != None and eval(tradeDataDict['optionsEligibilityConditions'])):
                    

                    # Buy the put options first for hedging, and then buy futures
                    tradeDataDict['positionDirection'] = 'LONG'
                    tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')                                            
                    tradeDataDict['futTransactionType'] = 'BUY'

                    
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

                    futInstName, futInstToken, lotSize, expDate, rawExpiry, futExchangeToken = util.get_futures_instruments(mySQLCursor, tradeDataDict['tradeSymbol'], minDaysToExpiry=tradeDataDict['minDaysToExpiry'])
                    tradeDataDict['futTradeSymbol']= tradeDataDict['tradeSymbol']  + expDate + 'F'
                    tradeDataDict['futInstToken'] = futInstToken
                    tradeDataDict['futExchangeToken'] = futExchangeToken
                    tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")    
                    tradeDataDict['quantity'] = int(lotSize) * tradeDataDict['lotSizeMultiplier'] 
                    tradeDataDict['instrumentType'] = 'FUT'
                    tradeDataDict['rawExpiry'] = rawExpiry
                    tradeDataDict['futTransactionType'] = 'BUY'   
                    tradeDataDict['dummyEntryFlag'] = "Y"
                    orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)        

                    # Now place the put option for hedging.             
                    


        elif (tradeDataDict['entrySellConditionFlag']):
            # Get the patter Signals

            if (tradeDataDict['revPatternCheckFlag'] == 'N' or (tradeDataDict['DIRECTION']['age'] < 5 and  tradeDataDict['DIRECTION']['direction'] == 'Sell')):
                tradeDataDict['instrumentType'] = 'CALL' 
                tradeDataDict = util.get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry=tradeDataDict['minDaysToExpiry'])
                tradeDataDict = util.bsm_options_pricing(brokerApi, tradeDataDict)        
                
                if (tradeDataDict['status'] == 'success'):
                    alertMsg = f"The implied volatility is {tradeDataDict['IMPLIED_VOLATILITY']} for {tradeDataDict['futTradeSymbol']}"
                    util.add_logs(cnx, mySQLCursor, 'INFO', alertMsg, tradeDataDict)

                    if (tradeDataDict['optionsEligibilityConditions'] != None and eval(tradeDataDict['optionsEligibilityConditions'])):
                        
                        # buy the call options of hedging and Sell the futures
                        tradeDataDict['positionDirection'] = 'SHORT'
                        tradeDataDict['positionGroupId'] = util.get_unique_id(cnx, mySQLCursor, tradeDataDict['strategyId'], 'POSITIONS_GROUP_ID')                    
                        tradeDataDict['futTransactionType'] = 'BUY'          
                        brokerAPI, isbrokerAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, tradeDataDict['accountId'], tradeDataDict['broker'], uatFlag = tradeDataDict['uatFlag'])        
                        orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

                        futInstName, futInstToken, lotSize, expDate, rawExpiry = util.get_futures_instruments(mySQLCursor, tradeDataDict['tradeSymbol'], minDaysToExpiry=tradeDataDict['minDaysToExpiry'])
                        tradeDataDict['futTradeSymbol']= tradeDataDict['tradeSymbol']  + expDate + 'F'
                        tradeDataDict['futInstToken'] = futInstToken
                        tradeDataDict['signalDate'] = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")    
                        tradeDataDict['quantity'] = int(lotSize) * tradeDataDict['lotSizeMultiplier'] 
                        tradeDataDict['instrumentType'] = 'FUT'
                        tradeDataDict['rawExpiry'] = rawExpiry
                        tradeDataDict['futTransactionType'] = 'SELL'       
                        tradeDataDict['dummyEntryFlag'] = "Y"
                        orderSuccessFlag = util.place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict)

                    # Now place the call option for hedging.
             

        
        response['status'] = 'success'
        response['remarks'] = 'hedged_futures_entry has been successfully completed'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = f"Exception occured in hedged_futures_entry: {str(e)}"
   
    return response

def process_entries(tradeAccounts):
    try:
        accountId = tradeAccounts[0]
        broker = tradeAccounts[1]        
        cnx, mySQLCursor = util.connect_mysql_db()
        # It fetches only the active stragies within a specific time frame; for testing you might need to change the start and end time in USS
        userList = util.get_strategy_setttings(mySQLCursor, accountId, entryOrExit='ENTRY')
        if (len(userList) > 0):
            brokerApi, isApiConnected = prostocksApi.connect_broker_api(cnx, mySQLCursor, accountId, broker)
            if (isApiConnected):
                for userData in userList:
                    allocatedCashExceededFlag = userData['ALLOCATED_CASH_EXCEEDED_FLAG']              
                    accountId = userData['TRADE_ACCOUNT']
                    strategyId = userData['STRATEGY_ID']
                    print(f"Checking {strategyId}")
                    # Take the trade in between scheduled trade time and end time                    
                    
                    # Don't proceed further in case no cash allocation available for the strategy
                    if (allocatedCashExceededFlag == 'N'):
                        
                        instList = util.get_user_defined_inst_list(mySQLCursor, strategyId, accountId)
                        # Check if there are any instruments to be processed for the specific strategy
                        if (len(instList) > 0):
                            tradeDataDict = util.copy_user_data_to_dict(strategyId, userData)
                        # Iterate through each instruments and check for the entry opertunities 
                            for instRow in instList:
                                tradeDataDict['instrumentToken']  = instRow[0]
                                tradeDataDict['tradeSymbol']  = instRow[1]                                        
                                tradeDataDict['stockName'] = instRow[2]                        
                                tradeDataDict['baseExchangeToken'] = str(instRow[3])
                                tradeDataDict['exchange'] = str(instRow[4])
                                if (len(instRow) >= 4):
                                    tradeDataDict['positionGroupId'] = instRow[3]
                                

                                tradeDataDict['baseTradeSymbol'] = tradeDataDict['tradeSymbol']
                                tradeDataDict['entryBuyConditionFlag'] = False
                                tradeDataDict['entrySellConditionFlag'] = False
                                
                                
                            
                                # Get the technical indicator only when it is needed in either entry or sell
                                if (tradeDataDict['entryBuyCondition'] != None or tradeDataDict['entrySellCondition'] != None):
                                    tradeDataDict, techIndResponse = util.techinical_indicator_in_dict(brokerApi, tradeDataDict, interval=tradeDataDict['techIndicatorInterval'])
                                    
                                    # if the response from techinical indicator is in failed status, don't process further
                                    if (techIndResponse['status'] == 'success'):
                                        try:
                                            if (tradeDataDict['entryBuyCondition'] != None and eval(tradeDataDict['entryBuyCondition'])):
                                                tradeDataDict['entryBuyConditionFlag'] = True

                                            elif (tradeDataDict['entrySellCondition'] != None and eval(tradeDataDict['entrySellCondition'])):
                                                tradeDataDict['entrySellConditionFlag'] = True
                                        except Exception as e:
                                            util.add_logs(cnx, mySQLCursor, 'ERROR', f"Unable to execute entry/exit condition check {str(e)} ", tradeDataDict)                                    
                                    else:                  
                                        util.add_logs(cnx, mySQLCursor, 'ERROR', techIndResponse['remarks'], tradeDataDict)
                                else:
                                    tradeDataDict['CLOSE-PRICE']  = prostocksApi.get_last_traded_price(brokerApi, tradeDataDict['exchangeToken'], exchange=tradeDataDict['exchange'])
                                    
                                # Check the reveral patterns only when it is needed
                                if (tradeDataDict['revPatternCheckFlag'] == 'Y'):
                                    tradeDataDict = util.get_all_pattern_signals(cnx, mySQLCursor, brokerApi, tradeDataDict, interval=tradeDataDict['techIndicatorInterval'])
                        
                                # ## ## ## ## ## ## ## CHECK THE STRATEGIES FROM HERE ## ## ## ## ## ## ## #
                                if (tradeDataDict['strategyType'] == 'OPTIONS_BUY'):
                                    response = options_buy_entry(cnx, mySQLCursor, brokerApi, tradeDataDict)
                                    if (response['status'] == 'failed'):
                                        util.add_logs(cnx, mySQLCursor, 'ERROR', response['remarks'], tradeDataDict)

                                elif (tradeDataDict['strategyType'] == 'OPTIONS_SELL'):
                                    response = options_sell_entry(cnx, mySQLCursor, brokerApi, tradeDataDict)
                                    if (response['status'] == 'failed'):
                                        util.add_logs(cnx, mySQLCursor, 'ERROR', response['remarks'], tradeDataDict)
                                
                                elif (tradeDataDict['strategyType'] == 'FUT_PATTERN_BASED_ENTRY'):
                                    response = patterns_based_entry(cnx, mySQLCursor, brokerApi, tradeDataDict)
                                    if (response['status'] == 'failed'):
                                        util.add_logs(cnx, mySQLCursor, 'ERROR', response['remarks'], tradeDataDict)
                                
                                elif (tradeDataDict['strategyType'] == 'OPTIONS_SHORT_STRANGLE'):
                                    response = options_short_strangle_entry(cnx, mySQLCursor, brokerApi, tradeDataDict)
                                    if (response['status'] == 'failed'):
                                        util.add_logs(cnx, mySQLCursor, 'ERROR', response['remarks'], tradeDataDict)

                                elif (tradeDataDict['strategyType'] == 'OPTIONS_LONG_STRANGLE'):
                                    response = options_long_strangle_entry(cnx, mySQLCursor, brokerApi, tradeDataDict)
                                    if (response['status'] == 'failed'):
                                        util.add_logs(cnx, mySQLCursor, 'ERROR', response['remarks'], tradeDataDict)

                                elif (tradeDataDict['strategyType'] == 'OPTIONS_SELL_WITH_HEDGE'):
                                    response = options_bull_credit_spread_entry(cnx, mySQLCursor, brokerApi, tradeDataDict)
                                    if (response['status'] == 'failed'):
                                        util.add_logs(cnx, mySQLCursor, 'ERROR', response['remarks'], tradeDataDict)

                                elif (tradeDataDict['strategyType'] == 'OPTIONS_BEAR_DEBT_SPREAD'):
                                    response = options_bear_debt_spread_entry(cnx, mySQLCursor, brokerApi, tradeDataDict)
                                    if (response['status'] == 'failed'):
                                        util.add_logs(cnx, mySQLCursor, 'ERROR', response['remarks'], tradeDataDict)
                                
                                elif (tradeDataDict['strategyType'] == 'HEDGED_FUTURES'):
                                    response = hedged_futures_entry(cnx, mySQLCursor, brokerApi, tradeDataDict)
                                    if (response['status'] == 'failed'):
                                        util.add_logs(cnx, mySQLCursor, 'ERROR', response['remarks'], tradeDataDict)

                                # ## ## ## ## ## ## ## ALL THE STRATEGIES ARE FINISHED ## ## ## ## ## ## ## #
                    end=datetime.datetime.now()
            cnx.commit()    
    except Exception as e:
        print(e)

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
            try:                
                activeAccountsList =  util.get_active_trade_accounts(mySQLCursor, entryOrExit='ENTRY')
                
                start = time.perf_counter()
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                    results = executor.map(process_entries, activeAccountsList)
                
                end = time.perf_counter()
                
                print(f"total time taken {end - start}")

            except Exception as e:
                alertMsg = f"Exception occured in the main entry program: {str(e)}" 
                util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, sysDict)

        elif (int(currentTime) > int(sysSettings['SYSTEM_END_TIME'])):           
            programExitFlag = False
            util.add_logs(cnx, mySQLCursor, 'ALERT', f'Program {programName} ended', sysDict)
        
        # util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
        util.disconnect_db(cnx, mySQLCursor)