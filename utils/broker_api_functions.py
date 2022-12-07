import datetime
from kiteconnect import KiteConnect
import time
import mysql.connector
import logging
import os
import json
from bson import json_util

from . import util_functions as util
from . import broker_api_functions as baf
import pandas as pd
import platform
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import urllib.parse as urlparse
from urllib.parse import parse_qs
import requests
import re


def get_prostocks_positions (brokerApi, tradeSymbol):
    orderData = brokerApi.get_positions()
    existingOrderFound = False
    quantity = 0
    if (isinstance(orderData, list) and len(orderData) > 0 and orderData[0]['stat'] == 'Ok'):
        for orders in orderData:
            try:
                exch = orders["exch"]
                posTradeSymbol = orders["tsym"]    
                quantity = int(orders["netqty"])

                if (exch == "NFO" and  posTradeSymbol == tradeSymbol and int(quantity) != 0):
                    existingOrderFound = True
                    break
                else:
                    quantity = 0

            except Exception as e:
                logging.info("Errored while updating the order status: " + str(e))            
                pass
    return existingOrderFound, quantity


def get_prostocks_orders(brokerApi, tradeSymbol, inTransType = None):
    if inTransType == 'BUY':
        inTransType='B'
    else:
        inTransType='S'
   
    orderData = brokerApi.get_order_book()

    existingOrderFound = False
    orderId = 0
    transactionType = ''
    quantity = 0
    if (isinstance(orderData, list) and len(orderData) > 0 and orderData[0]['stat'] == 'Ok'):
        for orders in orderData:
            try:
                exch = orders["exch"]
                posTradeSymbol = orders["tsym"]
                orderStatus = orders["status"] 
                quantity = orders["qty"]
                orderId = orders["norenordno"]
                transactionType = orders["trantype"]

                if (exch == "NFO" and  posTradeSymbol == tradeSymbol and int(quantity) != 0 and orderStatus == 'OPEN' and inTransType == transactionType):
                    existingOrderFound = True
                    break
                else:
                    quantity = 0
                    
            except Exception as e:
                logging.info("Errored while updating the order status: " + str(e))            
                pass
        
        if (not(existingOrderFound)):
            orderId = 0
            transactionType = ''
            quantity = 0
    return existingOrderFound, orderId, transactionType, quantity
    

def get_historical_data(kite, instrumentToken, fromDate, toDate, interval, broker=None):
    try:
        return kite.historical_data(instrumentToken, fromDate, toDate, interval)       
    except:        
        try:
            time.sleep(1)
            return kite.historical_data(instrumentToken, fromDate, toDate, interval)       
        except:
            try:
                time.sleep(2)            
                logging.info("Trying to get historical data after 3 seconds")
                return kite.historical_data(instrumentToken, fromDate, toDate, interval)       
            except Exception as e:
                logging.info("Error while trying to get historical data: " + str(e))
                return ""


def placeFNOOrder(kite, transactionType, fnoTradingSymbol,fnoLotSize, fnoBuyPrice, fnoLastPrice):
    try:
        gttOrders = [{"transaction_type": transactionType, "quantity": fnoLotSize,
                      "price": float(fnoBuyPrice), "order_type": kite.ORDER_TYPE_LIMIT, "product": kite.PRODUCT_NRML}]

        orderResp = kite.place_gtt(trigger_type="single",
                                   tradingsymbol=fnoTradingSymbol,
                                   exchange="NFO",
                                   trigger_values=[float(fnoBuyPrice)],
                                   last_price=float(fnoLastPrice),
                                   orders=gttOrders
                                   )


        triggerId = orderResp["trigger_id"]

        orderRemarks = "FNO ORDER PLACED"

        logging.info(transactionType +
                     " ORDER PLACED FOR FNO, AND THE ID IS: " + str(triggerId))
        print("BUY ORDER PLACED FOR FNO: " + str(triggerId))
    except Exception as e:
        logging.info(transactionType + " ORDER PLACEMENT FAILED FOR FNO: " + str(e))
        print(transactionType + " ORDER PLACEMENT FAILED FOR FNO: " + str(e))
                                                                         
        orderRemarks = str(e)
        triggerId = 0
    logging.info(
        "*******************************************************")
    return triggerId, orderRemarks


def placeGTTOrder(kite, transactionType, tradeSymbol, quantity, triggerPrice, lastPrice):
    try:
        gttOrders = [{"transaction_type": transactionType, "quantity": quantity,
                      "price": float(triggerPrice), "order_type": kite.ORDER_TYPE_LIMIT, "product": kite.PRODUCT_CNC}]

        orderResp = kite.place_gtt(trigger_type="single",
                                   tradingsymbol=tradeSymbol,
                                   exchange=kite.EXCHANGE_NSE,
                                   trigger_values=[float(triggerPrice)],
                                   last_price=float(lastPrice),
                                   orders=gttOrders
                                   )

        triggerId = orderResp["trigger_id"]
        # orderId = 1
        orderRemarks = "GTT ORDER PLACED"

        logging.info(transactionType +
                     " ORDER PLACED ID IS: " + str(triggerId))        
        print("BUY ORDER PLACED FOR SWING: " + str(triggerId))
    except Exception as e:
        logging.info(transactionType + " ORDER PLACEMENT FAILED: " + str(e))
        print(transactionType + " ORDER PLACEMENT FAILED: " + str(e))
                                                                         
        orderRemarks = str(e)
        triggerId = 0
    logging.info(
        "*******************************************************")
    return triggerId, orderRemarks


def placeLimitOrder(kite, transactionType, tradeSymbol, quantity, testingFlag, triggerPrice, orderPrice,tagId):
    try:

        if(testingFlag):
            orderId = kite.place_order(tradingsymbol=tradeSymbol,
                                       exchange=kite.EXCHANGE_NSE,
                                       transaction_type=transactionType,
                                       quantity=quantity,
                                       order_type=kite.ORDER_TYPE_MARKET,
                                       product=kite.PRODUCT_MIS,
                                       variety=kite.VARIETY_AMO)
        else:
            orderId = kite.place_order(tradingsymbol=tradeSymbol,
                                       exchange=kite.EXCHANGE_NSE,
                                       transaction_type=transactionType,
                                       quantity=quantity,
                                       order_type=kite.ORDER_TYPE_LIMIT,
                                       product=kite.PRODUCT_MIS,
                                       variety=kite.VARIETY_REGULAR,
                                       trigger_price=triggerPrice,
                                       price=orderPrice,
                                       tag=tagId)

        # orderId = 1
        orderRemarks = "ORDER PLACED"

        logging.info(transactionType + " ORDER PLACED ID IS: " + str(orderId))
        print(transactionType + " ORDER PLACED ID IS: " + str(orderId))
    except Exception as e:
        logging.info(transactionType + " ORDER PLACEMENT FAILED: " + str(e))
        print(transactionType + " ORDER PLACEMENT FAILED: " + str(e))
        orderRemarks = str(e)
        orderId = 0
    logging.info(
        "*******************************************************")
    return orderId, orderRemarks


def placeMISExitOrder(kite, transactionType, tradeSymbol, quantity,tagId):
    try:

        orderId = kite.place_order(tradingsymbol=tradeSymbol,
                                   exchange=kite.EXCHANGE_NSE,
                                   transaction_type=transactionType,
                                   quantity=quantity,
                                   order_type=kite.ORDER_TYPE_MARKET,
                                   product=kite.PRODUCT_MIS,
                                   variety=kite.VARIETY_REGULAR,
                                   tag=tagId)

        # orderId = 1
        orderRemarks = "ORDER PLACED"

        logging.info(transactionType + " ORDER PLACED ID IS: " + str(orderId))
        print(transactionType + " ORDER PLACED ID IS: " + str(orderId))
    except Exception as e:
        logging.info(transactionType + " ORDER PLACEMENT FAILED: " + str(e))
        print(transactionType + " ORDER PLACEMENT FAILED: " + str(e))
        orderRemarks = str(e)
        orderId = 0
    logging.info(
        "*******************************************************")
    return orderId, orderRemarks


def place_buy_order(brokerApi, accountId, broker, quantity, tradeSymbol, triggerPrice, lastPrice, stockName, orderType):    
    orderId = 0
    orderRemarks = ''
    if (broker == "ZERODHA"):
        try:
            if (orderType == "GTT"):
                gttOrders = [{"transaction_type": "BUY", "quantity": int(quantity), "price": float(triggerPrice), "order_type": brokerApi.ORDER_TYPE_LIMIT, "product": brokerApi.PRODUCT_CNC}]
                
                orderResponse = brokerApi.place_gtt(trigger_type="single",
                                            tradingsymbol=tradeSymbol,
                                            exchange=brokerApi.EXCHANGE_NSE,
                                            trigger_values=[float(triggerPrice)],
                                            last_price=float(lastPrice),
                                            orders=gttOrders
                                            )

                orderId = orderResponse["trigger_id"]            

                orderRemarks = "GTT ORDER PLACED"
                logging.info("GTT BUY ORDER PLACED ID IS: " + str(orderId))   

            elif(orderType == "LMT"):
                orderId = brokerApi.place_order(tradingsymbol=tradeSymbol,
                                        exchange=brokerApi.EXCHANGE_NSE,
                                        transaction_type= "BUY",
                                        quantity=quantity,
                                        order_type=brokerApi.ORDER_TYPE_LIMIT,
                                        product=brokerApi.PRODUCT_CNC,
                                        variety=brokerApi.VARIETY_REGULAR,
                                        trigger_price=triggerPrice,
                                        price=lastPrice)

                orderRemarks = 'LMT ORDER PLACED'
                logging.info("LIMIT BUY ORDER PLACED FOR "+ str(tradeSymbol)+ " WITH ID: " + str(orderId))
            
            elif(orderType == "MKT"):
                orderId = brokerApi.place_order(tradingsymbol=tradeSymbol,
                                        exchange=brokerApi.EXCHANGE_NSE,
                                        transaction_type= "BUY",
                                        quantity=quantity,
                                        order_type=brokerApi.ORDER_TYPE_MARKET,
                                        product=brokerApi.PRODUCT_CNC,
                                        variety=brokerApi.VARIETY_REGULAR)

                orderRemarks = 'MKT ORDER PLACED'
                logging.info("MARKET BUY ORDER PLACED FOR "+ str(tradeSymbol)+ " WITH ID: " + str(orderId))

        except Exception as e:
            logging.info("ORDER PLACEMENT FAILED: " + str(e))
            orderRemarks = str(e)
            
        return orderId, orderRemarks
    elif(broker == "PROSTOCKS"):    

        try: 
            tradeSymbol = (tradeSymbol  + "-EQ").replace('&', '%26')

            if (orderType == 'LMT'):   
                data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\"NSE\", \"tsym\":\""+tradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(triggerPrice)+"\", \"prd\":\"C\", \"trantype\":\"B\", \"prctyp\":\"LMT\", \"ret\":\"DAY\"}&jKey=" + brokerApi
            elif (orderType == 'MKT'):   
                data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\"NSE\", \"tsym\":\""+tradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(0)+"\", \"prd\":\"C\", \"trantype\":\"B\", \"prctyp\":\"MKT\", \"ret\":\"DAY\"}&jKey=" + brokerApi        
            else:
                logging.info('Order Placement is failed: Order Type is not supported')
                
                return -1, 'Order Placement is failed: Order Type is not supported'

            response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
            json_response = response.json()

            if (json_response['stat'] == 'Ok'):        
                orderId = json_response['norenordno']
                logging.info("LIMIT BUY ORDER PLACED FOR "+ str(tradeSymbol)+ " WITH ID: " + str(orderId))       
                return orderId, 'LIMIT ORDER PLACED'
            else:
                logging.info('Order Placement is failed: ' + json_response['emsg'])
                
                return -1, 'Order Placement is failed: ' + json_response['emsg']

        except Exception as e:
            logging.info('Order Placement is failed: ' + str(e))

            return -1, 'Order Placement is failed: ' + str(e)




def place_cash_buy_order(brokerApi, tradeDataDict):  

    broker= tradeDataDict['broker']
    tradeSymbol = tradeDataDict['tradeSymbol']    
    accountId = tradeDataDict['accountId']
    quantity = tradeDataDict['quantity']
    triggerPrice = tradeDataDict['triggerPrice']
    orderType = tradeDataDict['orderType']
    lastPrice = tradeDataDict['lastPrice']
    orderRemarks = 'CASH ORDER PLACED'
    orderId = 0

    if (broker == "ZERODHA"):
        try:
            if (orderType == "GTT"):
                gttOrders = [{"transaction_type": "BUY", "quantity": int(quantity), "price": float(triggerPrice), "order_type": brokerApi.ORDER_TYPE_LIMIT, "product": brokerApi.PRODUCT_CNC}]
                
                orderResponse = brokerApi.place_gtt(trigger_type="single",
                                            tradingsymbol=tradeSymbol,
                                            exchange=brokerApi.EXCHANGE_NSE,
                                            trigger_values=[float(triggerPrice)],
                                            last_price=float(lastPrice),
                                            orders=gttOrders
                                            )

                orderId = orderResponse["trigger_id"]           

                logging.info("GTT BUY ORDER PLACED ID IS: " + str(orderId))   

            elif(orderType == "LMT"):
                orderId = brokerApi.place_order(tradingsymbol=tradeSymbol,
                                        exchange=brokerApi.EXCHANGE_NSE,
                                        transaction_type= "BUY",
                                        quantity=quantity,
                                        order_type=brokerApi.ORDER_TYPE_LIMIT,
                                        product=brokerApi.PRODUCT_CNC,
                                        variety=brokerApi.VARIETY_REGULAR,
                                        trigger_price=triggerPrice,
                                        price=lastPrice)

                logging.info("LIMIT BUY ORDER PLACED FOR "+ str(tradeSymbol)+ " WITH ID: " + str(orderId))
            
            elif(orderType == "MKT"):
                orderId = brokerApi.place_order(tradingsymbol=tradeSymbol,
                                        exchange=brokerApi.EXCHANGE_NSE,
                                        transaction_type= "BUY",
                                        quantity=quantity,
                                        order_type=brokerApi.ORDER_TYPE_MARKET,
                                        product=brokerApi.PRODUCT_CNC,
                                        variety=brokerApi.VARIETY_REGULAR)

                logging.info("MARKET BUY ORDER PLACED FOR "+ str(tradeSymbol)+ " WITH ID: " + str(orderId))

        except Exception as e:
            logging.info("ORDER PLACEMENT FAILED: " + str(e))
            orderRemarks = str(e)    

    elif(broker == "PROSTOCKS"):    

        try: 
            tradeSymbol = (tradeSymbol  + "-EQ").replace('&', '%26')

            if (orderType == 'LMT' or orderType == 'LEG2_CASH'):   
                data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\"NSE\", \"tsym\":\""+tradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(triggerPrice)+"\", \"prd\":\"C\", \"trantype\":\"B\", \"prctyp\":\"LMT\", \"ret\":\"DAY\"}&jKey=" + brokerApi
            elif (orderType == 'MKT'):   
                data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\"NSE\", \"tsym\":\""+tradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(0)+"\", \"prd\":\"C\", \"trantype\":\"B\", \"prctyp\":\"MKT\", \"ret\":\"DAY\"}&jKey=" + brokerApi        
            else:
                logging.info('Order Placement is failed: Order Type is not supported')                
                orderRemarks = 'Order Placement is failed: Order Type is not supported'

            response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
            json_response = response.json()

            if (json_response['stat'] == 'Ok'):        
                orderId = json_response['norenordno']
                logging.info("LIMIT BUY ORDER PLACED FOR "+ str(tradeSymbol)+ " WITH ID: " + str(orderId))       
            else:
                logging.info('Order Placement is failed: ' + json_response['emsg'])                
                orderRemarks = 'Order Placement is failed: ' + json_response['emsg']

        except Exception as e:
            logging.info('Order Placement is failed: ' + str(e))

            orderRemarks = 'Order Placement is failed: ' + str(e)

    return orderId, orderRemarks


def modify_fno_order(brokerApi, accountId, orderId, futTriggerPrice, futTradeSymbol, quantity, uatFlag = False):
    try:

        data = "jData={\"uid\":\""+accountId+"\", \"norenordno\":\""+orderId+"\", \"exch\":\"NFO\", \"tsym\":\""+futTradeSymbol+"\", \"prc\":\""+str(futTriggerPrice)+"\", \"qty\":\""+str(quantity)+"\", \"prctyp\":\"LMT\", \"ret\":\"DAY\"}&jKey=" + brokerApi
        
        if uatFlag:
            response = requests.post('https://staruat.prostocks.com/NorenWClientTP/ModifyOrder', data=data)
        else:
            response = requests.post('https://starapi.prostocks.com/NorenWClientTP/ModifyOrder', data=data)
        
        json_response = response.json()

        if (json_response['stat'] == 'Ok'):        
            orderId = json_response['result']
            logging.info(f"Modify order completed for {futTradeSymbol} with order id: {str(orderId)} in account: {str(accountId)}")
    
    except Exception as e:
        orderRemarks = str(e)           
        logging.info(str(accountId) + ": ORDER PLACEMENT FAILED: " + str(e))

   


# Place BUY order for future instruments 
def place_future_buy_order(brokerApi, tradeDataDict):    
    broker= tradeDataDict['broker']
    quantity = abs(tradeDataDict['quantity'])
    accountId = tradeDataDict['accountId']
    futTriggerPrice = tradeDataDict['futTriggerPrice']
    futTradeSymbol = tradeDataDict['futTradeSymbol']               
    futOrderType = tradeDataDict['futOrderType']                   
    futLastPrice = tradeDataDict['futLastPrice']
    futTransactionType = tradeDataDict['futTransactionType']
    exchange = 'NFO'
    orderId = 0
    orderRemarks = 'FUTURE ORDER PLACED'
    if(broker == "ZERODHA"):  
        try:
            orderId = 0
            orderId = brokerApi.place_order(
                                    tradingsymbol=futTradeSymbol,
                                    exchange=brokerApi.EXCHANGE_NFO,
                                    transaction_type= "BUY",
                                    quantity=quantity,
                                    order_type=brokerApi.ORDER_TYPE_LIMIT,
                                    product=brokerApi.PRODUCT_NRML,
                                    variety=brokerApi.VARIETY_REGULAR,
                                    trigger_price=futTriggerPrice,
                                    price=futLastPrice)    
            logging.info("FUTURE BUY ORDER PLACED FOR "+ str(futTradeSymbol)+ " WITH ID: " + str(orderId) + "FOR ACCOUNT: "+ str(accountId))

        except Exception as e:            
            orderRemarks = str(e)           
            logging.info(str(accountId) + ": ORDER PLACEMENT FAILED: " + str(e))
    
    elif(broker == "PROSTOCKS"):  
        try:
            prdType='M'
            if (futTransactionType == 'BUY'):
                futTransactionType = 'B'
            else:
                futTransactionType = 'S'
            
            if (futOrderType == 'MKT'):
                futTriggerPrice = 0

            # # data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\"NFO\", \"tsym\":\""+futTradeSymbol+"\"
            # # ,\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(futTriggerPrice)+"\", \"prd\":\""+str(prdType)+"\", \"trantype\":\""+str(futTransactionType)+"\",
            # #  \"prctyp\":\""+str(futOrderType)+"\", \"ret\":\"DAY\"}&jKey=" + brokerApi
            # if uatFlag:
            #     response = requests.post('https://staruat.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
            # else:
            #     response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
            json_response =  brokerApi.place_order(buy_or_sell=futTransactionType, product_type=prdType,
                                exchange=exchange, tradingsymbol=futTradeSymbol, 
                                quantity=quantity, discloseqty=0,price_type=futOrderType, price=futTriggerPrice, trigger_price=None,
                                retention='DAY', remarks='my_order_001')

            if (json_response['stat'] == 'Ok'):        
                orderId = json_response['norenordno']
                logging.info(f"{futTransactionType} ORDER PLACED FOR {str(futTradeSymbol)} WITH ID: {str(orderId)} FOR ACCOUNT {accountId}")
            
        except Exception as e:
            orderRemarks = str(e)           
            logging.info(str(accountId) + ": ORDER PLACEMENT FAILED: " + str(e))
               
        
    return orderId, orderRemarks




# Needs to be amended 
def place_options_buy_order(brokerApi, tradeDataDict):    
    broker= tradeDataDict['broker']
    quantity = tradeDataDict['quantity']  
    accountId = tradeDataDict['accountId']
    orderId = 0
    orderRemarks = 'OPTIONS ORDER PLACED'    
    optionsTriggerPrice = tradeDataDict['optionsTriggerPrice']
    optionsTradeSymbol = tradeDataDict['optionsTradeSymbol']
    transactionType = tradeDataDict['transactionType']

    if(broker == "PROSTOCKS"):  
        try:
            prdType='M'
            if (transactionType == 'BUY'):
                transactionType = 'B'
            else:
                transactionType = 'S'

            data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\"NFO\", \"tsym\":\""+optionsTradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(optionsTriggerPrice)+"\", \"prd\":\""+str(prdType)+"\", \"trantype\":\""+str(transactionType)+"\", \"prctyp\":\"LMT\", \"ret\":\"DAY\"}&jKey=" + brokerApi
            response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
            json_response = response.json()
            if (json_response['stat'] == 'Ok'):        
                orderId = json_response['norenordno']                                    
                logging.info("BUY ORDER PLACED FOR "+ str(optionsTradeSymbol)+ " WITH ID: " + str(orderId) + "FOR ACCOUNT: "+ str(accountId))
        
        except Exception as e:
            logging.info("ORDER PLACEMENT FAILED: " + str(e))
            orderRemarks = str(e)        
        
    return orderId, orderRemarks


def place_options_order(brokerApi, tradeDataDict):    
    broker= tradeDataDict['broker']
    quantity = tradeDataDict['quantity']  
    accountId = tradeDataDict['accountId']
    orderId = 0
    orderRemarks = 'OPTIONS ORDER PLACED'    
    optionsTriggerPrice = tradeDataDict['optionsTriggerPrice']
    optionsTradeSymbol = (tradeDataDict['optionsTradeSymbol'])
    transactionType = tradeDataDict['transactionType']

    if(broker == "PROSTOCKS"):  
        try:
            prdType='M'
            if (transactionType == 'BUY'):
                transactionType = 'B'
            else:
                transactionType = 'S'
            data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\"NFO\", \"tsym\":\""+optionsTradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(optionsTriggerPrice)+"\", \"prd\":\""+str(prdType)+"\", \"trantype\":\""+str(transactionType)+"\", \"prctyp\":\"LMT\", \"ret\":\"DAY\"}&jKey=" + brokerApi
            response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
            json_response = response.json()
            if (json_response['stat'] == 'Ok'):        
                orderId = json_response['norenordno']                                    
                logging.info("BUY ORDER PLACED FOR "+ str(optionsTradeSymbol)+ " WITH ID: " + str(orderId) + "FOR ACCOUNT: "+ str(accountId))
        
        except Exception as e:
            logging.info("ORDER PLACEMENT FAILED: " + str(e))
            orderRemarks = str(e)        
        
    return orderId, orderRemarks




# Needs to be amended 
def place_2l_fut_buy_order(brokerApi, tradeDataDict, tradeType, exchange):    
    broker= tradeDataDict['broker']
    quantity = tradeDataDict['quantity']  
    accountId = tradeDataDict['accountId']

    if(broker == "ZERODHA"):  
        try: 
            if (tradeType == 'LEG1_PUT'):
                try:
                    optionsLastPrice = tradeDataDict['optionsLastPrice']
                    optionsTriggerPrice = tradeDataDict['optionsTriggerPrice']
                    optionsTradeSymbol = (tradeDataDict['optionsTradeSymbol'])
                    orderId = 0
              
                    orderId = brokerApi.place_order(
                                        tradingsymbol=optionsTradeSymbol,
                                        exchange=brokerApi.EXCHANGE_NFO,
                                        transaction_type= "BUY",
                                        quantity=quantity,
                                        order_type=brokerApi.ORDER_TYPE_LIMIT,
                                        product=brokerApi.PRODUCT_NRML,
                                        variety=brokerApi.VARIETY_REGULAR,
                                        trigger_price=optionsTriggerPrice,
                                        price=optionsLastPrice)


                    orderRemarks = 'LMT ORDER PLACED'
                    logging.info("LMT BUY ORDER PLACED FOR LEG1_PUT ON "+ str(optionsTradeSymbol)+ " WITH ID: " + str(orderId))
                except Exception as e:
                    logging.info("ORDER PLACEMENT FAILED: " + str(e))
                    orderRemarks = str(e)

                return orderId, orderRemarks

            elif (tradeType == 'LEG2_FUT' or tradeType == 'FUT'):
                try:
                    futTriggerPrice = tradeDataDict['futTriggerPrice']
                    futTradeSymbol = (tradeDataDict['futTradeSymbol'])
                    futLastPrice = (tradeDataDict['futLastPrice'])
                    orderId = 0
                    orderId = brokerApi.place_order(
                                        tradingsymbol=futTradeSymbol,
                                        exchange=brokerApi.EXCHANGE_NFO,
                                        transaction_type= "BUY",
                                        quantity=quantity,
                                        order_type=brokerApi.ORDER_TYPE_LIMIT,
                                        product=brokerApi.PRODUCT_NRML,
                                        variety=brokerApi.VARIETY_REGULAR,
                                        trigger_price=futTriggerPrice,
                                        price=futLastPrice)                    
                  
                    orderRemarks = 'LMT ORDER PLACED'
                    logging.info("LMT BUY ORDER PLACED FOR LEG2_FUT ON  "+ str(futTradeSymbol)+ " WITH ID: " + str(orderId))
                except Exception as e:
                    logging.info("ORDER PLACEMENT FAILED: " + str(e))
                    orderRemarks = str(e)
                return orderId, orderRemarks
        except Exception as e:
            logging.info("ORDER PLACEMENT FAILED OUTSIDE WHILE PLACING 2L_FUTURE ORDER WITH ZERODHA: " + str(e))
            orderRemarks = str(e)
    
    elif(broker == "PROSTOCKS"):  
     
        orderId = 0

        if (tradeType == 'LEG1_PUT'):                
            try:
                optionsLastPrice = tradeDataDict['optionsLastPrice']
                optionsTriggerPrice = tradeDataDict['optionsTriggerPrice']
                optionsTradeSymbol = tradeDataDict['optionsTradeSymbol']
                prdType='M'

                data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\""+exchange+"\", \"tsym\":\""+optionsTradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(optionsTriggerPrice)+"\", \"prd\":\""+str(prdType)+"\", \"trantype\":\"B\", \"prctyp\":\"LMT\", \"ret\":\"DAY\"}&jKey=" + brokerApi
                response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
                json_response = response.json()

                if (json_response['stat'] == 'Ok'):        
                    orderId = json_response['norenordno']
                    orderRemarks = tradeType + ' ORDER PLACED'                        
                    logging.info("LMT BUY ORDER PLACED FOR LEG1_PUT ON "+ str(optionsTradeSymbol)+ " WITH ID: " + str(orderId))
            
            except Exception as e:
                logging.info("ORDER PLACEMENT FAILED: " + str(e))
                orderRemarks = str(e)

        elif (tradeType == 'LEG2_FUT'):
            try:
                
                futTriggerPrice = tradeDataDict['futTriggerPrice']
                futTradeSymbol = tradeDataDict['futTradeSymbol']               
                futLastPrice = (tradeDataDict['futLastPrice'])
                prdType='M'

                data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\""+exchange+"\", \"tsym\":\""+futTradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(futTriggerPrice)+"\", \"prd\":\""+str(prdType)+"\", \"trantype\":\"B\", \"prctyp\":\"LMT\", \"ret\":\"DAY\"}&jKey=" + brokerApi
                response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
                json_response = response.json()

                if (json_response['stat'] == 'Ok'):        
                    orderId = json_response['norenordno']
                    orderRemarks = tradeType + ' ORDER PLACED'
                    logging.info(tradeType + " BUY ORDER PLACED FOR "+ str(futTradeSymbol)+ " WITH ID: " + str(orderId))
            
                
            except Exception as e:
                logging.info("ORDER PLACEMENT FAILED: " + str(e))
                orderRemarks = str(e)
               
        
        return orderId, orderRemarks


def place_sell_order(brokerApi, accountId, broker, tradeSymbol, quantity, triggerPrice = None, exchange = None):
    if(broker == "ZERODHA"):    
        try:
            orderId = 0

            if (exchange == None or exchange == 'NSE'):
                orderId = brokerApi.place_order(tradingsymbol=tradeSymbol,
                                        exchange=brokerApi.EXCHANGE_NSE,
                                        transaction_type="SELL",
                                        quantity=quantity,
                                        order_type=brokerApi.ORDER_TYPE_MARKET,
                                        product=brokerApi.PRODUCT_CNC,
                                        variety=brokerApi.VARIETY_REGULAR)

            elif(exchange == 'NFO'):
                orderId = brokerApi.place_order(tradingsymbol=tradeSymbol,
                                        exchange=brokerApi.EXCHANGE_NFO,
                                        transaction_type=brokerApi.TRANSACTION_TYPE_SELL,
                                        quantity=quantity,
                                        order_type=brokerApi.ORDER_TYPE_LIMIT,
                                        product=brokerApi.PRODUCT_NRML,
                                        variety=brokerApi.VARIETY_REGULAR,
                                        price=triggerPrice, 
                                        validity='DAY')

            orderRemarks = "ORDER PLACED"
           
        except Exception as e:            
            orderRemarks = str(e)
            orderId = -1
            
            if (re.search('Read timed out', str(e))):
                orderRemarks = "TIMED OUT"
                orderId = -2

        return orderId, orderRemarks
        
    elif(broker == "PROSTOCKS"):
        try: 
            prdType = 'C'
            if (exchange == 'NFO'):
                prdType = 'M'
            else:
                tradeSymbol = (tradeSymbol  + "-EQ").replace('&', '%26')
                exchange = 'NSE'
                

            data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\""+exchange+"\", \"tsym\":\""+tradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(0)+"\", \"prd\":\""+prdType+"\", \"trantype\":\"S\", \"prctyp\":\"MKT\", \"ret\":\"DAY\"}&jKey=" + brokerApi        

            response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
            json_response = response.json()

            if (json_response['stat'] == 'Ok'):  
                orderId = json_response['norenordno']
                return orderId, 'ORDER PLACED'                
            else:
                orderRemarks =  json_response['emsg']                
                if (re.search('Session Expired', str(orderRemarks))):
                    return -3, orderRemarks
                else:
                    return -1, orderRemarks

        except Exception as e:
            logging.info('Sell order failed for ' + str(tradeSymbol) +' with error ' + str(e))                
            return -1, str(e)



def modifyFnoSellOrder(kiteFno, fnoOrderId, fnoSellPrice):
    orderId = 0
    try:        
        orderId = kiteFno.modify_order(kiteFno.VARIETY_REGULAR,fnoOrderId,price=fnoSellPrice)        
        logging.info("FNO MODIFY ORDER PLACED ID IS: " + str(orderId))
        print("FNO MODIFY ORDER PLACED ID IS: " + str(orderId))
    except Exception as e:
        logging.info("FNO MODIFY ORDER FAILED: " + str(e))
        print("FNO MODIFY ORDER FAILED: " + str(e))
        orderId = 0
    logging.info(
        "*******************************************************")
    return orderId


def placeExitOrderFNO(kite, transactionType, tradeSymbol, quantity,fnoSellPrice):
    try:
        orderId = kite.place_order(tradingsymbol=tradeSymbol,
                                   exchange=kite.EXCHANGE_NFO,
                                   transaction_type=transactionType,
                                   quantity=quantity,
                                   order_type=kite.ORDER_TYPE_LIMIT,
                                   product=kite.PRODUCT_NRML,
                                   variety=kite.VARIETY_REGULAR,
                                   price=fnoSellPrice, 
                                   validity='DAY')
        # orderId = 1
        orderRemarks = "ORDER PLACED"

        logging.info(transactionType + " ORDER PLACED ID IS: " + str(orderId))
        print(transactionType + " ORDER PLACED ID IS: " + str(orderId))
    except Exception as e:
        logging.info(transactionType + " ORDER PLACEMENT FAILED: " + str(e))
        print(transactionType + " ORDER PLACEMENT FAILED: " + str(e))
        orderRemarks = str(e)
        orderId = 0
    logging.info(
        "*******************************************************")
    return orderId, orderRemarks


def getKiteHoldings(kite):
    holdings = kite.holdings()
    jsonData = baf.get_json_response(holdings)
    return jsonData

def getKiteMargins(kite):
    margins = kite.margins(segment=kite.MARGIN_EQUITY)
    jsonData = baf.get_json_response(margins)
    return jsonData

def getKitePositions(kite):
    positions = kite.positions()
    jsonData = baf.get_json_response(positions)
    return jsonData


def getKiteOrders(kite):
    orders = kite.orders()
    jsonData = baf.get_json_response(orders)
    return jsonData


def getKiteGTTOrders(kite):
    gttOrders = kite.get_gtts()
    jsonData = baf.get_json_response(gttOrders)
    return jsonData

def get_ltp(kite,instrumentToken):
    try:
        ltpOrder = kite.ltp(instrumentToken)
        jsonData = baf.get_json_response(ltpOrder)
        return jsonData  
    except:        
        try:
            time.sleep(1)
            ltpOrder = kite.ltp(instrumentToken)
            jsonData = baf.get_json_response(ltpOrder)
            return jsonData
        except:
            time.sleep(2)            
            logging.info("Trying LTP after 3 seconds")
            ltpOrder = kite.ltp(instrumentToken)
            jsonData = baf.get_json_response(ltpOrder)
            return jsonData


def placeCancelOrder(kite, orderId):
    kite.cancel_order(variety=kite.VARIETY_REGULAR,
                      order_id=orderId, parent_order_id=None)

def getGTTOrder(kite,triggerId):
    orders = kite.get_gtt(triggerId)
    jsonData = baf.get_json_response(orders)
    return jsonData                                    

def get_json_response(jsonData):
    # Serializing json
    jsonData = json.dumps(jsonData, indent=4, default=json_util.default)
    return json.loads(jsonData)

# get kite profile 
def get_profile(kite):
    orders = kite.profile()
    jsonData = get_json_response(orders)
    return jsonData       

def getQuote(kite,instrumentToken):
    orders = kite.quote(instrumentToken)
    jsonData = baf.get_json_response(orders)
    return jsonData  

def get_quote(kite, instrumentToken):
    try:
        orders = kite.quote(instrumentToken)
        jsonData = baf.get_json_response(orders)
        return jsonData  
    except:        
        try:
            time.sleep(1)
            orders = kite.quote(instrumentToken)
            jsonData = baf.get_json_response(orders)
            return jsonData
        except:
            time.sleep(2)
            logging.info("Trying Quote after 3 seconds")
            orders = kite.quote(instrumentToken)
            jsonData = baf.get_json_response(orders)
            return jsonData


def deleteGTTOrder(kite,triggerId):
    orders = kite.delete_gtt(triggerId)
    jsonData = baf.get_json_response(orders)
    return jsonData       

# Get the list of instruments traded in Zerodha
def getInstruments(kite):
    try:
        insList=kite.instruments(exchange=kite.EXCHANGE_NSE)
        df = pd.DataFrame(insList)
        df.to_csv('kite_instruments.csv')
        return True
    except Exception as e:
        print(e)
        return False

def checkExistingOrders(kite, instrumentToken):
    orderData = baf.getKiteOrders(kite)
    orderExistFlag = False
    tagId = ""
    orderId = 0
    for orders in orderData:
        if ((str(orders["instrument_token"]) == str(instrumentToken)) and 
                (str(orders["status"]) in ("OPEN","OPEN PENDING","VALIDATION PENDING","PUT ORDER REQ RECEIVED","MODIFIED","MODIFY PENDING","MODIFY VALIDATION PENDING","TRIGGER PENDING","AMO REQ RECEIVED")) and
                    (str(orders['product']) == "NRML") and
                        (str(orders['transaction_type']) == "SELL")):
            orderExistFlag = True
            orderId = orders["order_id"]
            tagId = str(orders["tag"])
            break
        elif ((str(orders["instrument_token"]) == str(instrumentToken)) and 
                (str(orders["status"]) in ("OPEN","OPEN PENDING","VALIDATION PENDING","PUT ORDER REQ RECEIVED","MODIFIED","MODIFY PENDING","MODIFY VALIDATION PENDING","TRIGGER PENDING","AMO REQ RECEIVED")) and
                    (str(orders['product']) == "CNC") and 
                        (str(orders['transaction_type']) == "SELL")):
            orderExistFlag = True
            orderId = orders["order_id"]
            tagId = str(orders["tag"])
            break        
    return orderExistFlag,tagId,orderId

def exitFNO(kite, tradeSymbol, transactionType, quantity,fnoSellPrice):
    logging.info("place the " + transactionType + " order now")   

    orderId = baf.placeExitOrderFNO(kite, transactionType, tradeSymbol, quantity,fnoSellPrice)

    if (orderId != 0):
        logging.info("Successfully placed Sell Order for FNO")
    else:
        logging.info("Failed to place FNO " + str(orderId))


def connect_prostocks_api(mySQLCursor, accountId):
    try: 
        selectStatment = "SELECT API_PIN, PASSWORD, API_KEY, ACCESS_TOKEN, ACCESS_TOKEN_VALID_FLAG, DATE(UPDATED_ON), REQUEST_TOKEN FROM USR_TRADE_ACCOUNTS WHERE TRADE_ACCOUNT = '"+ str(accountId)+"'"
        mySQLCursor.execute(selectStatment)
            # gets the number of rows affected by the command executed
        results = mySQLCursor.fetchall()    
            
        apiKey = ''
        apiPin = ''
        password = ''

        for row in results:
            apiPin = row[0]
            password = row[1]
            apiKey = row[2]
            vendorKey = row[6]
                    
        data = """jData={"apkversion":"1.0.0", "uid":\""""+ str(accountId) +"""\", "pwd":\""""+password+"""\", "factor2":\""""+apiPin+"""\", "imei":"ag3tbbbb33", "source":"API", "vc":\""""+ str(vendorKey) +"""\", "appkey":\""""+ apiKey +"""\"}"""
        response = requests.post('https://starapi.prostocks.com/NorenWClientTP/QuickAuth', data=data)
        json_response = response.json()
        accessToken = json_response['susertoken']  
        isConnected = True
        return accessToken, isConnected
    except Exception as e:
        logging.info("Can't connect to prostocks API for account id" + str(e))
        return "", False
    

# connect to broker account api via account id
def connect_broker_api(cnx, mySQLCursor, accountId, broker, uatFlag = False):

    if (broker == 'ZERODHA' or broker is None):
        # Check if we have any valid existing keys in database; if found connect it with existing keys
        kite = baf.connect_existing_api_keys(cnx, mySQLCursor, accountId, broker)

        isKiteConnected = baf.validate_access_token(kite, cnx, mySQLCursor,accountId)
        
        if (not(isKiteConnected)):
            # Invalid redirect token found, so create it new with the selanium automation
            autoKeySuccessflag = baf.auto_get_access_token(cnx, mySQLCursor, accountId)

            # Connect to Kite Again
            kite = baf.connect_existing_api_keys(cnx, mySQLCursor, accountId, broker)
            isKiteConnected = baf.validate_access_token(kite, cnx, mySQLCursor,accountId)
            
            if (not(isKiteConnected)):
                # Invalid redirect token found, so create it new with the selanium automation
                autoKeySuccessflag = baf.auto_get_access_token(cnx, mySQLCursor, accountId)

                # Connect to Kite Again
                kite = baf.connect_existing_api_keys(cnx, mySQLCursor, accountId, broker)
                isKiteConnected = baf.validate_access_token(kite, cnx, mySQLCursor,accountId)


        return kite, isKiteConnected
    elif (broker == 'PROSTOCKS'):
        accessToken  = baf.connect_existing_api_keys(cnx, mySQLCursor, accountId, broker, uatFlag)
        isConnected = baf.validate_prostocks_access_token(accessToken, cnx, mySQLCursor,accountId, uatFlag) 
        return accessToken, isConnected


def connect_existing_api_keys(cnx, mySQLCursor, accountId, broker, uatFlag=False):
    createdOn = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
    currDate = util.get_date_time_formatted("%Y-%m-%d")         
    if (broker == 'ZERODHA' or broker is None):        
        selectStatment = "SELECT API_KEY, API_SECRET, REQUEST_TOKEN, ACCESS_TOKEN, ACCESS_TOKEN_VALID_FLAG, DATE(UPDATED_ON) FROM USR_TRADE_ACCOUNTS WHERE TRADE_ACCOUNT = '"+ str(accountId)+"'"
        mySQLCursor.execute(selectStatment)
        # gets the number of rows affected by the command executed
        results = mySQLCursor.fetchall()    
        
        apiKey = ''
        apiSecret = ''
        redirectToken = ''
        accessToken = ''
        updatedOn = ''
        accessTokenValidFlag = 'N'
        for row in results:
            apiKey = row[0]
            apiSecret = row[1]
            redirectToken = row[2]
            accessToken = row[3]
            accessTokenValidFlag = row[4]
            updatedOn = row[5]

        if (redirectToken is None or redirectToken == ''):
            redirectToken = 'CvdGMQUcxGYiodYpHFigv2lmbTT0x8x1'
        
        kite = KiteConnect(api_key=apiKey)
        
        # if ((currDate != updatedOn.strftime("%Y-%m-%d") and str(accessTokenValidFlag) != '1') or (accessToken is None or accessToken == '')):
        if ((currDate != updatedOn.strftime("%Y-%m-%d") or str(accessTokenValidFlag) == 'N')):
            try:            
                # The below line throws exception when a valid access token was generated already for same redirect token
                # It also throws exception when invalid or expired redirect token found
                data = kite.generate_session(redirectToken, api_secret=apiSecret)
                accessToken = data["access_token"]
                updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET ACCESS_TOKEN ='" + str(accessToken) + "' WHERE TRADE_ACCOUNT = '"+ str(accountId)+"'")
                mySQLCursor.execute(updateQuery)
                cnx.commit() 
            except:
                print("Invalid Token Found; Regenerate it")
        
        kite.set_access_token(accessToken)
        return kite

    elif (broker == 'PROSTOCKS'):

        selectStatment = "SELECT API_PIN, PASSWORD, API_KEY, ACCESS_TOKEN, ACCESS_TOKEN_VALID_FLAG, DATE(UPDATED_ON), REQUEST_TOKEN FROM USR_TRADE_ACCOUNTS WHERE TRADE_ACCOUNT = '"+ str(accountId)+"'"
        mySQLCursor.execute(selectStatment)
            # gets the number of rows affected by the command executed
        results = mySQLCursor.fetchall()    

        apiKey = ''
        apiPin = ''
        password = ''
        accessTokenValidFlag = 'N'
        updatedOn = ''
        accessToken = ''

        for row in results:
            apiPin = row[0]
            password = row[1]
            apiKey = row[2]
            accessToken = row[3]
            accessTokenValidFlag = row[4]
            updatedOn = row[5]
            vendorKey = row[6]
        
        if ((currDate != updatedOn.strftime("%Y-%m-%d") or str(accessTokenValidFlag) == 'N')):
            data = """jData={"apkversion":"1.0.0", "uid":\""""+ str(accountId) +"""\", "pwd":\""""+password+"""\", "factor2":\""""+apiPin+"""\", "imei":"ag3tbbbb33", "source":"API", "vc":\""""+vendorKey+"""\", "appkey":\""""+ apiKey +"""\"}"""
            if uatFlag: 
                response = requests.post('https://staruat.prostocks.com/NorenWClientTP/QuickAuth', data=data)
            else:
                response = requests.post('https://starapi.prostocks.com/NorenWClientTP/QuickAuth', data=data)
            json_response = response.json()
            accessToken = json_response['susertoken']  

            updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET UPDATED_ON='" + str(createdOn) + "', ACCESS_TOKEN ='" + str(accessToken) + "' WHERE TRADE_ACCOUNT = '"+ str(accountId)+"'")
            mySQLCursor.execute(updateQuery)
            cnx.commit() 
        
        return accessToken

# check kite connected or not
def validate_prostocks_access_token(accessToken, cnx, mySQLCursor, accountId, uatFlag=False):
    try:
        updatedOn = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')       
        data = "jData={\"uid\":\""+ str(accountId) +"\"}&jKey=" + accessToken        
        
        if uatFlag:
            response = requests.post('https://staruat.prostocks.com/NorenWClientTP/UserDetails', data=data)
        else:
            response = requests.post('https://starapi.prostocks.com/NorenWClientTP/UserDetails', data=data)
        responseJson = response.json()

        if (responseJson['stat'] == 'Not_Ok'):
            updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET ACCESS_TOKEN_VALID_FLAG = 'N', UPDATED_ON='" + str(updatedOn) + "' WHERE TRADE_ACCOUNT = '"+str(accountId)+"'") 
            mySQLCursor.execute(updateQuery) 
            cnx.commit()  
            return False
        else:
            updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET ACCESS_TOKEN_VALID_FLAG = 'Y', UPDATED_ON='" + str(updatedOn) + "' WHERE TRADE_ACCOUNT = '"+str(accountId)+"'") # ACCESS_TOKEN_VALID_FLAG='N' update when error occurs 
            mySQLCursor.execute(updateQuery)
            cnx.commit()              
            return True        

    except Exception as e:
        logging.info("ERROR OCCURED WHILE TRYING TO CONNECT PROSTOCKS: Invalid Token" + str(e))        
        updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET ACCESS_TOKEN_VALID_FLAG = 'N', UPDATED_ON='" + str(updatedOn) + "' WHERE TRADE_ACCOUNT = '"+str(accountId)+"'") # ACCESS_TOKEN_VALID_FLAG='N' update when error occurs 
        mySQLCursor.execute(updateQuery)
        cnx.commit()  
        return False

     
# check kite connected or not
def validate_access_token(kite, cnx, mySQLCursor, accountId):
    try:
        updatedOn = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
        profileData = baf.get_profile(kite)   # get profile in json format     
        updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET ACCESS_TOKEN_VALID_FLAG = 'Y', UPDATED_ON='" + str(updatedOn) + "' WHERE TRADE_ACCOUNT = '"+str(accountId)+"'") 
        mySQLCursor.execute(updateQuery) # ACCESS_TOKEN_VALID_FLAG='Y' update when kite have profile 
        cnx.commit()  
        return True
    except Exception as e:
        logging.info("ERROR OCCURED WHILE TRYING TO CONNECT KITE: Invalid Token" + str(e))        
        updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET ACCESS_TOKEN_VALID_FLAG = 'N', UPDATED_ON='" + str(updatedOn) + "' WHERE TRADE_ACCOUNT = '"+str(accountId)+"'") # ACCESS_TOKEN_VALID_FLAG='N' update when error occurs 
        mySQLCursor.execute(updateQuery)
        cnx.commit()  
        return False


def auto_get_access_token(cnx, mySQLCursor, accountId):
    selectStatment = "SELECT API_KEY, PASSWORD, API_PIN FROM USR_TRADE_ACCOUNTS WHERE TRADE_ACCOUNT = '"+ str(accountId) +"'"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()    
    # gets the number of rows affected by the command executed
    apiKey = ''
    password = ''
    apiPin = ''

    for row in results:
        apiKey = row[0]
        password = row[1]
        apiPin = row[2]
    if (platform.system() == 'Windows'):
        driver = webdriver.Chrome('D:\\alphagain_development\\core\\utils\\external\\windows\\chromedriver.exe')  # Optional argument, if not specified will search path.driver = webdriver.Chrome('helpers\\chromedriver.exe')  # Optional argument, if not specified will search path.
        driver.get("https://kite.trade/connect/login?api_key="+str(apiKey)+"&v=3")
    else:   
        options = Options()         
        options.add_argument("--disable-dev-shm-usage") 
        options.add_argument("--no-sandbox")  
        options.add_argument('headless')

        caps = DesiredCapabilities.CHROME
        caps['loggingPrefs'] = {'performance': 'ALL'}
        driver = webdriver.Chrome(options=options, desired_capabilities=caps, executable_path='/root/install/chromedriver')
        
        driver.get("https://kite.trade/connect/login?api_key="+str(apiKey)+"&v=3")

    time.sleep(5) # Let the user actually see something!
    accountIdBox = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input')
    accountIdBox.send_keys(accountId)
    passwordBox = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input')
    passwordBox.send_keys(password)
    submitButton = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button')
    submitButton.click()
    time.sleep(5) 
    pinBox = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/div/input')
    pinBox.send_keys(apiPin)
    pinSubmitButton = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[3]/button')
    pinSubmitButton.click()
    time.sleep(5) 
    
    try:
        authorizeButton = driver.find_element_by_xpath('/html/body/div[1]/div/div[1]/div/div/div[3]/button')
        authorizeButton.click()
        time.sleep(5)
    except:
        pass

    requestURL = driver.current_url
    driver.quit()
    parsed = urlparse.urlparse(requestURL)
    status = parse_qs(parsed.query)['status'][0]
    updatedOn = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
    
    if (status == 'success'):
        redirectToken = parse_qs(parsed.query)['request_token'][0]
        try:
            # with open('redirect_token_ME7995.txt', 'w') as redirectTokenFile:
                # redirectTokenFile.write(redirectToken)
            updateQuery ="UPDATE USR_TRADE_ACCOUNTS SET REQUEST_TOKEN = '" + str(redirectToken) + "', UPDATED_ON='" + str(updatedOn) + "' WHERE TRADE_ACCOUNT = '"+ str(accountId) +"'"
            mySQLCursor.execute(updateQuery)
            cnx.commit()  
            return True
        except:
            print('Unable to update the access key')
            return False

    else:
        print('Can\'t get the access key')
        return False