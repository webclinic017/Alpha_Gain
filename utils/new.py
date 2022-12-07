
def update_order_status(kite, accountId, cnx, mySQLCursor):
    response = {}
    try: 
        orderData = baf.getKiteOrders(kite)

        for orders in orderData:

            transactionType = orders["transaction_type"]
            updateOn = orders["exchange_update_timestamp"]
            try:
                if (updateOn is None or updateOn == 'None'):
                    updateOn = int(orders["order_timestamp"]['$date']) / 1000
                    updateOn = datetime.datetime.utcfromtimestamp(updateOn).strftime("%Y-%m-%d %H:%M:%S")
            except:
                updateOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S") 

                
            orderId = orders["order_id"]
            orderPrice = orders["average_price"]
            filledQuantity = int(orders['filled_quantity'])
            pendingQuantity = int(orders['pending_quantity'])

            orderStatus = orders["status"]            
            orderRemarks = orders["status_message"]
            quantity = int(orders["quantity"])

            buyValue = quantity * float(orderPrice)
            updateQuery = ""

            if (transactionType == 'BUY'):
                if (orderStatus == 'COMPLETE'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'OPEN' AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount
                    if rowCount == 0:
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET BUY_ORDER_PRICE="+str(orderPrice) + ", TRADE_STATUS='OPEN', QUANTITY="+str(quantity)+", BUY_ORDER_STATUS='COMPLETE', BUY_ORDER_DATE='" +
                                str(updateOn)+"', BUY_VALUE='"+str(buyValue)+"' WHERE BUY_ORDER_ID='"+str(orderId) + "' AND TRADE_STATUS NOT IN ('OPEN','EXITED')")


                elif (orderStatus == 'OPEN' and filledQuantity > 0 and pendingQuantity > 0):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'P-OPEN' AND QUANTITY="+str(filledQuantity)+" AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:
                        buyValue = filledQuantity * float(orderPrice)                
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET ORDER_REMARKS ='PARTIALLY EXECUTED', BUY_ORDER_PRICE="+str(orderPrice) + ", TRADE_STATUS='P-OPEN', BUY_ORDER_STATUS='P-COMPLETE', \
                            BUY_ORDER_DATE='" +str(updateOn)+ "', BUY_VALUE='"+str(buyValue)+"', QUANTITY="+str(filledQuantity)+" WHERE BUY_ORDER_ID='"+str(orderId) + "'")       

                elif (orderStatus == 'CANCELLED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'CANCELLED' AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:                        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET ORDER_REMARKS ='Buy Order is cancelled by either Zerotha or User', BUY_ORDER_STATUS='CANCELLED', TRADE_STATUS='CANCELLED' WHERE BUY_ORDER_ID='"+str(orderId) + "'")
                
                elif (orderStatus == 'REJECTED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'REJECTED' AND BUY_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:                        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET TRADE_STATUS='REJECTED', BUY_ORDER_STATUS='REJECTED', ORDER_REMARKS ='" +
                                                    str(orderRemarks) + "' WHERE BUY_ORDER_ID='"+str(orderId) + "'")


            elif(transactionType == 'SELL'):
                if (orderStatus == 'COMPLETE'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS = 'EXITED' AND SELL_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                
                
                    if rowCount == 0:
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_PRICE="+str(orderPrice) + ", SELL_ORDER_STATUS='COMPLETE', TRADE_STATUS='EXITED', SELL_ORDER_DATE='"+str(updateOn)+"', PROFIT_PERCENT=(("+str(float(orderPrice))+" - BUY_ORDER_PRICE) / BUY_ORDER_PRICE) * 100, \
                            PROFIT_AMOUNT=("+str(float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY), TRADE_RESULT = IF ((("+str(float(orderPrice))+" * QUANTITY) - (BUY_ORDER_PRICE * QUANTITY)) > 0, 'GAIN','LOSS') WHERE SELL_ORDER_ID ='"+str(orderId)+"'")

                elif(orderStatus == 'REJECTED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE SELL_ORDER_STATUS = 'REJECTED' AND SELL_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:                        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_STATUS = 'REJECTED', SELL_ORDER_DATE='"+str(updateOn)+"' WHERE SELL_ORDER_ID ='"+str(orderId)+"'")
                
                elif (orderStatus == 'CANCELLED'):
                    selectStatment = "SELECT TRADE_STATUS FROM TRADE_TRANSACTIONS WHERE SELL_ORDER_STATUS = 'CANCELLED' AND SELL_ORDER_ID='"+str(orderId)+"'"
                    mySQLCursor.execute(selectStatment)
                    rowCount = mySQLCursor.rowcount                    
                    if rowCount == 0:                        
                        updateQuery = ("UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_REMARKS ='Sell Order is cancelled by either Zerotha or User', SELL_ORDER_STATUS='CANCELLED' WHERE SELL_ORDER_ID='"+str(orderId) + "'")
                

            if (updateQuery != ""):
                mySQLCursor.execute(updateQuery)
                cnx.commit()

        response['status'] = 'success'
        response['remarks'] = 'Successfully updated the orders from Zerodha for account: ' + str(accountId)
            
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to complete the orders update for Zerodha for account: ' + str(accountId)
    
    return response
