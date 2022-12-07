from utils import util_functions as util
from utils import broker_api_functions as baf
import requests
import logging
import datetime
import os
from config import Config



def update_last_mkt_date(cnx, mySQLCursor):
    currDate = util.get_date_time_formatted("%Y-%m-%d")
    holidayFlag = util.isItHoliday(mySQLCursor, currDate)
    day = util.get_day_of_date(currDate)
    
    response = {}
    try:
        if (day != 'Saturday' and day != 'Sunday' and not(holidayFlag)):
            updateQuery = ("UPDATE LAST_TRADING_DATE SET TRADING_DATE = '"+ str(currDate)+ "' WHERE AUTO_ID = 1")
            mySQLCursor.execute(updateQuery)
            cnx.commit()        
            response['status'] = 'success'
            response['remarks'] = 'updated'
        else:        
            response['status'] = 'failed'
            response['remarks'] = 'It\'s either holiday or weekend, so not updated the last trading date'
    
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to update the last trading date' + str(e)

    return response

def clear_sell_order_status(cnx, mySQLCursor): 
    response = {}
    try:
        updateQuery = "UPDATE TRADE_TRANSACTIONS SET SELL_ORDER_STATUS = '' WHERE SELL_ORDER_STATUS IN ('REJECTED','ABANDONED')"
        mySQLCursor.execute(updateQuery)
        cnx.commit()
        response['status'] = 'success'
        response['remarks'] = 'Sell order status is cleared'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to reset the sell order status' + str(e)

    return response

# Copy the current close price to prvious day's price; this will be used to calculate price changes of major indices
def copy_close_price_to_previous(cnx, mySQLCursor): 
    response = {}
    try:
        updateQuery = "UPDATE LAST_TRADED_INDICES_PRICE SET PREV_CLOSE_PRICE = CLOSE_PRICE"
        mySQLCursor.execute(updateQuery)
        cnx.commit()
        response['status'] = 'success'
        response['remarks'] = 'Copied current close price to previous day\'s close price'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to copy current close price to previous day\'s close price. The error is, ' + str(e)

    return response

def copy_close_price_to_previous_trades(cnx, mySQLCursor): 
    response = {}
    try:
        updateQuery = "UPDATE TRADE_TRANSACTIONS SET PREV_CLOSE_PRICE = CURRENT_MKT_PRICE, TODAY_PROFIT=0, TODAY_PROFIT_PCT=0 WHERE TRADE_STATUS = 'OPEN'"
        mySQLCursor.execute(updateQuery)
        cnx.commit()
        response['status'] = 'success'
        response['remarks'] = 'Copied current close price to previous day\'s close price'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to copy current close price to previous day\'s close price. The error is, ' + str(e)

    return response


# Delete all the pending and open signals at the end of day.
def delete_pending_signals(cnx, mySQLCursor):
    try:
        sql = "DELETE FROM TRADE_SIGNALS WHERE SIGNAL_STATUS IN ('PENDING', 'OPEN')"
        mySQLCursor.execute(sql)
        cnx.commit()
    except Exception as e:
        logging.info("DB FAILURE: UNABLE TO DELETE DATA FROM TRADE_SIGNALS : "+str(e))



def insert_blank_inst_market_updates(cnx, mySQLCursor):
    response = {}
    try:
        currDate = util.get_date_time_formatted("%Y-%m-%d")
        cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)

        insertQuery = "INSERT INTO MARKET_PERFORMANCE_TBL(DATE, INSTRUMENT_TOKEN, TRADING_SYMBOL, BENCHMARK_INDEX, MARKET_CAP, \
                        SECTOR, INDUSTRY, FNO, STOCK_NAME, CATEGORY, UPDATED_ON,EXCHANGE_TOKEN) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        
        # Fetch list of all instruments from stock universe for the provided category
        selectStatment = "SELECT INSTRUMENT_TOKEN, TRADINGSYMBOL, BENCHMARK_INDEX, MARKET_CAP, SECTOR, INDUSTRY, FNO, NAME, CATEGORY, EXCHANGE_TOKEN FROM \
                            STOCK_UNIVERSE WHERE CATEGORY IN ('Market Cap','Sector','Stocks') ORDER BY TRADINGSYMBOL"
        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()    
        
        insertArrayValues = []
        tmpCnt = 0
        errorFlag = 0
        
        for row in results:
            INSTRUMENT_TOKEN = row[0]
            TRADINGSYMBOL = row[1]
            BENCHMARK_INDEX = row[2]
            MARKET_CAP = row[3]
            SECTOR = row[4]
            INDUSTRY = row[5]
            FNO = row[6]
            STOCK_NAME = row[7]
            CATEGORY = row[8]
            EXCHANGE_TOKEN = row[9]
            
            updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
            insertVal = []
            insertVal.insert(0, str(currDate))
            insertVal.insert(1, str(INSTRUMENT_TOKEN))
            insertVal.insert(2, str(TRADINGSYMBOL))
            insertVal.insert(3, str(BENCHMARK_INDEX))
            insertVal.insert(4, str(MARKET_CAP))
            insertVal.insert(5, str(SECTOR))
            insertVal.insert(6, str(INDUSTRY))
            insertVal.insert(7, str(FNO))
            insertVal.insert(8, str(STOCK_NAME))
            insertVal.insert(9, str(CATEGORY))
            insertVal.insert(10, str(updatedOn))
            insertVal.insert(11, str(EXCHANGE_TOKEN))

            insertArrayValues.insert(tmpCnt, insertVal)
            tmpCnt += 1
            try: 
                if (tmpCnt == 100):                                                
                    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)
                    mySQLCursor.executemany(insertQuery, insertArrayValues)
                    cnx.commit()                                  
            
                    insertArrayValues = []
                    tmpCnt = 0   
                                        
            except Exception as e:        
                errorFlag = 1            
                response['status'] = 'error'
                response['remarks'] = 'Unable to create blank records for all instruments in market performance table. The error is, ' + str(e)
                pass

        if (tmpCnt > 0):               
            cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)
            mySQLCursor.executemany(insertQuery, insertArrayValues)
            cnx.commit()             
        
    except Exception as e:    
        errorFlag = 1
        response['status'] = 'error'
        response['remarks'] = 'Unable to create blank records for all instruments in market performance table. The error is, ' + str(e)

    if (errorFlag == 0):
        response['status'] = 'success'
        response['remarks'] = 'Successfully created blank records in market performance table'

    return response


# extract all the instrucments traded in NFO segment
def update_fno_instruments(cnx, mySQLCursor):

    response = {}
    try:
        # Insert query for adding instruments list
        insertQuery = "INSERT INTO INSTRUMENTS (instrument_token, exchange_token, tradingsymbol, name, last_price, expiry, strike, tick_size, lot_size, instrument_type, segment, exchange) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        
        # Get the instruments list from Zerodha website
        req = requests.get('https://api.kite.trade/instruments')
        iCnt = 0
        insertVal = []
        
        # Before inserting the update instruments list, delete all the instruments first  
        deleteQuery = "DELETE FROM INSTRUMENTS"
        mySQLCursor.execute(deleteQuery)
        cnx.commit()

        # Iterates over the response data, one line at a time.
        for row in req.iter_lines():
            instrumentList= str(row.decode('utf-8'))
            instrumentList = instrumentList.replace("\"","")
            instrumentList = instrumentList.strip('\n').split(',')

            exchange = instrumentList[11]

            if (exchange == 'NFO'):
                insertVal.insert(iCnt, instrumentList)            
                iCnt += 1
                if (iCnt == 500):
                    # Verify whether the connection to MySQL database is open
                    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)                          
                    mySQLCursor.executemany(insertQuery, insertVal)
                    cnx.commit()
                    iCnt = 0
                    insertVal = []                    
                  

        if (iCnt > 0):           
            # Verify whether the connection to MySQL database is open
            cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)             
            mySQLCursor.executemany(insertQuery, insertVal)
            cnx.commit() 
                      
        response['status'] = 'success'
        response['remarks'] = 'INSTRUMENTS table has been updated'

    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to delete and add instruments list in INSTRUMENTS table. The error is, ' + str(e)
    
    return response

# Main function is called by default, and the first function to be executed
if __name__ == "__main__":   # __name__ is a built-in variable which evaluates to the name of the current module 
    
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
    currDate = util.get_date_time_formatted("%Y-%m-%d")

    # Connect to Kite ST
    kite, isKiteConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = adminTradeBroker) 

    try: 
        toDate = util.get_lookup_date(0)
        day = (datetime.datetime.strptime(toDate, '%Y-%m-%d')).strftime("%A")

        if (day != 'Saturday' and day != 'Sunday'):
            # Delete log files on monday
            # if (day == 'Monday'):
            #     if os.path.exists('/apps/core/logs'):
            #         os.remove('/apps/core/logs/*.*')
            #         os.remove('/apps/core/logs/nohup/*.*')

            response = update_fno_instruments(cnx, mySQLCursor)             
            util.add_logs(cnx, mySQLCursor, 'UPDATE',  response['remarks'], sysDict)

            # Update the market 
            response = update_last_mkt_date(cnx, mySQLCursor)
            util.add_logs(cnx, mySQLCursor, 'UPDATE',  response['remarks'], sysDict)
            # Clear the sell order status where status is set as REJECTED and ABANDONED
            response = clear_sell_order_status(cnx, mySQLCursor)
            util.add_logs(cnx, mySQLCursor, 'UPDATE',  response['remarks'], sysDict)

            # Copy the current close price to prvious day's price; this will be used to calculate price changes of major indices
            response = copy_close_price_to_previous(cnx, mySQLCursor)
            util.add_logs(cnx, mySQLCursor, 'ALERT',  response['remarks'], sysDict)

            # Copy the current close price to prvious day's price; this will be used to calculate price changes of major indices
            response = copy_close_price_to_previous_trades(cnx, mySQLCursor)
            util.add_logs(cnx, mySQLCursor, 'ALERT',  response['remarks'], sysDict)

            response = insert_blank_inst_market_updates(cnx, mySQLCursor)
            util.add_logs(cnx, mySQLCursor, 'UPDATE',  response['remarks'], sysDict)

    except Exception as e:
        alertMsg = 'Unable to update last market date: '+ str(e)
        util.add_logs(cnx, mySQLCursor, 'ERROR', alertMsg, sysDict)

        
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)

    util.update_program_running_status(cnx, mySQLCursor, programName, 'INACTIVE')
    util.add_logs(cnx, mySQLCursor, 'UPDATE',  "Program ended", sysDict)
    util.disconnect_db(cnx, mySQLCursor)