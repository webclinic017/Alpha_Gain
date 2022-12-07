from utils import util_functions as util
from NorenRestApiPy.NorenApi import NorenApi
import logging
import datetime, time
from datetime import timedelta

api = None
class StarApiPy(NorenApi):
    
    def __init__(self, *args, **kwargs):
        super(StarApiPy, self).__init__(host='https://starapi.prostocks.com/NorenWClientTP', websocket='wss://starapi.prostocks.com/NorenWS/', eodhost='https://star.prostocks.com/chartApi/getdata')
        global api
        api = self


def validate_prostocks_connection(brokerApi, cnx, mySQLCursor, accountId):
    try:
        updatedOn = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')       
        responseData = brokerApi.get_limits()

        if (responseData['stat'] == 'Not_Ok'):
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

def connect_prostocks_api(cnx, mySQLCursor, accountId):
    apiSession = None
    try: 
        createdOn = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')
        currDate = util.get_date_time_formatted("%Y-%m-%d")         
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
        
        apiSession = StarApiPy()
        if ((currDate != updatedOn.strftime("%Y-%m-%d") or str(accessTokenValidFlag) == 'N')):
            # data = """jData={"apkversion":"1.0.0", "uid":\""""+ str(accountId) +"""\", "pwd":\""""+password+"""\", "factor2":\""""+apiPin+"""\", "imei":"ag3tbbbb33", "source":"API", "vc":\""""+vendorKey+"""\", "appkey":\""""+ apiKey +"""\"}"""
            apiResponse = apiSession.login(userid = accountId, password = password, twoFA=apiPin, vendor_code=vendorKey, api_secret=apiKey, imei='ag3tbbbb33')
            if apiResponse != None:
                # response = requests.post('https://starapi.prostocks.com/NorenWClientTP/QuickAuth', data=data)
                accessToken = apiResponse['susertoken']
                updateQuery = ("UPDATE USR_TRADE_ACCOUNTS SET UPDATED_ON='" + str(createdOn) + "', ACCESS_TOKEN ='" + str(accessToken) + "' WHERE TRADE_ACCOUNT = '"+ str(accountId)+"'")
                mySQLCursor.execute(updateQuery)
                cnx.commit()
        else:
            apiSession.set_session(accountId,password,accessToken)

    except Exception as e:
        logging.info(f"Can't connect to prostocks API for account id {str(e)}")

    return apiSession
    

# connect to broker account api via account id
def connect_broker_api(cnx, mySQLCursor, accountId, broker):
    if (broker == 'PROSTOCKS'):
        apiSession  = connect_prostocks_api(cnx, mySQLCursor, accountId)
        isConnected  = True
        # isConnected = validate_prostocks_connection(apiSession, cnx, mySQLCursor, accountId)
        return apiSession, isConnected

def get_lookup_date(noOfDays, dateNow = None):
    # get date before current date
    if (dateNow == None):
        return (datetime.datetime.now() - datetime.timedelta(days=noOfDays)).strftime("%d-%m-%Y")
    else:
        return (datetime.datetime.strptime(dateNow, '%Y-%m-%d') - datetime.timedelta(days=noOfDays)).strftime("%d-%m-%Y")

def get_historical_data(brokerApi, tradeSymbol, exchangeToken, exchange, fromDate, toDate, interval):
    try:
        if (interval == 'day'):
            fromDate = time.strptime(fromDate, '%d-%m-%Y %H:%M:%S')
            fromDate = time.mktime(fromDate)
            
            toDate = toDate + ' 23:00:00'
            toDate = time.strptime(toDate, '%d-%m-%Y %H:%M:%S')
            toDate = time.mktime(toDate)

            return brokerApi.get_daily_price_series(exchange, tradeSymbol, startdate=fromDate, enddate=toDate)
        else:
            fromDate = fromDate + ' 00:00:00'
            fromDate = time.strptime(fromDate, '%d-%m-%Y %H:%M:%S')
            fromDate = time.mktime(fromDate)
            
            toDate = toDate + ' 23:00:00'
            toDate = time.strptime(toDate, '%d-%m-%Y %H:%M:%S')
            toDate = time.mktime(toDate)
            return brokerApi.get_time_price_series(exchange, exchangeToken, starttime=fromDate, endtime=toDate, interval=interval)
    except:        
        try:
            time.sleep(1)
            if (interval == 'day'):
                return brokerApi.get_daily_price_series(exchange, tradeSymbol, startdate=fromDate, enddate=toDate)
            else:
                return brokerApi.get_time_price_series(exchange, exchangeToken, starttime=fromDate, endtime=toDate, interval=interval)

        except:
            try:
                time.sleep(2)            
                logging.info("Trying to get historical data after 3 seconds")
                if (interval == 'day'):
                    return brokerApi.get_daily_price_series(exchange, tradeSymbol, startdate=fromDate, enddate=toDate)
                else:
                    return brokerApi.get_time_price_series(exchange, exchangeToken, starttime=fromDate, endtime=toDate, interval=interval)

            except Exception as e:
                logging.info("Error while trying to get historical data: " + str(e))
                return ""

def get_from_date_based_interval(broker, interval):
    switcher = {}
    
    if (broker == 'ZERODHA'):
        if (interval == '5minute'):
            fromDate = util.get_lookup_date(8)    
        elif (interval == '15minute'):
            fromDate = util.get_lookup_date(24)
        elif (interval == '30minute'):
            fromDate = util.get_lookup_date(38)
        elif (interval == '60minute'):
            fromDate = util.get_lookup_date(65)
        elif (interval == '2hour'):
            fromDate = util.get_lookup_date(130)
        elif (interval == 'day'):
            fromDate = util.get_lookup_date(400)
        elif (interval == 'week'):
            fromDate = util.get_lookup_date(2000)
        else:
            fromDate = util.get_lookup_date(200)
    elif (broker == 'PROSTOCKS'):
        if (broker == 'PROSTOCKS'):
            switcher = {
                '1' : get_lookup_date(1),
                '3': get_lookup_date(3),
                '5': get_lookup_date(8),
                '10': get_lookup_date(12),
                '15': get_lookup_date(24),
                '30': get_lookup_date(38),
                '60': get_lookup_date(65),
                '120': get_lookup_date(130),
                'day': get_lookup_date(400)
            }
    # Default interval is set to 30 miniutes, and it returns 38 days of historical data
    return switcher.get(interval, get_lookup_date(38))

def get_quotes(brokerApi, exchangeToken, exchange='NSE'):
    try:
        quotes = brokerApi.get_quotes(exchange, str(exchangeToken))
        return quotes
    except:        
        try:
            time.sleep(1)
            quotes = brokerApi.get_quotes(exchange, str(exchangeToken))
            return quotes
        except:
            time.sleep(2)            
            logging.info("Trying LTP after 3 seconds")            
            quotes = brokerApi.get_quotes(exchange, str(exchangeToken))
            return quotes

def get_last_traded_price(brokerApi, exchangeToken, exchange='NSE'):
    try:
        ltp = brokerApi.get_quotes(exchange, str(exchangeToken))
        return float(ltp['lp'])
    except:        
        try:
            time.sleep(1)
            ltp = brokerApi.get_quotes(exchange, str(exchangeToken))
            return float(ltp['lp'])            
        except:
            time.sleep(2)            
            logging.info("Trying LTP after 3 seconds")            
            ltp = brokerApi.get_quotes(exchange, str(exchangeToken))
            return float(ltp['lp'])