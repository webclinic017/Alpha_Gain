# package import statement
from smartapi import SmartConnect #or from smartapi.smartConnect import SmartConnect
#import smartapi.smartExceptions(for smartExceptions)

refreshToken= 'eyJhbGciOiJIUzUxMiJ9.eyJ0b2tlbiI6IlJFRlJFU0gtVE9LRU4iLCJpYXQiOjE2MzQwNDY4ODF9.Q5lU_yOz5cYuSMJ_W6BG8taryNpPemKuKAj_qcjPZlqOpBYbnXiGvTfRqapl8unrk6viDWxHbG5carG-fVFJZA'
#create object of call
obj=SmartConnect(api_key="avox7vqg")
                #optional
                #access_token = "your access token",
                # refresh_token = "refreshToken")

#login api call

data = obj.generateSession("S608881","Sanjay11$")
# print(data)
# refreshToken= data['data']['refreshToken']
# print(refreshToken)
#fetch the feedtoken
feedToken=obj.getfeedToken()
print(feedToken)

#fetch User Profile
userProfile= obj.getProfile(refreshToken)
print(userProfile)
successFlag = userProfile['status']
print(successFlag)
#place order
try:
    orderparams = {
        "variety": "NORMAL",
        "tradingsymbol": "SBIN-EQ",
        "symboltoken": "3045",
        "transactiontype": "BUY",
        "exchange": "NSE",
        "ordertype": "LIMIT",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "price": "19500",
        "squareoff": "0",
        "stoploss": "0",
        "quantity": "1"
        }
    orderId=obj.placeOrder(orderparams)
    print("The order id is: {}".format(orderId))
except Exception as e:
    print("Order placement failed: {}".format(e.message))

    
#gtt rule list
try:
    status=["FORALL"] #should be a list
    page=1
    count=10
    lists=obj.orderBook()
    print(lists)
except Exception as e:
    print("GTT Rule List failed: {}".format(e.message))

#Historic api
try:
    # historicParam={
    # "exchange": "NSE",
    # "symboltoken": "3045",
    # "interval": "ONE_DAY",
    # "fromdate": "2021-01-01 09:00", 
    # "todate": "2021-02-01 09:16"
    # }
    historicParam={'exchange': 'NSE', 'symboltoken': '7', 'interval': 'ONE_DAY', 'fromdate': '2020-11-26 09:00', 'todate': '2021-10-14 15:30'}
                
    respData =  obj.getCandleData(historicParam)
    data = respData['data']
    print(data)
except Exception as e:
    print("Historic Api failed: {}".format(e.message))
#logout
try:
    logout=obj.terminateSession('Your Client Id')
    print("Logout Successfull")
except Exception as e:
    print("Logout failed: {}".format(e.message))