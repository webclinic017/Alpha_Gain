import requests

# futTradeSymbol = 'HDFCLIFE26MAY22F'
# quantity = 1100
# modifyQuantity = quantity * 2
# futTriggerPrice =  0 # 578.30

futTradeSymbol = 'APOLLOTYRE-EQ' 
quantity =  2700 
modifyQuantity = quantity * 2
futTriggerPrice =  224.45
instToken = 26000

prdType= 'M'
futTransactionType = 'B'
futOrderType = 'LMT' 
broker = 'PROSTOCKS'

# Suresh Mom account
# accountId = 'S2160'
# password = '45f57305fe50211fa40361aa844bae8db941dda5e9ef454036b872441339a60b'   #'Rithu@15'  -- SHA256
# apiPin = '29101959'
# apiKey = '99cb48e8d3a21cc5a16510110ee7b77f826158095d3f8698788bd7b887d556d9' #S2160|pssLivAPI03052022HJEWFEQ8WR -- SHA256
# vendorCode = 'S2160_U'


# Sanjay17$
# Siva Account
# accountId = 'Y0063'
# password = '25675ce65969952ac2640640a0dd0c9b12507ce01b71f4e3e07c011bee4a203c'   #'Yena@2312'  -- SHA256
# apiPin = ' '
# apiKey = 'ea8219079d55d4d4c9513b337a2424606b6bd7614cb9a1fc80a7a1b201f74f32' #Y0063|pssLivAPI14052022IKFJGN869L -- SHA256
# vendorCode = 'Y0063_U'

accountId = 'R0428'
password = '09d729663d3c81fa3533da7af4d8df4182d8fd659ebc1b69e7aa16ba05e4863c'   #'Yena@2312'  -- SHA256
apiPin = '50801'
apiKey = '4b7de2a8249db1e24f6b63d6d0c639196279efd785cd3d6e88f0d4971300343e' #Y0063|pssLivAPI14052022IKFJGN869L -- SHA256
vendorCode = 'R0428_S'
accessToken= "d95cc40321650d57609a21990e427e85c7a2a7ed1b7fea0cd62e8003518a8eac"




data = "jData={\"uid\":\""+accountId+"\", \"exch\":\"NFO\", \"token\":\""+str(instToken)+"\"}&jKey=" + accessToken
response = requests.post('https://starapi.prostocks.com/NorenWClientTP/GetQuotes', data=data)
json_response = response.json()
print(json_response)


# GetTimePriceSeries
import datetime
from datetime import datetime as dt
        
week_ago = datetime.date.today() - datetime.timedelta(days=7)
startdate = dt.combine(week_ago, dt.min.time()).timestamp()
enddate = dt.now().timestamp()
        


lastBusDay = datetime.datetime.today()
lastBusDay = lastBusDay.replace(hour=0, minute=0, second=0, microsecond=0)
lastBusDay = lastBusDay.timestamp()
data = "jData={\"uid\":\""+accountId+"\", \"exch\":\"NSE\", \"token\":\""+str(instToken)+"\", \"st\":\""+str(startdate)+"\", \"intrv\":\""+str(5)+"\"}&jKey=" + accessToken

response = requests.post('https://starapi.prostocks.com/NorenWClientTP/TPSeries', data=data)
json_response = response.json()
print(json_response)






data = """jData={"apkversion":"1.0.0", "uid":\""""+ str(accountId) +"""\", "pwd":\""""+password+"""\", "factor2":\""""+apiPin+"""\", "imei":"ag3tbbbb33", "source":"API", "vc":\"""" + vendorCode + """\", "appkey":\""""+ apiKey +"""\"}"""
response = requests.post('https://starapi.prostocks.com/NorenWClientTP/QuickAuth', data=data)
json_response = response.json()
accessToken = json_response['susertoken']  
print(accessToken)

# Get Quates
data = "jData={\"uid\":\""+accountId+"\", \"exch\":\"NFO\", \"token\":\""+str(instToken)+"\"}&jKey=" + accessToken
response = requests.post('https://starapi.prostocks.com/NorenWClientTP/GetQuotes', data=data)
json_response = response.json()
print(json_response)

# GetTimePriceSeries
import datetime

lastBusDay = datetime.datetime.today()
lastBusDay = lastBusDay.replace(hour=0, minute=0, second=0, microsecond=0)
lastBusDay = lastBusDay.timestamp()
data = "jData={\"uid\":\""+accountId+"\", \"exch\":\"NFO\", \"token\":\""+str(instToken)+"\", \"st\":\""+str(lastBusDay)+"\", \"intrv\":\""+str(5)+"\"}&jKey=" + accessToken

response = requests.post('https://starapi.prostocks.com/NorenWClientTP/get_time_price_series', data=data)
json_response = response.json()
print(json_response)


data = "jData={\"uid\":\""+accountId+"\", \"actid\":\""+accountId+"\", \"exch\":\"NFO\", \"tsym\":\""+futTradeSymbol+"\",\"qty\":\""+str(quantity)+"\", \"prc\":\""+str(futTriggerPrice)+"\", \"prd\":\""+str(prdType)+"\", \"trantype\":\""+str(futTransactionType)+"\", \"prctyp\":\""+str(futOrderType)+"\", \"ret\":\"DAY\"}&jKey=" + accessToken
response = requests.post('https://starapi.prostocks.com/NorenWClientTP/PlaceOrder', data=data)
json_response = response.json()

if (json_response['stat'] == 'Ok'):        
    orderId = json_response['norenordno']
    print(orderId)

data = "jData={\"uid\":\""+accountId+"\", \"norenordno\":\""+orderId+"\", \"exch\":\"NFO\", \"tsym\":\""+futTradeSymbol+"\", \"prc\":\""+str(futTriggerPrice)+"\", \"qty\":\""+str(modifyQuantity)+"\", \"prctyp\":\"LMT\", \"ret\":\"DAY\"}&jKey=" + accessToken
response = requests.post('https://starapi.prostocks.com/NorenWClientTP/ModifyOrder', data=data)
json_response = response.json()
print(json_response)


data = "jData={\"uid\":\""+accountId+"\", \"norenordno\":\""+orderId+"\"}&jKey=" + accessToken
response = requests.post('https://starapi.prostocks.com/NorenWClientTP/CancelOrder', data=data)
json_response = response.json()
print(json_response)

print("-------------------------------------")
data = "jData={\"uid\":\""+accountId+"\", \"prd\":\"C\"}&jKey=" + accessToken
response = requests.post('https://starapi.prostocks.com/NorenWClientTP/OrderBook', data=data)
json_response = response.json()
print(json_response)
print("-------------------------------------")

