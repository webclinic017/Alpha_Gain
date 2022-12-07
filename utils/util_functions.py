import datetime
from datetime import timedelta
import logging
from matplotlib.style import available
from mysql.connector.cursor import ERR_NO_RESULT_TO_FETCH
import pandas as pd
import os
import os.path

import mysql.connector
import talib
import numpy as np
import requests
from . import trade_scoring as ts
from utils import broker_api_functions as baf
from utils import util_functions as util
from dateutil import relativedelta
from utils import chart_patterns as cp
from utils import trade_scoring_copy as tsc

# importing all required libraries for telebot
import telebot
from config import Config
import mibian
from utils import prostocks_api_helper as prostocksApi


def send_telegram_message(telegramUserNames, alertMsg):
    telegramUserNamesList = telegramUserNames.split(',')
    try:
        bot = telebot.TeleBot("5372717376:AAEayf6ru4H-lZUOCQK0DZsITlifG_PGzp4")
        for telegramUserName in telegramUserNamesList:
            bot.send_message(telegramUserName, alertMsg)
    except Exception as e:
        logging.info(f"ERROR: Unable to send telegram message {str(e)}")

    
def get_current_fno_expiry_day():
    lastFNOExpiryFound = 'Y'
    endOfMonthDay = 0
    endOfMonth = datetime.datetime.now() + relativedelta.relativedelta(day=31)
    while lastFNOExpiryFound == 'Y':
        endOfMonthDay = (endOfMonth).weekday()
        if (endOfMonthDay == 3):  # last thursday of month
            lastFNOExpiryFound = 'N'
        else:
            endOfMonth = endOfMonth - timedelta(days=1)
    
    return int(endOfMonth.strftime('%d'))


def get_rollover_options_instruments(mySQLCursor, tradeSymbol, optionsType, strikePrice):

    currDate = get_date_time_formatted("%Y-%m-%d")

    instrumentType = 'CE'
    selectStatment = f"SELECT tradingsymbol, instrument_token, lot_size, expiry, strike, exchange_token FROM INSTRUMENTS WHERE name = '{tradeSymbol}' AND \
        exchange='NFO' AND instrument_type='{instrumentType}' and strike = {strikePrice} and expiry > '{currDate}' order by expiry, strike desc limit 1"

    if (optionsType == 'SELL' or optionsType == 'PUT'):
        instrumentType = 'PE'
        selectStatment = f"SELECT tradingsymbol, instrument_token, lot_size, expiry, strike, exchange_token FROM INSTRUMENTS WHERE name = '{str(tradeSymbol)}' \
            AND exchange='NFO' AND instrument_type='{instrumentType}' and strike = {strikePrice} and expiry > '{currDate}' order by expiry, strike limit 1"
    

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    futInstName = ''
    futInstToken = ''
    lotSize = 0
    expDate = ''
    strikePrice = 0
    rawExpiry = ''
    exchangeToken = ''

    for row in results:
        futInstName = row[0]
        futInstToken = row[1]
        lotSize = row[2]
        expDate = str(row[3].strftime("%d%b%y")).upper()
        strikePrice = float(row[4])
        rawExpiry = str(row[3].strftime("%Y-%m-%d")).upper()
        exchangeToken = str(row[5])

    return futInstName, futInstToken, lotSize, expDate, strikePrice, rawExpiry, exchangeToken

# Adjust the price based on the length of integer
def get_round_of_value(value):    
    if (len(str(int(value))) == 5):
        returnVal = round(value, -2)
    elif (len(str(int(value))) == 4):
        returnVal = round(value, -1)
    else:
        returnVal = round(value)
    
    return returnVal

def get_weekly_options_instruments(mySQLCursor, tradeDataDict, tradeSymbol, optionsType, stockClosePrice, adjustOrderFlag, minDaysToExpiry = 3, strikeAdjust=0):
    futInstName = ''
    futInstToken = ''
    lotSize = 0
    expDate = ''
    strikePrice = 0
    rawExpiry = ''
    exchangeToken = ''
    try:
        if (adjustOrderFlag):
            nextExpiryDate = tradeDataDict['expiryDate'].strftime("%Y-%m-%d")
        else:
            nextExpiryDate = (datetime.datetime.now() + datetime.timedelta(days=minDaysToExpiry)).strftime("%Y-%m-%d")

        if (optionsType == 'SELL' or optionsType == 'PUT'):
            instrumentType = 'PE'
        
            stockClosePrice = get_round_of_value(float(stockClosePrice - strikeAdjust))
            
            selectStatment = f"SELECT tradingsymbol, instrument_token, lot_size, expiry, strike, exchange_token FROM INSTRUMENTS WHERE expiry >= '{nextExpiryDate}' and name = '{str(tradeSymbol)}' AND exchange='NFO' AND instrument_type='{instrumentType}' and strike >= {str(stockClosePrice)} order by expiry, strike limit 1"
        else:            
            stockClosePrice = get_round_of_value(float(stockClosePrice + strikeAdjust))
            instrumentType = 'CE'
            selectStatment = f"SELECT tradingsymbol, instrument_token, lot_size, expiry, strike, exchange_token FROM INSTRUMENTS WHERE expiry >= '{nextExpiryDate}' and name = '{str(tradeSymbol)}' AND exchange='NFO' AND instrument_type='{instrumentType}' and strike <= {str(stockClosePrice)} order by expiry, strike desc limit 1"


        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()

        for row in results:
            futInstName = row[0]
            futInstToken = row[1]
            lotSize = row[2]
            expDate = str(row[3].strftime("%d%b%y")).upper()
            strikePrice = float(row[4])
            rawExpiry = str(row[3].strftime("%Y-%m-%d")).upper()
            exchangeToken = str(row[5])

    except Exception as e:
        alertMsg = f"Exceptions occured get_weekly_options_instruments: {str(e)}"
        logging.info('ERROR', alertMsg)

    return futInstName, futInstToken, lotSize, expDate, strikePrice, rawExpiry, exchangeToken


# If you want trade every 2 weeks, pass the value of minDaysToExpiry as 9 days in weekly contract
def get_options_instruments(mySQLCursor, tradeDataDict, minDaysToExpiry, adjustOrderFlag=False, hedgingFlag='N'):
    stockClosePrice = float(tradeDataDict['CLOSE-PRICE'])
    strikeAdjust = 0         
    futInstName = ''
    futInstToken = ''
    lotSize = 0
    expDate = ''
    strikePrice = ''
    rawExpiry = ''
    futExchangeToken = ''
    strikeSelectByPercent = 0

    if (hedgingFlag == 'Y'):
        strikeSelectByPercent = tradeDataDict['hedgingStrikeSelectPercent']
    else:
        strikeSelectByPercent = tradeDataDict['strikeSelectByPercent']

    if (strikeSelectByPercent != 0):
        if (tradeDataDict['strikeType'] == 'OTM'):
            strikeAdjust = stockClosePrice * (strikeSelectByPercent / 100)
        elif (tradeDataDict['strikeType'] == 'ITM'):
            strikeAdjust = stockClosePrice * (strikeSelectByPercent / 100) * -1
        elif (tradeDataDict['strikeType'] == 'ATM'):
            strikeAdjust = 0

    if (tradeDataDict['contractType'] == "MONTHLY"):        
        futInstName, futInstToken, lotSize, expDate, strikePrice, rawExpiry, futExchangeToken  = util.get_monthly_options_instruments(mySQLCursor, tradeDataDict, tradeDataDict['tradeSymbol'], tradeDataDict['instrumentType'], tradeDataDict['CLOSE-PRICE'], adjustOrderFlag, minDaysToExpiry=minDaysToExpiry, strikeAdjust=strikeAdjust)
    
    elif (tradeDataDict['contractType'] == "WEEKLY"):
        futInstName, futInstToken, lotSize, expDate, strikePrice, rawExpiry, futExchangeToken  = util.get_weekly_options_instruments(mySQLCursor, tradeDataDict, tradeDataDict['tradeSymbol'], tradeDataDict['instrumentType'], tradeDataDict['CLOSE-PRICE'], adjustOrderFlag, minDaysToExpiry=minDaysToExpiry, strikeAdjust=strikeAdjust)


    if (tradeDataDict['instrumentType'] == 'CALL'):
        tradeDataDict['futTradeSymbol'] = str(tradeDataDict['tradeSymbol']).replace('&', '%26') + expDate + 'C' + str(strikePrice).replace('.0','')
        
    elif (tradeDataDict['instrumentType'] == 'PUT'):
        tradeDataDict['futTradeSymbol'] = str(tradeDataDict['tradeSymbol']).replace('&', '%26') + expDate + 'P' + str(strikePrice).replace('.0','')

    tradeDataDict['quantity'] = int(lotSize) * tradeDataDict['lotSizeMultiplier'] 
    tradeDataDict['futInstToken'] = futInstToken
    tradeDataDict['rawExpiry'] = rawExpiry
    tradeDataDict['strikePrice'] = strikePrice
    tradeDataDict['strikeAdjust'] = strikeAdjust
    tradeDataDict['futExchangeToken'] = futExchangeToken

    return tradeDataDict



def get_futures_instruments(mySQLCursor, tradeSymbol, minDaysToExpiry=3):    
    currentDay = int(datetime.datetime.today().strftime('%d'))
    lastExpiryDay = util.get_current_fno_expiry_day()
    
    expiryMonth = ""
    if (currentDay > (lastExpiryDay - minDaysToExpiry)):
        expiryMonth = (((datetime.date.today() + relativedelta.relativedelta(months=1))).strftime("%y%b")).upper()
    else:
        expiryMonth= ((datetime.datetime.now()).strftime("%y%b")).upper()

    
    instSearchString = str(tradeSymbol) + str(expiryMonth)
    
    selectStatment = "SELECT tradingsymbol, instrument_token, lot_size, expiry, exchange_token FROM INSTRUMENTS WHERE name = '"+str(tradeSymbol)+"' AND tradingsymbol LIKE '" + instSearchString + "%' AND exchange='NFO' AND instrument_type='FUT' order by expiry limit 1"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    futInstName = ''
    futInstToken = ''
    lotSize = 0
    expDate = ''
    rawExpiry = ''    
    exchangeToken = ''

    for row in results:
        futInstName = row[0]
        futInstToken = row[1]
        lotSize = row[2]
        expDate = str(row[3].strftime("%d%b%y")).upper()
        rawExpiry = str(row[3].strftime("%Y-%m-%d")).upper()        
        exchangeToken = row[4]

    return futInstName, futInstToken, lotSize, expDate, rawExpiry, exchangeToken

   
def get_bt_from_date_based_interval(interval, dateNow):
    fromDate = ''
    if (interval == '15minute'):
        fromDate = util.get_lookup_date(100, dateNow=dateNow)
    elif (interval == '30minute'):
        fromDate = util.get_lookup_date(150 , dateNow=dateNow)
    elif (interval == '60minute'):
        fromDate = util.get_lookup_date(200, dateNow=dateNow)
    elif (interval == '2hour'):
        fromDate = util.get_lookup_date(200, dateNow=dateNow)
    elif (interval == 'day'):
        fromDate = util.get_lookup_date(800, dateNow=dateNow)
    elif (interval == 'week'):
        fromDate = util.get_lookup_date(2000, dateNow=dateNow)
    else:
        fromDate = util.get_lookup_date(200, dateNow=dateNow)

    return fromDate

def get_from_date_based_interval(interval):
    fromDate = ''
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

    return fromDate
    

def get_indice_price_changes(histRecords):    
    df = pd.DataFrame(histRecords)
    close = df['close']        
    df['ROC1'] = talib.ROC(close, timeperiod=1)
    lastRecord = df.tail(1)
    return lastRecord    

def get_ti_for_market_updates(histRecords, niftyHistRecords, category, tradeSymbol):
    df = pd.DataFrame(histRecords)
    dfNifty = pd.DataFrame(niftyHistRecords)
    if (df.shape[0] >= 200 and df.shape[0] == dfNifty.shape[0]):
        dfNifty = pd.DataFrame(niftyHistRecords)
        close = df['close']        
        volume = df['volume']
        high = df['high']
        low = df['low']
        df['ROC1'] = talib.ROC(close, timeperiod=1)   
        # Add the NIFTY'S close data to existing dataframe
        NIFTY_CLOSE = dfNifty['close']
        df['BETA'] = talib.BETA(close, NIFTY_CLOSE, timeperiod=200)
        # KST Composite indicator based on MA of ROC
        df['ROC6'] = talib.ROC(close, timeperiod=6)
        df['ROC12'] = talib.ROC(close, timeperiod=12)
        df['ROC18'] = talib.ROC(close, timeperiod=18)
        df['ROC24'] = talib.ROC(close, timeperiod=24)
        df['SMA_ROC6'] = talib.SMA(df['ROC6'], timeperiod=6)
        df['SMA_ROC12'] = talib.SMA(df['ROC12'], timeperiod=6)
        df['SMA_ROC18'] = talib.SMA(df['ROC18'], timeperiod=9)
        df['SMA_ROC24'] = talib.SMA(df['ROC24'], timeperiod=9)
        df['KST'] = ((2 * df['SMA_ROC18']) + ( 2 * df['SMA_ROC12']) + (1 * df['SMA_ROC6']))/5
        # NIFTY - KST Composite indicator based on MA of ROC
        df['ROC6_NIFTY'] = talib.ROC(NIFTY_CLOSE, timeperiod=6)
        df['ROC12_NIFTY'] = talib.ROC(NIFTY_CLOSE, timeperiod=12)
        df['ROC18_NIFTY'] = talib.ROC(NIFTY_CLOSE, timeperiod=18)
        df['ROC24_NIFTY'] = talib.ROC(NIFTY_CLOSE, timeperiod=24)
        df['SMA_ROC6_NIFTY'] = talib.SMA(df['ROC6_NIFTY'], timeperiod=6)
        df['SMA_ROC12_NIFTY'] = talib.SMA(df['ROC12_NIFTY'], timeperiod=6)
        df['SMA_ROC18_NIFTY'] = talib.SMA(df['ROC18_NIFTY'], timeperiod=9)
        df['SMA_ROC24_NIFTY'] = talib.SMA(df['ROC24_NIFTY'], timeperiod=9)
        df['KST_NIFTY'] =  ((2 * df['SMA_ROC18_NIFTY']) + ( 2 * df['SMA_ROC12_NIFTY']) + (1 * df['SMA_ROC6_NIFTY']))/5
        df['KST_RATIO'] = (df['KST'] - df['KST_NIFTY'])/ abs(df['KST_NIFTY'])
        df['SMA3_KST_RATIO'] = talib.SMA(df['KST_RATIO'], timeperiod=3)
        df['SMA9_KST_RATIO'] = talib.SMA(df['KST_RATIO'], timeperiod=9)
        df['HIGH252'] = talib.MAX(close, timeperiod=200).shift(1)
        df['LOW252'] = talib.MIN(close, timeperiod=200).shift(1)    
    
        
        df['26W_Low_Price'], df['26W_High_Price'] = talib.MINMAX(df['close'], timeperiod=126)
        df['Price_Momentum_26w'] = ((df['close'] - df['26W_Low_Price'])/(df['26W_High_Price'] - df['26W_Low_Price']))*100                                   

        # TTM Squeze 
        df['CMO'] = talib.CMO(close, timeperiod=14) 
        df['BBANDS_UPPER'], df['BBANDS_MIDDLE'], df['BBANDS_LOWER'] = talib.BBANDS(close, timeperiod=10, nbdevup=1, nbdevdn=1, matype=0) 
        #Keltner Channel
        df['TYPPRICE'] = talib.TYPPRICE(high, low, close)
        df['ATR'] = talib.ATR(high, low, close, timeperiod=10)
        df['EMA20_TYPPRICE'] = talib.EMA(df['TYPPRICE'], timeperiod=10)
        df['KC_UpperLine'] = df['EMA20_TYPPRICE'] + (1*df['ATR'])
        df['KC_LowerLine'] = df['EMA20_TYPPRICE'] - (1*df['ATR'])
        
        def in_Squeeze(df):
            Squeeze = 0
            if (df['BBANDS_UPPER'] < df['KC_UpperLine']) and (df['BBANDS_LOWER'] > df['KC_LowerLine']):
                Squeeze = 1
            else : 
                Squeeze = 0 
            return Squeeze

        df['TTMSqueeze'] = df.apply(in_Squeeze, axis=1)
        df['TTMSLength'] = talib.SUM(abs(df['TTMSqueeze']), timeperiod=10)

      #NEW
        def UpDays(df):
            UpDays = 0
            if (df['ROC1'] > 0) :
                UpDays = 1
            else : 
                UpDays = 0 
            return UpDays
        df['UpDays'] = df.apply(UpDays, axis=1)
        df['UpDays_Count'] = talib.SUM((df['UpDays']), timeperiod=22)
        df['UP_IN_3_DAYS'] = talib.SUM((df['UpDays']), timeperiod=3)
        
        def DownDays(df):
            DownDays = 0
            if (df['ROC1'] < 0) :
                DownDays = 1
            else : 
                DownDays = 0 
            return DownDays
        df['DownDays'] = df.apply(DownDays, axis=1)
        df['DownDays_Count'] = talib.SUM((df['DownDays']), timeperiod=22)
        df['DOWN_IN_3_DAYS'] = talib.SUM((df['DownDays']), timeperiod=3)
        df['UD_Days_Ratio_22'] = ((df['UpDays_Count'] / df['DownDays_Count']))
        df['SMA3_UD_Days_Ratio'] = talib.SMA(df['UD_Days_Ratio_22'] , timeperiod=3)
        df['SMA9_UD_Days_Ratio'] = talib.SMA(df['UD_Days_Ratio_22'] , timeperiod=9)

        def UpVolume(df):
            UpVolume = 0
            if (df['ROC1'] > 0) :
                UpVolume = df['volume'] * abs(df['ROC1'])
            else : 
                UpVolume = 0 
            return UpVolume
        df['UpVolume'] = df.apply(UpVolume, axis=1)
        df['UpVolume_Sum'] = talib.SUM((df['UpVolume']), timeperiod=22)
        
        def DownVolume(df):
            DownVolume = 0
            if (df['ROC1'] < 0) :
                DownVolume = df['volume'] * abs(df['ROC1'])
            else : 
                DownVolume = 0 
            return DownVolume
            
        df['DownVolume'] = df.apply(DownVolume, axis=1)
        df['DownVolume_Sum'] = talib.SUM((df['DownVolume']), timeperiod=22)


            


        lastRecord = df.tail(1)    
        return lastRecord, "YES"
    else:        
        print(tradeSymbol)
        return "", "NO"

def get_next_trading_date(mySQLCursor): 
    foundFlag = 'N'
    dateIncr = 1
    while foundFlag != 'Y':
        nextTradingDate = (datetime.datetime.now() + datetime.timedelta(days=dateIncr)).strftime("%Y-%m-%d")
        holidayFlag = util.isItHoliday(mySQLCursor, nextTradingDate)
        day = util.get_day_of_date(nextTradingDate)
        if (day != 'Saturday' and day != 'Sunday' and not(holidayFlag)):
            foundFlag = 'Y'
        else:
            dateIncr += 1

    return nextTradingDate

def isItHoliday(mySQLCursor, currDate):
    selectStatment = "SELECT HOLIDAY_DATE FROM HOLIDAYS_LIST"
    mySQLCursor.execute(selectStatment)    
    results = mySQLCursor.fetchall()    
    isHolidayFlag = False
    
    for row in results:
        holiday = row[0]
        if (holiday == currDate):
            isHolidayFlag = True

    return isHolidayFlag

def get_day_of_date(currDate):
    return (datetime.datetime.strptime(currDate, '%Y-%m-%d')).strftime("%A")

# funtions will be in small letters with _ separation
# variable names will be in camel case starting with small case
# constant variables will be ALL upper case

def get_indicators(histRecords, niftyHistRecords, stockName, category=None):    
    try:
        if (category == None):
            category='Stocks'
        df = pd.DataFrame(histRecords)
        dfNifty = pd.DataFrame(niftyHistRecords)
        if (df.shape[0] >= 200 and df.shape[0] == dfNifty.shape[0]):

            df['TIME'] = df['date'].dt.strftime('%H%M')
            df['STOCK_NAME'] = stockName
            close = df['close']
            volume = df['volume']
            high = df['high']
            low = df['low']
            
            df['DATETIME'] = df['date'].dt.strftime('%Y-%m-%d---%H%M')
            df['SMA5'] = talib.SMA(close, timeperiod=5)
            df['SMA10'] = talib.SMA(close, timeperiod=10)
            df['SMA200'] = talib.SMA(close, timeperiod=200)
            df['ROC1'] = talib.ROC(close, timeperiod=1)
            df['LOW10_TMP'], df['HIGH10_TMP'] = talib.MINMAX(
                close, timeperiod=10)
            df['LOW10'] = df['LOW10_TMP'].shift(1)
            df['HIGH10'] = df['HIGH10_TMP'].shift(1)
            
            df['SMA20_VOL'] = talib.SMA(volume, timeperiod=20)
            df['RSI'] = talib.RSI(close, timeperiod=14)
            df['RSI_3'] = talib.RSI(close, timeperiod=14).shift(3)
            df['RSI_MA3'] = talib.SMA(df['RSI'], timeperiod=3)
            df['RSI_MA3_1'] = talib.SMA(df['RSI'], timeperiod=3).shift(1)
            
            df['SAR'] = talib.SAR(high, low, acceleration=0.02, maximum=0.2)
            df['index'] = 100
            df['LOW5_TMP'], df['HIGH5_TMP'] = talib.MINMAX(close, timeperiod=3)
            df['LOW5'] = df['LOW5_TMP'].shift(1)
            df['HIGH5'] = df['HIGH5_TMP'].shift(1)
            # Added for trend strategy
            df['HIGH20'] = talib.MAX(high, timeperiod=20).shift(1)
            df['LOW20'] = talib.MIN(low, timeperiod=20).shift(1)
            df['HIGH200'] = talib.MAX(close, timeperiod=200).shift(1)
            df['LOW200'] = talib.MIN(close, timeperiod=200).shift(1)    

            df['EMA5'] = talib.EMA(close, timeperiod=5)
            df['EMA8'] = talib.EMA(close, timeperiod=8)
            df['EMA21'] = talib.EMA(close, timeperiod=21)
            df['EMA34'] = talib.EMA(close, timeperiod=34)
            df['EMA55'] = talib.EMA(close, timeperiod=55)
            df['EMA84'] = talib.EMA(close, timeperiod=84)
            
            df['EMA12'] = talib.EMA(close, timeperiod=12)
            df['EMA26'] = talib.EMA(close, timeperiod=26)
            df['MACD'], df['MACDSIGNAL'], df['MACDHIST'] = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
            df['EMA200'] = talib.EMA(close, timeperiod=100)
            df['EMA40'] = talib.EMA(close, timeperiod=40)
            df['EMA40(1)'] = df['EMA40'].shift(1)
            
            df['MFI'] = talib.MFI(high, low, close, volume, timeperiod=14)
            
            df['STOCH_SLOWK'], df['STOCH_SLOWD'] = talib.STOCH(high, low, close, fastk_period=14, slowk_period=9, slowk_matype=0, slowd_period=9, slowd_matype=0)
            df['SMA3_STOCH'] = talib.SMA(df['STOCH_SLOWK'], timeperiod=3)
            df['SMA3_STOCH_1'] = talib.SMA(df['STOCH_SLOWK'], timeperiod=3).shift(1)
            
            df['STOCHRSI'], df['STOCHRSI_FASTD'] = talib.STOCHRSI(
                close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
            df['STOCHRSI(1)'] = df['STOCHRSI'].shift(1)
            df['SMA10_VOL'] = talib.SMA(volume, timeperiod=10)
            df['SMA10_VOL(1)'] = df['SMA10_VOL'].shift(1)
            df['SMA5_VOL'] = talib.SMA(volume, timeperiod=5)
            df['SMA5_VOL(1)'] = df['SMA5_VOL'].shift(1)
            df['26W_Low_Price'], df['26W_High_Price'] = talib.MINMAX(df['close'], timeperiod=126)
            df['Price_Momentum_26w'] = ((df['close'] - df['26W_Low_Price'])/(df['26W_High_Price'] - df['26W_Low_Price']))*100
            
            
            df["Cdl_Bullish"] = ((close - low) / (high - low)).shift(1)
            df["Cdl_Bearish"] = ((high - close) / (high - low)).shift(1)  
            # TTM Squeze 
            df['CMO'] = talib.CMO(close, timeperiod=14) 
            df['BBANDS_UPPER'], df['BBANDS_MIDDLE'], df['BBANDS_LOWER'] = talib.BBANDS(close, timeperiod=10, nbdevup=1, nbdevdn=1, matype=0) 
            #Keltner Channel
            df['TYPPRICE'] = talib.TYPPRICE(high, low, close)
            df['ATR'] = talib.ATR(high, low, close, timeperiod=10)
            df['EMA20_TYPPRICE'] = talib.EMA(df['TYPPRICE'], timeperiod=10)
            df['KC_UpperLine'] = df['EMA20_TYPPRICE'] + (1*df['ATR'])
            df['KC_LowerLine'] = df['EMA20_TYPPRICE'] - (1*df['ATR'])
            
            def in_Squeeze(df):
                Squeeze = 0
                if (df['BBANDS_UPPER'] < df['KC_UpperLine']) and (df['BBANDS_LOWER'] > df['KC_LowerLine']):
                    Squeeze = 1
                else : 
                    Squeeze = 0 
                return Squeeze
            
            df['TTMSqueeze'] = df.apply(in_Squeeze, axis=1)
            df['TTMSqueeze1'] = df.apply(in_Squeeze, axis=1).shift(1)
            df['TTMSLength'] = talib.SUM(abs(df['TTMSqueeze']), timeperiod=10)
            
            
            def UpDays(df):
                UpDays = 0
                if (df['ROC1'] > 0) :
                    UpDays = 1
                else : 
                    UpDays = 0 
                return UpDays
            df['UpDays'] = df.apply(UpDays, axis=1)
            df['UpDays_Count'] = talib.SUM((df['UpDays']), timeperiod=22)
            df['UP_IN_3_DAYS'] = talib.SUM((df['UpDays']), timeperiod=3)
            
            def DownDays(df):
                DownDays = 0
                if (df['ROC1'] < 0) :
                    DownDays = 1
                else : 
                    DownDays = 0 
                return DownDays

            df['DownDays'] = df.apply(DownDays, axis=1)
            df['DownDays_Count'] = talib.SUM((df['DownDays']), timeperiod=22)
            df['DOWN_IN_3_DAYS'] = talib.SUM((df['DownDays']), timeperiod=3)
            df['UD_Days_Ratio_22'] = ((df['UpDays_Count'] / df['DownDays_Count']))
            df['SMA3_UD_Days_Ratio'] = talib.SMA(df['UD_Days_Ratio_22'] , timeperiod=3)
            df['SMA9_UD_Days_Ratio'] = talib.SMA(df['UD_Days_Ratio_22'] , timeperiod=9)

            
            def UpVolume(df):
                UpVolume = 0
                if (df['ROC1'] > 0) :
                    UpVolume = df['volume'] * abs(df['ROC1'])
                else : 
                    UpVolume = 0 
                return UpVolume
            df['UpVolume'] = df.apply(UpVolume, axis=1)
            df['UpVolume_Sum'] = talib.SUM((df['UpVolume']), timeperiod=63)
            
            def DownVolume(df):
                DownVolume = 0
                if (df['ROC1'] < 0) :
                    DownVolume = df['volume'] * abs(df['ROC1'])
                else : 
                    DownVolume = 0 
                return DownVolume
            
            df['DownVolume'] = df.apply(DownVolume, axis=1)
            df['DownVolume_Sum'] = talib.SUM((df['DownVolume']), timeperiod=22)        
    
            df['Linear_Angle'] = talib.LINEARREG_ANGLE(close, timeperiod=66).shift(10)
            df['Linear_Angle10'] = talib.LINEARREG_ANGLE(close, timeperiod=10)
            df['CLHIBeta'] = talib.BETA(close, high, timeperiod=66)
            df['ROC1'] = talib.ROC(close, timeperiod=1)
            df['LOW10_TMP'], df['HIGH10_TMP'] = talib.MINMAX(close, timeperiod=10)
            df['LOW10'] = df['LOW10_TMP'].shift(1)
            df['HIGH10'] = df['HIGH10_TMP'].shift(1)        
            df['SMA20_VOL'] = talib.SMA(volume, timeperiod=20)
            df['STDDEV60'] = talib.STDDEV(df['ROC1'], timeperiod=120, nbdev=1)
            df['SUMROC'] = talib.SUM(abs(df['ROC1']), timeperiod=66)
            df['ROC66'] = talib.ROC(close, timeperiod=66)
            df['ER'] = df['ROC66'] / df['SUMROC']

            # KST Composite indicator based on MA of ROC
            df['ROC6'] = talib.ROC(close, timeperiod=6)
            df['ROC12'] = talib.ROC(close, timeperiod=12)
            df['ROC18'] = talib.ROC(close, timeperiod=18)
            df['ROC24'] = talib.ROC(close, timeperiod=24)
            df['SMA_ROC6'] = talib.SMA(df['ROC6'], timeperiod=6)
            df['SMA_ROC12'] = talib.SMA(df['ROC12'], timeperiod=6)
            df['SMA_ROC18'] = talib.SMA(df['ROC18'], timeperiod=9)
            df['SMA_ROC24'] = talib.SMA(df['ROC24'], timeperiod=9)
            
            # Add the NIFTY'S close data to existing dataframe
            NIFTY_CLOSE = dfNifty['close']
            df['BETA'] = talib.BETA(close, NIFTY_CLOSE, timeperiod=200)
            # NIFTY - KST Composite indicator based on MA of ROC
            df['ROC6_NIFTY'] = talib.ROC(NIFTY_CLOSE, timeperiod=6)
            df['ROC12_NIFTY'] = talib.ROC(NIFTY_CLOSE, timeperiod=12)
            df['ROC18_NIFTY'] = talib.ROC(NIFTY_CLOSE, timeperiod=18)
            df['ROC24_NIFTY'] = talib.ROC(NIFTY_CLOSE, timeperiod=24)
            df['SMA_ROC6_NIFTY'] = talib.SMA(df['ROC6_NIFTY'], timeperiod=6)
            df['SMA_ROC12_NIFTY'] = talib.SMA(df['ROC12_NIFTY'], timeperiod=6)
            df['SMA_ROC18_NIFTY'] = talib.SMA(df['ROC18_NIFTY'], timeperiod=9)
            df['SMA_ROC24_NIFTY'] = talib.SMA(df['ROC24_NIFTY'], timeperiod=9)


            df['KST'] = ((2 * df['SMA_ROC18']) + (2 * df['SMA_ROC12']) + (1 * df['SMA_ROC6'])) / 5
            df['KST_NIFTY'] =  ((2 * df['SMA_ROC18_NIFTY']) + ( 2 * df['SMA_ROC12_NIFTY']) + (1 * df['SMA_ROC6_NIFTY'])) / 5
            df['KST_RATIO'] = ((df['KST'] - df['KST_NIFTY']) / abs(df['KST_NIFTY'])) + 1

            df['SMA3_KST_RATIO'] = talib.SMA(df['KST_RATIO'], timeperiod=3)
            df['SMA9_KST_RATIO'] = talib.SMA(df['KST_RATIO'], timeperiod=9)
            
            if (category == "Stocks"):
                # OBV related indicators
                df['OBV'] = talib.OBV(close, volume)
                df['SMA10_OBV'] = talib.SMA(df['OBV'], timeperiod=10)
                df['SMA10_OBV(1)'] = df['SMA10_OBV'].shift(1)
                df['SMA5_OBV'] = talib.SMA(df['OBV'], timeperiod=5)
                df['SMA5_OBV(1)'] = df['SMA5_OBV'].shift(1)
                df['MACD_OBV'], df['MACDSIGNAL_OBV'], df['MACDHIST_OBV'] = talib.MACD(df['OBV'], fastperiod=12, slowperiod=26, signalperiod=9)
                df['MACD_OBV1'] = df['MACD_OBV'].shift(1)
                df['MACDSIGNAL_OBV1'] = df['MACDSIGNAL_OBV'].shift(1)
                df['26W_Low_OBV'], df['26W_High_OBV'] = talib.MINMAX(df['OBV'], timeperiod=126)
                df['OBV_Momentum_26w'] = ((df['OBV'] - df['26W_Low_OBV'])/(df['26W_High_OBV'] - df['26W_Low_OBV']))*100
                df['OBVMM/PMM'] = df['OBV_Momentum_26w'] / df['Price_Momentum_26w']
                df['Total_TrendPoints'] = df['OBV_Momentum_26w'] + df['Linear_Angle'] + (df['CLHIBeta'] * 50) + (df['ER'] * 80) + abs(df['Linear_Angle10'] * 0.50)
                
                df['UD_Volume_Ratio'] = ((df['UpVolume_Sum'] / df['DownVolume_Sum']))
                df['SMA3_UD_Volume_Ratio'] = talib.SMA(df['UD_Volume_Ratio'] , timeperiod=3)
                df['SMA9_UD_Volume_Ratio'] = talib.SMA(df['UD_Volume_Ratio'] , timeperiod=9)

            return df, True
        else:
            return df, False
    except Exception as e:        
        logging.info("Errored in get_indicator: " + str(e))
        return df, False

#call application path directory
def get_full_path(relativePath):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relativePath
        )
    )

def initialize_logs(logFileName):
    logFileName = get_full_path("../logs/" + logFileName)
    logging.basicConfig(filename=logFileName, format='%(asctime)s %(message)s', datefmt='%d-%m-%Y %I:%M:%S', level=logging.INFO)  

# current time
def get_system_time():
    return datetime.datetime.now()

def get_lookup_date(noOfDays, dateNow = None):
    # get date before current date
    if (dateNow == None):
        return (datetime.datetime.now() - datetime.timedelta(days=noOfDays)).strftime("%Y-%m-%d") 
    else:
        return (datetime.datetime.strptime(dateNow, '%Y-%m-%d') - datetime.timedelta(days=noOfDays)).strftime("%Y-%m-%d") 

# current date
def get_system_date():
    return (datetime.datetime.now()).strftime("%Y-%m-%d")

# date time conversion method
def get_date_time_formatted(dateTimeFormat):
    return (datetime.datetime.now()).strftime(dateTimeFormat)

# connect mysql server 
def connect_mysql_db(configDict = {}):
    try:
        cnx = mysql.connector.connect(
            user=Config.DB_USER_ID, password=Config.DB_PASSWORD, database=Config.DB_DATABASE, host=Config.DB_HOST, port=Config.DB_PORT)
        mySQLCursor = cnx.cursor(buffered=True) 
        # fetch total rowcount initially when buffered=true and rowcount=-1 initially when buffered=false
    except Exception as e:
        print ("Error occured while connecting to Database; " + str(e))
    return cnx, mySQLCursor

def verify_db_connection(cnx, mySQLCursor, configDict = {}):    
    try:
        sqlStatement = "SELECT NOW()"
        mySQLCursor.execute( sqlStatement )
        rowCount = mySQLCursor.rowcount        
        if rowCount != 0:        
            return cnx, mySQLCursor
        else:                              
            cnx, mySQLCursor = util.connect_mysql_db()
            return cnx, mySQLCursor
    except:        
        cnx, mySQLCursor = util.connect_mysql_db()
        return cnx, mySQLCursor
        



def get_telegram_id():
    # Chat ID for AlphaGain Alerts
    chatId = '-489407341'
    # Chat ID for AlphaGain Signals
    # chatId = '-514860679'
    # Chat ID for AlphaGain Admin
    chatIdAdmin = '-573550832'    
    return chatId, chatIdAdmin

def get_active_strategy_id(mySQLCursor, entryOrExit = None):
    # selectStatment = "SELECT USS.STRATEGY_ID FROM USR_TRADE_ACCOUNTS UTA LEFT JOIN USR_STRATEGY_SUBSCRIPTIONS USS ON USS.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT LEFT JOIN SYS_STRATEGY_MASTER SSM ON SSM.STRATEGY_ID= USS.STRATEGY_ID WHERE UTA.ACTIVE_FLAG='Y' AND UTA.ACCOUNT_TYPE = 'LIVE' AND SSM.ACTIVE_FLAG ='ACTIVE' GROUP BY USS.STRATEGY_ID"
    
    if (entryOrExit == 'ENTRY'):
        selectStatment = "SELECT USS.STRATEGY_ID FROM USR_TRADE_ACCOUNTS UTA LEFT JOIN USR_STRATEGY_SUBSCRIPTIONS USS ON USS.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT WHERE UTA.ACTIVE_FLAG='Y' AND UTA.ACCOUNT_TYPE = 'LIVE' AND USS.ACTIVE_FLAG ='ACTIVE' GROUP BY USS.STRATEGY_ID"
    else:
        selectStatment = "SELECT STRATEGY_ID FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS IN ('OPEN','PENDING') GROUP BY STRATEGY_ID"
    
    mySQLCursor.execute(selectStatment)
    return mySQLCursor.fetchall()    


def get_active_trade_accounts(mySQLCursor, entryOrExit = None):
    currDate = util.get_date_time_formatted("%Y-%m-%d") 
    selectStatment = None
    if (entryOrExit == 'ENTRY'):
        selectStatment = f"""SELECT UTA.TRADE_ACCOUNT, UTA.BROKER FROM USR_TRADE_ACCOUNTS UTA
            LEFT JOIN USR_STRATEGY_SUBSCRIPTIONS USS ON USS.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT
            WHERE USS.ACTIVE_FLAG ='ACTIVE' AND USS.STRATEGY_ON_OFF_FLAG ='ON' AND UTA.ACTIVE_FLAG='Y' AND UTA.ACCOUNT_TYPE = 'LIVE' 
            AND (UTA.ACCESS_TOKEN_VALID_FLAG = 'Y' AND DATE(UTA.ACCESS_VALIDITY_DATE) = '{currDate}') AND (CURTIME() >= CAST(USS.TRADE_START_TIME AS TIME) AND CURTIME() < CAST(USS.TRADE_END_TIME AS TIME)) GROUP BY USS.TRADE_ACCOUNT"""

    elif(entryOrExit == 'EXIT'):
        selectStatment = f"""SELECT UTA.TRADE_ACCOUNT, UTA.BROKER FROM USR_TRADE_ACCOUNTS UTA 
            LEFT JOIN USR_STRATEGY_SUBSCRIPTIONS USS ON USS.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT  AND USS.ACTIVE_FLAG ='ACTIVE' 
            WHERE USS.ACTIVE_FLAG ='ACTIVE' AND UTA.ACTIVE_FLAG='Y' AND UTA.ACCOUNT_TYPE = 'LIVE' AND (UTA.ACCESS_TOKEN_VALID_FLAG = 'Y' AND DATE(UTA.ACCESS_VALIDITY_DATE) = '{currDate}') AND CURTIME() >= CAST(USS.EXIT_START_TIME AS TIME) 
            AND UTA.TRADE_ACCOUNT IN (SELECT TRADE_ACCOUNT FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS IN ('PENDING', 'OPEN') GROUP BY TRADE_ACCOUNT)
            GROUP BY USS.TRADE_ACCOUNT"""
    
    mySQLCursor.execute(selectStatment)
    return mySQLCursor.fetchall()    


def get_open_trades_account(mySQLCursor):    
    selectStatment = "SELECT TT.TRADE_ACCOUNT, UTA.BROKER FROM TRADE_TRANSACTIONS TT LEFT JOIN USR_TRADE_ACCOUNTS UTA ON TT.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT \
                        WHERE TT.TRADE_STATUS IN ('PENDING', 'OPEN') GROUP BY TT.TRADE_ACCOUNT"       
    mySQLCursor.execute(selectStatment)
    return mySQLCursor.fetchall()    


def get_open_signals(mySQLCursor, strategyId =  None):
    currDate = get_date_time_formatted("%Y-%m-%d")
    selectStatment = None
    if (strategyId is None):
        selectStatment = "SELECT STRATEGY_ID FROM TRADE_SIGNALS WHERE SIGNAL_STATUS = 'OPEN' AND DATE(TRIGGER_DATE) = '"+str(currDate)+"'\
             GROUP BY STRATEGY_ID"
    else:
        selectStatment = "SELECT INSTRUMENT_TOKEN, STOCK_NAME, TRADE_SYMBOL, ENTRY_PRICE, TRIGGER_DATE, SIGNAL_ID, TARGET_PRICE, \
            STOP_LOSS_PRICE, TGT_HORIZON, TGT_PROFIT_PCT, TGT_STOP_LOSS_PCT FROM TRADE_SIGNALS WHERE SIGNAL_STATUS = 'OPEN' \
                AND STRATEGY_ID = '" +str(strategyId)+ "' AND DATE(TRIGGER_DATE) = '"+str(currDate)+"' ORDER BY TRIGGER_DATE DESC"
        
    mySQLCursor.execute(selectStatment)
    return mySQLCursor.fetchall()    

def get_system_master_strategy(mySQLCursor, strategyId):
    selectStatment = "SELECT STRATEGY_ID, ACTIVE_FLAG, STRATEGY_NAME, STRATEGY_OWNER, STRATEGY_DF, STOCK_SELECTION_STRATEGY, \
                    PRICE_INTERVAL, LOOK_BACK_DAYS_START, LOOK_BACK_DAYS_END,  TOTAL_POSITION_LIMIT, \
                    DAY_POSITION_LIMIT, POSITION_SIZE, TGT_PROFIT_PCT, TGT_STOP_LOSS_PCT, TRAILING_THRESHOLD_PCT, \
                    TGT_HORIZON, TECHNICAL_EXIT, EVENT_BASED_EXIT, STRATEGY_SHORT_DESCRIPTION, STRATEGY_DESCRIPTION, \
                    STOCK_SELECTION_STRATEGY_DESCRIPTION, ENTRY_SETUP_DESCRIPTION, \
                    EXIT_STRATEGY_DESCRIPTION, KEY_RISK_FACTORS, TRADING_STYLE, STRATEGY_TYPE, IDEAL_MARKET_BIAS FROM SYS_STRATEGY_MASTER WHERE STRATEGY_ID = '" +str(strategyId)+ "' AND ACTIVE_FLAG ='ACTIVE'"
    mySQLCursor.execute(selectStatment)
    results =  mySQLCursor.fetchall()
    sysStgyMasterDict = {}    

    for row in results:
        sysStgyMasterDict['STRATEGY_ID']=row[0]
        sysStgyMasterDict['ACTIVE_FLAG']=row[1]
        sysStgyMasterDict['STRATEGY_NAME']=row[2]
        sysStgyMasterDict['STRATEGY_OWNER']=row[3]
        sysStgyMasterDict['STRATEGY_DF']=row[4]
        sysStgyMasterDict['STOCK_SELECTION_STRATEGY']=row[5]
        sysStgyMasterDict['PRICE_INTERVAL']=row[6]
        sysStgyMasterDict['LOOK_BACK_DAYS_START']=row[7]
        sysStgyMasterDict['LOOK_BACK_DAYS_END']=row[8]
        sysStgyMasterDict['TOTAL_POSITION_LIMIT']=row[10]
        sysStgyMasterDict['DAY_POSITION_LIMIT']=row[11]
        sysStgyMasterDict['POSITION_SIZE']=row[12]
        sysStgyMasterDict['TGT_PROFIT_PCT']=row[13]
        sysStgyMasterDict['TGT_STOP_LOSS_PCT']=row[14]
        sysStgyMasterDict['TRAILING_THRESHOLD_PCT']=row[15]
        sysStgyMasterDict['TGT_HORIZON']=row[16]
        sysStgyMasterDict['TECHNICAL_EXIT']=row[17]
        sysStgyMasterDict['EVENT_BASED_EXIT']=row[18]
        sysStgyMasterDict['STRATEGY_SHORT_DESCRIPTION']=row[19]
        sysStgyMasterDict['STRATEGY_DESCRIPTION']=row[20]
        sysStgyMasterDict['STOCK_SELECTION_STRATEGY_DESCRIPTION']=row[21]

        sysStgyMasterDict['ENTRY_SETUP_DESCRIPTION']=row[23]
        sysStgyMasterDict['EXIT_STRATEGY_DESCRIPTION']=row[24]
        sysStgyMasterDict['KEY_RISK_FACTORS']=row[25]
        sysStgyMasterDict['TRADING_STYLE']=row[26]
        sysStgyMasterDict['STRATEGY_TYPE']=row[27]
        sysStgyMasterDict['IDEAL_MARKET_BIAS']=row[28]

    return sysStgyMasterDict

def get_available_margin(mySQLCursor, accountId): 
    query = f"SELECT AVAILABLE_MARGIN FROM PROD.USR_TRADE_ACCOUNTS WHERE TRADE_ACCOUNT='{accountId}'"
    mySQLCursor.execute(query)
    results = mySQLCursor.fetchall()
    availableMargin = 0
    for row in results:        
        availableMargin = float(row[0])

    return availableMargin

# Exit Flag is important for the exit programs, without the exitflag=True, it won't exit or check for the updates
def get_strategy_setttings(mySQLCursor, accountId, entryOrExit='ENTRY'):

    selectStatment = f"SELECT USS.CAPITAL_ALLOCATION, \
                        USS.TOTAL_POSITION_LIMIT, \
                        USS.DAY_POSITION_LIMIT, \
                        USS.POSITION_SIZE, \
                        USS.EXIT_STRATEGY_ID, \
                        USS.TGT_PROFIT_PCT, \
                        USS.TGT_STOP_LOSS_PCT, \
                        USS.TRAILING_THRESHOLD_PCT, \
                        USS.TRADE_ACCOUNT, \
                        UTA.BROKER, \
                        UTA.AVAILABLE_MARGIN, \
                        UTA.UAT_FLAG, \
                        SSM.ENTRY_BUY_CONDITION, \
                        SSM.ENTRY_SELL_CONDITION, \
                        SSM.EXIT_BUY_CONDITION, \
                        SSM.EXIT_SELL_CONDITION, \
                        SSM.STRATEGY_TYPE, \
                        USS.ENTRY_DIRECTION, \
                        USS.EXCHANGE, \
                        USS.EXIT_INTERVAL_TIME, \
                        USS.LOT_SIZE_MULTIPLIER, \
                        USS.ROLL_OVER_FLAG, \
                        USS.CONTRACT_TYPE, \
                        USS.PRODUCT_TYPE, \
                        UTA.TELEGRAM_USER_NAME, \
                        USS.ALLOCATED_CASH_EXCEEDED_FLAG, \
                        USS.STRIKE_SELECT_BY_PERCENT, \
                        USS.STRIKE_SELECT_BY_ATM, \
                        USS.TECH_INDICATOR_INTERVAL, \
                        USS.REV_PATTERN_CHECK_FLAG, \
                        USS.TRADE_START_TIME, \
                        USS.TRADE_END_TIME, \
                        USS.STRIKE_TYPE, \
                        USS.OPTIONS_ELIGIBILITY_CONDITIONS, \
                        USS.DUPLICATE_POS_ALLOWED_FLAG, \
                        USS.MIN_DAYS_TO_EXPIRY_FOR_NEW_TRADE, \
                        USS.HEDGING_REQUIRED_FLAG, \
                        USS.HEDGING_STRIKE_SELECT_PERCENT, \
                        USS.EOD_EXIT_FLAG, \
                        USS.STRATEGY_ID \
                            FROM USR_TRADE_ACCOUNTS UTA \
                            LEFT JOIN USR_STRATEGY_SUBSCRIPTIONS USS ON USS.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT \
                            LEFT JOIN SYS_STRATEGY_MASTER SSM ON SSM.STRATEGY_ID= USS.STRATEGY_ID AND SSM.ACTIVE_FLAG='ACTIVE'"

    
    if (entryOrExit == 'ENTRY'):
        selectStatment = f"{selectStatment} WHERE UTA.ACTIVE_FLAG='Y' AND UTA.ACCOUNT_TYPE = 'LIVE' AND USS.TRADE_ACCOUNT ='{accountId}' AND \
                (CURTIME() >= CAST(USS.TRADE_START_TIME AS TIME) AND CURTIME() < CAST(USS.TRADE_END_TIME AS TIME)) \
                AND USS.ACTIVE_FLAG ='ACTIVE' AND USS.STRATEGY_ON_OFF_FLAG ='ON'"
    elif (entryOrExit == 'EXIT'):
        selectStatment = f"{selectStatment} WHERE UTA.ACTIVE_FLAG='Y' AND UTA.ACCOUNT_TYPE = 'LIVE' AND USS.TRADE_ACCOUNT ='{accountId}' AND \
                CURTIME() >= CAST(USS.EXIT_START_TIME AS TIME) AND USS.ACTIVE_FLAG ='ACTIVE'"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    usrStrySbcptDict = {}
    usrStrySbcptList = []
    # gets the number of rows affected by the command executed
    for row in results:     

        usrStrySbcptDict['CAPITAL_ALLOCATION'] = row[0]
        usrStrySbcptDict['TOTAL_POSITION_LIMIT'] = row[1]
        usrStrySbcptDict['DAY_POSITION_LIMIT'] = row[2]
        usrStrySbcptDict['POSITION_SIZE'] = row[3]
        usrStrySbcptDict['EXIT_STRATEGY_ID'] = row[4]
        usrStrySbcptDict['TGT_PROFIT_PCT'] = row[5]
        usrStrySbcptDict['TGT_STOP_LOSS_PCT'] = row[6]
        usrStrySbcptDict['TRAILING_THRESHOLD_PCT'] = row[7]
        usrStrySbcptDict['TRADE_ACCOUNT'] = row[8]
        usrStrySbcptDict['BROKER'] = row[9]
        usrStrySbcptDict['AVAILABLE_MARGIN'] = row[10]
        usrStrySbcptDict['UAT_FLAG'] = row[11]
        usrStrySbcptDict['ENTRY_BUY_CONDITION'] = row[12]
        usrStrySbcptDict['ENTRY_SELL_CONDITION'] = row[13]
        usrStrySbcptDict['EXIT_BUY_CONDITION'] = row[14]
        usrStrySbcptDict['EXIT_SELL_CONDITION'] = row[15]
        usrStrySbcptDict['STRATEGY_TYPE'] = row[16]
        usrStrySbcptDict['ENTRY_DIRECTION'] = row[17]
        usrStrySbcptDict['EXCHANGE'] = row[18]
        usrStrySbcptDict['EXIT_INTERVAL_TIME'] = row[19]
        usrStrySbcptDict['LOT_SIZE_MULTIPLIER'] = row[20]
        usrStrySbcptDict['ROLL_OVER_FLAG'] = row[21]
        usrStrySbcptDict['CONTRACT_TYPE'] = row[22]
        usrStrySbcptDict['PRODUCT_TYPE'] = row[23]
        usrStrySbcptDict['TELEGRAM_USER_NAME'] = row[24]
        usrStrySbcptDict['ALLOCATED_CASH_EXCEEDED_FLAG'] = row[25]
        usrStrySbcptDict['STRIKE_SELECT_BY_PERCENT'] = row[26]
        usrStrySbcptDict['STRIKE_SELECT_BY_ATM'] = row[27]
        usrStrySbcptDict['TECH_INDICATOR_INTERVAL'] = row[28]
        usrStrySbcptDict['REV_PATTERN_CHECK_FLAG'] = row[29]
        usrStrySbcptDict['TRADE_START_TIME'] = row[30]
        usrStrySbcptDict['TRADE_END_TIME'] = row[31]
        usrStrySbcptDict['STRIKE_TYPE'] = row[32]
        usrStrySbcptDict['OPTIONS_ELIGIBILITY_CONDITIONS'] = row[33]
        usrStrySbcptDict['DUPLICATE_POS_ALLOWED_FLAG'] = row[34]
        usrStrySbcptDict['MIN_DAYS_TO_EXPIRY_FOR_NEW_TRADE'] = row[35]        
        usrStrySbcptDict['HEDGING_REQUIRED_FLAG'] = row[36]   
        usrStrySbcptDict['HEDGING_STRIKE_SELECT_PERCENT'] = row[37]  
        usrStrySbcptDict['EOD_EXIT_FLAG'] = row[38]  
        usrStrySbcptDict['STRATEGY_ID'] = row[39]  
        
        usrStrySbcptList.append(usrStrySbcptDict)
        usrStrySbcptDict = {}
    return usrStrySbcptList


def get_strategy_setttings_for_exit(mySQLCursor, strategyId):
    selectStatment = f"SELECT USS.TRADE_ACCOUNT, UTA.BROKER, USS.CONFIRMATION_INTERVAL_TIME, USS.EXIT_INTERVAL_TIME, \
                        SSM.EXIT_BUY_CONDITION, SSM.EXIT_SELL_CONDITION, USS.EXIT_STRATEGY_ID, UTA.UAT_FLAG, UTA.TELEGRAM_USER_NAME, \
                        USS.STRIKE_SELECT_BY_PERCENT, USS.STRIKE_TYPE, USS.CONTRACT_TYPE, USS.LOT_SIZE_MULTIPLIER, USS.DUPLICATE_POS_ALLOWED_FLAG, USS.TGT_PROFIT_PCT, USS.TGT_STOP_LOSS_PCT \
                        FROM USR_TRADE_ACCOUNTS UTA LEFT JOIN USR_STRATEGY_SUBSCRIPTIONS USS ON USS.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT \
                        LEFT JOIN SYS_STRATEGY_MASTER SSM ON SSM.STRATEGY_ID= USS.STRATEGY_ID AND  SSM.ACTIVE_FLAG='ACTIVE'  \
                        WHERE UTA.ACTIVE_FLAG='Y' AND UTA.ACCOUNT_TYPE = 'LIVE' AND USS.STRATEGY_ID ='{strategyId}'"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    usrStrySbcptDict = {}
    usrStrySbcptList = []
    # gets the number of rows affected by the command executed
    for row in results:        
        usrStrySbcptDict['TRADE_ACCOUNT'] = row[0]
        usrStrySbcptDict['BROKER'] = row[1]        
        usrStrySbcptDict['CONFIRMATION_INTERVAL_TIME'] = row[2]
        usrStrySbcptDict['EXIT_INTERVAL_TIME'] = row[3]           
        usrStrySbcptDict['EXIT_BUY_CONDITION'] = row[4]                
        usrStrySbcptDict['EXIT_SELL_CONDITION'] = row[5]       
        usrStrySbcptDict['EXIT_STRATEGY_ID'] = row[6]
        usrStrySbcptDict['UAT_FLAG'] = row[7]    
        usrStrySbcptDict['TELEGRAM_USER_NAME'] = row[8] 
        usrStrySbcptDict['STRIKE_SELECT_BY_PERCENT'] = row[9] 
        usrStrySbcptDict['STRIKE_TYPE'] = row[10] 
        usrStrySbcptDict['CONTRACT_TYPE'] = row[11] 
        usrStrySbcptDict['LOT_SIZE_MULTIPLIER'] = row[12] 
        usrStrySbcptDict['DUPLICATE_POS_ALLOWED_FLAG'] = row[13]
        usrStrySbcptDict['TGT_PROFIT_PCT'] = row[14]          
        usrStrySbcptDict['TGT_STOP_LOSS_PCT'] = row[15]
        
        usrStrySbcptList.append(usrStrySbcptDict)
        usrStrySbcptDict = {}
    return usrStrySbcptList





def get_usr_strategy_setttings(mySQLCursor, strategyId, accountType):

    if (accountType == 'PAPER'):
        selectStatment = "SELECT USS.CAPITAL_ALLOCATION, USS.TOTAL_POSITION_LIMIT, USS.DAY_POSITION_LIMIT, USS.POSITION_SIZE, \
                            USS.EXIT_STRATEGY_ID, USS.TGT_PROFIT_AMT, USS.TGT_PROFIT_PCT, USS.TGT_STOP_LOSS_AMT, USS.TGT_STOP_LOSS_PCT,\
                                USS.TRAILING_THRESHOLD_PCT, USS.SYSTEM_OVERRIDE_FLAG, USS.STRATEGY_ON_OFF_FLAG, USS.EXIT_OPEN_ORDERS_FLAG,\
                                    USS.EXIT_ALL_POSITIONS_FLAG, USS.EXIT_ALL_POSITIONS_FLAG_ALERT, USS.EXIT_OPEN_ORDERS_FLAG_ALERT, USS.ACTIVE_FLAG,\
                                        USS.SUBSCRIBED_ON, USS.TRADE_ACCOUNT, UTA.BROKER, UTA.AVAILABLE_MARGIN, USS.PRODUCT_ORDER_TYPE, USS.PER_INDUSTRY_CAP_LIMIT_PCT FROM USR_STRATEGY_SUBSCRIPTIONS USS \
                                            LEFT JOIN USR_TRADE_ACCOUNTS UTA ON USS.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT WHERE \
                                                USS.STRATEGY_ID='"+str(strategyId)+"' AND USS.STRATEGY_ON_OFF_FLAG ='ON' AND USS.ACTIVE_FLAG ='ACTIVE' \
                                                    AND USS.TRADE_ACCOUNT IN (SELECT TRADE_ACCOUNT FROM USR_TRADE_ACCOUNTS WHERE ACCOUNT_TYPE = 'PAPER')"
    else:
        selectStatment = "SELECT USS.CAPITAL_ALLOCATION, USS.TOTAL_POSITION_LIMIT, USS.DAY_POSITION_LIMIT, USS.POSITION_SIZE, \
                            USS.EXIT_STRATEGY_ID, USS.TGT_PROFIT_AMT, USS.TGT_PROFIT_PCT, USS.TGT_STOP_LOSS_AMT, USS.TGT_STOP_LOSS_PCT,\
                                USS.TRAILING_THRESHOLD_PCT, USS.SYSTEM_OVERRIDE_FLAG, USS.STRATEGY_ON_OFF_FLAG, USS.EXIT_OPEN_ORDERS_FLAG,\
                                    USS.EXIT_ALL_POSITIONS_FLAG, USS.EXIT_ALL_POSITIONS_FLAG_ALERT, USS.EXIT_OPEN_ORDERS_FLAG_ALERT, USS.ACTIVE_FLAG,\
                                        USS.SUBSCRIBED_ON, USS.TRADE_ACCOUNT, UTA.BROKER, UTA.AVAILABLE_MARGIN, USS.PRODUCT_ORDER_TYPE, USS.PER_INDUSTRY_CAP_LIMIT_PCT FROM USR_STRATEGY_SUBSCRIPTIONS USS \
                                            LEFT JOIN USR_TRADE_ACCOUNTS UTA ON USS.TRADE_ACCOUNT= UTA.TRADE_ACCOUNT WHERE \
                                                USS.STRATEGY_ID='"+str(strategyId)+"' AND USS.STRATEGY_ON_OFF_FLAG ='ON' AND USS.ACTIVE_FLAG ='ACTIVE' \
                                                    AND USS.TRADE_ACCOUNT IN (SELECT TRADE_ACCOUNT FROM USR_TRADE_ACCOUNTS WHERE ACCOUNT_TYPE = 'LIVE')"




    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    usrStrySbcptDict = {}
    usrStrySbcptList = []
    # gets the number of rows affected by the command executed
    for row in results:        
        usrStrySbcptDict['CAPITAL_ALLOCATION'] = row[0] 
        usrStrySbcptDict['TOTAL_POSITION_LIMIT'] = row[1] 
        usrStrySbcptDict['DAY_POSITION_LIMIT'] = row[2] 
        usrStrySbcptDict['POSITION_SIZE'] = row[3] 
        usrStrySbcptDict['EXIT_STRATEGY_ID'] = row[4] 
        usrStrySbcptDict['TGT_PROFIT_AMT'] = row[5] 
        usrStrySbcptDict['TGT_PROFIT_PCT'] = row[6] 
        usrStrySbcptDict['TGT_STOP_LOSS_AMT'] = row[7] 
        usrStrySbcptDict['TGT_STOP_LOSS_PCT'] = row[8] 
        usrStrySbcptDict['TRAILING_THRESHOLD_PCT'] = row[9] 
        usrStrySbcptDict['SYSTEM_OVERRIDE_FLAG'] = row[10] 
        usrStrySbcptDict['STRATEGY_ON_OFF_FLAG'] = row[11] 
        usrStrySbcptDict['EXIT_OPEN_ORDERS_FLAG'] = row[12] 
        usrStrySbcptDict['EXIT_ALL_POSITIONS_FLAG'] = row[13] 
        usrStrySbcptDict['EXIT_ALL_POSITIONS_FLAG_ALERT'] = row[14] 
        usrStrySbcptDict['EXIT_OPEN_ORDERS_FLAG_ALERT'] = row[15] 
        usrStrySbcptDict['ACTIVE_FLAG'] = row[16] 
        usrStrySbcptDict['SUBSCRIBED_ON'] = row[17]          
        usrStrySbcptDict['TRADE_ACCOUNT'] = row[18]
        usrStrySbcptDict['BROKER'] = row[19]
        usrStrySbcptDict['AVAILABLE_MARGIN'] = row[20]
        usrStrySbcptDict['PRODUCT_ORDER_TYPE'] = row[21]
        usrStrySbcptDict['PER_INDUSTRY_CAP_LIMIT_PCT'] = row[22]
        usrStrySbcptList.append(usrStrySbcptDict)
        usrStrySbcptDict = {}

    return usrStrySbcptList

def get_inst_selection_query(mySQLCursor, strategyId):
    instSelectionQuery = ""
    # Send list of all FNO stock as return in case strategy ID is not provided
    if (strategyId is None):
        instSelectionQuery = "SELECT INSTRUMENT_TOKEN, TRADING_SYMBOL, STOCK_NAME, EXCHANGE_TOKEN FROM MARKET_PERFORMANCE_TBL WHERE FNO = 'FNO' and CATEGORY='Stocks' \
        and DATE= (SELECT MAX(DATE) FROM MARKET_PERFORMANCE_TBL ORDER BY DATE DESC) ORDER BY KST_RATIO DESC"
    else:
        selectStatment = "SELECT INST_SELECTION_QUERY FROM SYS_INST_SELECTION WHERE INST_SELECTION_ID = (SELECT INST_SELECTION_ID FROM SYS_STRATEGY_MASTER WHERE STRATEGY_ID='"+str(strategyId)+"')"  
        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()
        
        for row in results:        
            instSelectionQuery = row[0]

    return instSelectionQuery


def get_pre_defined_inst_list(mySQLCursor, strategyId):
    instSelectionQuery = "SELECT INSTRUMENT_TOKEN, TRADING_SYMBOL, STOCK_NAME, EXCHANGE_TOKEN FROM SHORT_LISTED_INSTRUMENTS WHERE STRATEGY_ID='"+str(strategyId)+"' AND DATE = (SELECT TRADING_DATE FROM LAST_TRADING_DATE) ORDER BY SLI_ID"   
    mySQLCursor.execute(instSelectionQuery)
    return mySQLCursor.fetchall()



def insert_pattern_analysis(cnx, mySQLCursor, configDict, insertPaternAnalysis):
       
    # Insert pattern analysis data
    try:   
        cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
        insertQuery = "INSERT INTO PATTERN_ANALYSIS \
                (UPDATED_ON, INSTRUMENT_TOKEN, TRADE_SYMBOL, STOCK_NAME, CURRENT_MKT_PRICE, TRENDLINE_PRICE, REVERSAL_BUY_SELL, BUY_FAILED_SWING, \
                    BUY_DOUBLE_BOTTOM, BUY_NON_FAILURE_SWING, BUY_RISING_CHANNEL, BUY_HORIZONDAL_CHANNEL, HEAD_AND_SHOULDER, \
                    INVERTED_HEAD_AND_SHOULDER, BROADENING_TOP, BROADENING_BOTTOM, TRIANGLE_TOP, TRIANGLE_BOTTOM, RECTANGLE_TOP, \
                    RECTANGLE_BOTTOM, CUP_AND_HANDLE, BULLISH_REVERSAL_SCORE, BULLISH_REVERSAL_SCORE_PCT, INTERVAL_TIME,\
                    HT_CYCLE_SCORE, CDL_MAX_SCORE, CDL_TOTAL_SCORE, PRECEDING_TREND_SCORE, TREND_LEVEL_SCORE , OVER_EXTENSION_SCORE, \
                    RETRACEMENT_LEVEL_SCORE, VOLUME_OPEN_INTERST_SCORE, SELLING_CLIMAX_SCORE, SIGNAL_DATE_TIME, STACKED_EMA_SCORE, \
                    TTMS_SCORE, SUPPORT_SCORE, BULLISH_DIVERGENCE_SCORE, TTMS_LENGTH, STACKED_EMA_SELL_SCORE, BELOW_BUY_PRICE, ABOVE_SELL_PRICE, \
                    ATR_BASED_BUY_EXIT, ATR_BASED_SELL_EXIT,EMAS_LENGTH, ACTUAL_BUY_SELL, PROFIT_PCT, TRADE_VALUE) \
                    VALUES(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            
       

        mySQLCursor.executemany(insertQuery, insertPaternAnalysis)
        cnx.commit()
    except Exception as e:
        logging.info('ERROR: Unable to insert pattern analysis data: ' + str(e))
    logging.info("--------------------------------------------------")



def get_advance_decline_ratio(kite, instrumentToken, interval, patternSignalList = [], patternSignalDict = {}):
    
    toDate = util.get_lookup_date(0)
    fromDate = ''
    
    if (interval == '15minute'):
        fromDate = util.get_lookup_date(10)
    elif (interval == '30minute'):
        fromDate = util.get_lookup_date(15)
    elif (interval == 'day'):
        fromDate = util.get_lookup_date(300)

    histRecords = baf.get_historical_data(kite, instrumentToken, fromDate, toDate, interval)

    df = pd.DataFrame(histRecords)

    extrema, prices, smooth_extrema, smooth_prices = cp.find_extrema(
        df, bw="cv_ls"
    )

    patterns, patternDict  = cp.find_patterns(extrema, prices, max_bars=0)
    
    patternSignalDict[str(instrumentToken)] = ''

    for name, pattern_periods in patterns.items():            
        if (name == 'Buy' or name == 'Sell'):
            patternSignalList.append(name)
            patternSignalDict[str(instrumentToken)] = name
            break

    return patternSignalList, patternSignalDict

def get_only_pattern_signal(kite, instrumentToken, interval):
    
    toDate = util.get_lookup_date(0)
    fromDate = util.get_from_date_based_interval(interval)
    histRecords = baf.get_historical_data(kite, instrumentToken, fromDate, toDate, interval)
    df = pd.DataFrame(histRecords)
    extrema, prices, smooth_extrema, smooth_prices = cp.find_extrema(
        df, bw="cv_ls"
    )

    patterns, trendLinePrice  = cp.find_patterns(extrema, prices, max_bars=20)
    patternSignal = ''
    for name, pattern_periods in patterns.items():            
        if (name == 'Buy' or name == 'Sell'):
            patternSignal = name
            break     

    return patternSignal   


def get_patterns_signal(kite, instrumentToken, interval, stockName, tradeSymbol):
    
    toDate = util.get_lookup_date(0)
    fromDate = util.get_from_date_based_interval(interval)
    currDateTime = util.get_date_time_formatted('%Y-%m-%d %H:%M:%S')    
    patternSignal = ""

    histRecords = baf.get_historical_data(kite, instrumentToken, fromDate, toDate, interval)
    df = pd.DataFrame(histRecords)
    extrema, prices, smooth_extrema, smooth_prices = cp.find_extrema(
        df, bw="cv_ls"
    )

    lastMktPrice = prices.tail(1).values[0]
    patterns, trendLinePrice  = cp.find_patterns(extrema, prices, max_bars=100)
    reversalScoreDict = tsc.get_bullish_reversal_score(df, extrema, prices, interval, stockName)
    insertVal = []     
    if (len(reversalScoreDict) > 0):
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

        
        logging.info(f"Call: {str(patternSignal)} at {str(currDateTime)} and the current price {str(lastMktPrice)} ; interval: {str(interval)}; bullishReversalScore: {str(reversalScoreDict['netScore'])}")

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
        insertVal.insert(len(insertVal), str(updatedOn))
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
        


        return patternSignal, lastMktPrice, insertVal, reversalScoreDict
    else:
        return "", 0, insertVal, reversalScoreDict




def update_pattern_list(patternDataList1, patternDataList2, patternDataList3, futTransactionType, profitPercent, futBuyValue):
    # Mention that it is actual buy or sell in the pattern analysis data
    if (len(patternDataList1) > 0):
        patternDataList1.insert(len(patternDataList1), str(futTransactionType))
        patternDataList1.insert(len(patternDataList1), str(profitPercent))
        patternDataList1.insert(len(patternDataList1), str(futBuyValue))
        
    
    # Mention that it is actual buy or sell in the pattern analysis data
    if (len(patternDataList2) > 0):
        patternDataList2.insert(len(patternDataList2), str(futTransactionType))
        patternDataList2.insert(len(patternDataList2), str(profitPercent))
        patternDataList2.insert(len(patternDataList2), str(futBuyValue))
    
    # Mention that it is actual buy or sell in the pattern analysis data
    if (len(patternDataList3) > 0):
        patternDataList3.insert(len(patternDataList3), str(futTransactionType))
        patternDataList3.insert(len(patternDataList3), str(profitPercent))
        patternDataList3.insert(len(patternDataList3), str(futBuyValue))                        

    return patternDataList1, patternDataList2, patternDataList3


# Exit plan for close follow, and then follow trailing based on target
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


# Exit plan for close follow, and guranteed 3% up or down
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


# INSTRUMENT_TOKEN=6401 AND
def get_user_defined_inst_list(mySQLCursor, strategyId, accountId, placeExposureFlag = False):

    if (placeExposureFlag):
        instSelectionQuery = f"SELECT TT.BASE_INSTRUMENT_TOKEN, TT.BASE_TRADE_SYMBOL, TT.BASE_TRADE_SYMBOL, TT.POSITIONS_GROUP_ID, TT.EXCHANGE_TOKEN FROM TRADE_TRANSACTIONS AS TT \
            LEFT JOIN TRANSACTIONS_ADDITIONAL_DATA TAD ON TAD.TT_AUTO_ID =TT.AUTO_ID \
            WHERE TT.TRADE_STATUS IN ('OPEN') AND TT.STRATEGY_ID='{strategyId}' AND TT.TRADE_ACCOUNT='{accountId}' AND TAD.IS_IT_HEDGE_POS = 'Y' LIMIT 1"
            
    # Default it queries the open traded transactions only                
    else:
        instSelectionQuery = f"SELECT UDI.INSTRUMENT_TOKEN, UDI.TRADING_SYMBOL, UDI.STOCK_NAME, UDI.EXCHANGE_TOKEN, UDI.EXCHANGE FROM USER_DEFINED_INSTRUMENTS UDI \
            LEFT JOIN TRADE_TRANSACTIONS TT ON UDI.INSTRUMENT_TOKEN = TT.BASE_INSTRUMENT_TOKEN WHERE UDI.STRATEGY_ID='{str(strategyId)}' AND UDI.TRADE_ACCOUNT='{str(accountId)}' AND (TT.BASE_INSTRUMENT_TOKEN IS NULL OR TT.BASE_INSTRUMENT_TOKEN NOT IN (SELECT BASE_INSTRUMENT_TOKEN FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS IN ('OPEN','PENDING') AND STRATEGY_ID='{strategyId}' AND TRADE_ACCOUNT='{accountId}')) GROUP BY UDI.INSTRUMENT_TOKEN"
   
    mySQLCursor.execute(instSelectionQuery)
    return mySQLCursor.fetchall()
    

def check_hedging_status(mySQLCursor, strategyId, accountId):

    selectStatment = f"SELECT TT.TRADE_STATUS FROM TRADE_TRANSACTIONS AS TT \
            LEFT JOIN TRANSACTIONS_ADDITIONAL_DATA TAD ON TAD.TT_AUTO_ID =TT.AUTO_ID \
            WHERE TT.TRADE_STATUS IN ('PENDING', 'P-OPEN', 'OPEN') AND TT.STRATEGY_ID='{strategyId}' AND TT.TRADE_ACCOUNT='{accountId}' AND TAD.IS_IT_HEDGE_POS = 'Y'"
    
    mySQLCursor.execute(selectStatment)
    rowCount = mySQLCursor.rowcount
    placeHedgeFlag = False
    hedgingOrderPending = False
    placeExposureFlag = False
    
    if rowCount == 0:        
        placeHedgeFlag = True

    elif rowCount > 0:        
        results = mySQLCursor.fetchall()        
        for row in results:
            tradeStatus = row[0]    
            if (tradeStatus != 'OPEN'):
                hedgingOrderPending = True
                break
        
        if (not(hedgingOrderPending)):
            selectStatment = f"SELECT TT.TRADE_STATUS FROM TRADE_TRANSACTIONS AS TT \
            LEFT JOIN TRANSACTIONS_ADDITIONAL_DATA TAD ON TAD.TT_AUTO_ID =TT.AUTO_ID \
            WHERE TT.TRADE_STATUS IN ('PENDING', 'P-OPEN', 'OPEN') AND TT.STRATEGY_ID='{strategyId}' AND TT.TRADE_ACCOUNT='{accountId}' AND TAD.IS_IT_HEDGE_POS = 'N'"
    
            mySQLCursor.execute(selectStatment)
            rowCount = mySQLCursor.rowcount
            if rowCount == 0:
                placeExposureFlag = True

    return placeHedgeFlag, placeExposureFlag


def get_test_inst_list(mySQLCursor):
    instSelectionQuery = "SELECT INSTRUMENT_TOKEN, TRADING_SYMBOL, STOCK_NAME, RE_ENTRY_FLAG FROM MARKET_PERFORMANCE_TBL WHERE FNO = 'FNO' and DATE = (SELECT TRADING_DATE FROM LAST_TRADING_DATE) ORDER BY TRADING_SYMBOL"
    mySQLCursor.execute(instSelectionQuery)
    return mySQLCursor.fetchall()

def get_all_fno_inst_list(mySQLCursor, limitCnt = None):
    if (limitCnt == None):
        instSelectionQuery = "SELECT INSTRUMENT_TOKEN, TRADINGSYMBOL, NAME FROM STOCK_UNIVERSE WHERE FNO = 'FNO' ORDER BY TRADINGSYMBOL"
    else:
        instSelectionQuery = f"SELECT INSTRUMENT_TOKEN, TRADINGSYMBOL, NAME FROM STOCK_UNIVERSE WHERE FNO = 'FNO' ORDER BY TRADINGSYMBOL LIMIT {limitCnt}"

    mySQLCursor.execute(instSelectionQuery)
    return mySQLCursor.fetchall()

def get_dynamic_insturments_list(mySQLCursor, strategyId):            

    selectStatment = get_inst_selection_query(mySQLCursor, strategyId)
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()      
    return results

def get_trigger_price(high5Price, low5Price, lastPrice):
    triggerPrice = round(((0.80 * high5Price)+ 0.20 * low5Price), 1)

    if ((triggerPrice <= lastPrice) and ((triggerPrice/lastPrice) >= 0.9975)):
        triggerPrice = round((lastPrice * 0.997), 1)
    elif ((triggerPrice >= lastPrice) and ((triggerPrice/lastPrice) <= 1.0025)):
        triggerPrice = round((lastPrice * 1.003), 1)

    return triggerPrice

def check_existing_orders(mySQLCursor, instrumentToken = None, strategyId = None, accountId = None, accountType = None, transactionType = None, autoId = None):

    currDate = get_date_time_formatted("%Y-%m-%d")
    if (transactionType == "SELL"):
        selectStatment = "SELECT INSTRUMENT_TOKEN FROM TRADE_TRANSACTIONS WHERE AUTO_ID=" + str(autoId) + " AND \
                            ((SELL_ORDER_STATUS ='PENDING' OR SELL_ORDER_STATUS ='COMPLETED') OR \
                            ((SELL_ORDER_STATUS ='ABANDONED' OR SELL_ORDER_STATUS ='TIMED OUT') AND DATE(SELL_ORDER_DATE) = '"+str(currDate)+"'))"

        mySQLCursor.execute(selectStatment)
        # gets the number of rows affected by the command executed
        rowCount = mySQLCursor.rowcount
        orderExistFlag = False
        if rowCount != 0:        
            orderExistFlag = True   
        
        return orderExistFlag 
    else:
        selectStatment = "SELECT INSTRUMENT_TOKEN FROM TRADE_TRANSACTIONS WHERE BASE_INSTRUMENT_TOKEN='" + \
            instrumentToken + "' AND (TRADE_ACCOUNT= '"+ str(accountId) + "')  AND \
                ((TRADE_STATUS ='PENDING' OR TRADE_STATUS ='OPEN' OR TRADE_STATUS ='P-OPEN') OR (TRADE_STATUS= 'EXITED' AND DATE(SELL_ORDER_DATE) = '"+str(currDate)+"') OR (BUY_ORDER_STATUS= 'REJECTED' AND DATE(BUY_ORDER_DATE) = '"+str(currDate)+"'))"
        
        if (accountType == 'PAPER'):
            selectStatment = "SELECT INSTRUMENT_TOKEN FROM PAPER_TRADE_TRANSACTIONS WHERE BASE_INSTRUMENT_TOKEN='" + \
                instrumentToken + "' AND (TRADE_ACCOUNT= '"+ str(accountId) + "')  AND \
                    ((ORDER_STATUS ='ACTIVE' OR ORDER_STATUS= 'TRIGGERED' OR ORDER_STATUS= 'POSITION' OR ORDER_STATUS= 'OPEN') OR (ORDER_STATUS= 'EXITED' AND DATE(SELL_ORDER_DATE) = '"+str(currDate)+"')) "


        mySQLCursor.execute(selectStatment)
        # gets the number of rows affected by the command executed
        rowCount = mySQLCursor.rowcount
        orderExistFlag = False
        if rowCount != 0:        
            orderExistFlag = True   
        
        return orderExistFlag


def get_no_of_day_positions(mySQLCursor, strategyId, accountId, accountType = None): 
    currDate = get_date_time_formatted("%Y-%m-%d")
    selectStatment = "SELECT count(*) FROM TRADE_TRANSACTIONS WHERE DATE(BUY_ORDER_DATE) = '"+str(currDate)+"' AND \
        TRADE_STATUS IN ('PENDING', 'OPEN', 'EXITED') AND TRADE_ACCOUNT ='"+str(accountId)+"' AND STRATEGY_ID='"+str(strategyId)+"'"    

    if (accountType == 'PAPER'):
        selectStatment = "SELECT count(*) FROM PAPER_TRADE_TRANSACTIONS WHERE DATE(BUY_ORDER_DATE) = '"+str(currDate)+"' \
            AND ORDER_STATUS IN ('ACTIVE', 'TRIGGERED','OPEN', 'EXITED') AND TRADE_ACCOUNT ='"+str(accountId)+"' AND STRATEGY_ID='"+str(strategyId)+"'"
    
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    noOfActiveTrades = 0
    for row in results:
        noOfActiveTrades = row[0]

    return noOfActiveTrades

def get_total_no_of_positions(mySQLCursor, strategyId, accountId, accountType = None): 
    selectStatment = "SELECT count(*) FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS IN ('PENDING', 'OPEN') AND \
        TRADE_ACCOUNT ='"+str(accountId)+"' AND STRATEGY_ID='"+str(strategyId)+"'"
    if (accountType == 'PAPER'):
        selectStatment = "SELECT count(*) FROM PAPER_TRADE_TRANSACTIONS WHERE ORDER_STATUS IN ('ACTIVE', 'TRIGGERED','OPEN') \
            AND TRADE_ACCOUNT ='"+str(accountId)+"' AND STRATEGY_ID='"+str(strategyId)+"'"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    noOfActiveTrades = 0
    for row in results:
        noOfActiveTrades = row[0]

    return noOfActiveTrades

def get_utilized_cap_per_industry(mySQLCursor, accountId, instrumentToken):

    selectStatment = "SELECT SUM(BUY_VALUE) FROM TRADE_TRANSACTIONS TT LEFT JOIN STOCK_UNIVERSE SU ON TT.BASE_INSTRUMENT_TOKEN = SU.INSTRUMENT_TOKEN  \
                        WHERE SU.INDUSTRY IN (SELECT INDUSTRY FROM STOCK_UNIVERSE WHERE INSTRUMENT_TOKEN="+str(instrumentToken)+") AND TT.TRADE_ACCOUNT ='"+str(accountId)+"' AND TT.TRADE_STATUS IN ('PENDING', 'OPEN', 'P-OPEN') GROUP BY SU.INDUSTRY"       
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    utilizeCaptial = 0
    for row in results:
        utilizeCaptial = row[0]

    
    if (utilizeCaptial is not None):        
        return float(utilizeCaptial)
    else:
        return 0

def get_total_capital_allocation(mySQLCursor, accountId):
    selectStatment = "SELECT SUM(CAPITAL_ALLOCATION) FROM USR_STRATEGY_SUBSCRIPTIONS WHERE TRADE_ACCOUNT ='"+str(accountId)+"'"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    totalCaptial = 0
    for row in results:
        totalCaptial = float(row[0])
    
    if (totalCaptial is not None):        
        return totalCaptial
    else:
        return 0

def get_utilized_capital(mySQLCursor, accountId, strategyId = None, accountType =  None):
    selectStatment = "SELECT SUM(BUY_VALUE) FROM TRADE_TRANSACTIONS WHERE TRADE_STATUS IN ('PENDING', 'OPEN', 'P-OPEN') AND \
        TRADE_ACCOUNT ='"+str(accountId)+"' AND STRATEGY_ID='"+str(strategyId)+"'"

    if (accountType == 'PAPER'):
        selectStatment = "SELECT SUM(BUY_VALUE) FROM PAPER_TRADE_TRANSACTIONS WHERE ORDER_STATUS IN ('ACTIVE', 'TRIGGERED','OPEN') \
            AND TRADE_ACCOUNT ='"+str(accountId)+"' AND STRATEGY_ID='"+str(strategyId)+"'"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    utilizeCaptial = 0
    for row in results:
        utilizeCaptial = row[0]
    
    if (utilizeCaptial is not None):        
        return utilizeCaptial
    else:
        return 0


def calc_quantity(closePrice, marginAllowed):

    return int(int(marginAllowed)/int(float(closePrice)))

# (cnx, mySQLCursor, kite, chatId, tradeVariables, transactionType, tradeSymbol, lastPrice, smaDistance, instrumentToken, buySellScore, high5Price, low5Price, stockName, tradeTag, strategyName):
def place_gtt_order(kite, quantity, transactionType, tradeSymbol, triggerPrice, lastPrice, stockName):
    triggerId = 0
    orderRemarks = ''
    try:
        gttOrders = [{"transaction_type": transactionType, "quantity": int(quantity), "price": float(triggerPrice), "order_type": kite.ORDER_TYPE_LIMIT, "product": kite.PRODUCT_CNC}]

        orderResponse = kite.place_gtt(trigger_type="single",
                                    tradingsymbol=tradeSymbol,
                                    exchange=kite.EXCHANGE_NSE,
                                    trigger_values=[float(triggerPrice)],
                                    last_price=float(lastPrice),
                                    orders=gttOrders
                                    )

        triggerId = orderResponse["trigger_id"]            
        orderRemarks = "GTT ORDER PLACED"
        logging.info(transactionType + " ORDER PLACED ID IS: " + str(triggerId))                    

    except Exception as e:
        logging.info(transactionType + " ORDER PLACEMENT FAILED: " + str(e))
        orderRemarks = str(e)

    return triggerId, orderRemarks

def insert_paper_trades(cnx, mySQLCursor, ptDataDict): 
    try:
        insertVal = []
        updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        insertQuery = "INSERT INTO PAPER_TRADE_TRANSACTIONS (TRIGGER_DATE, INSTRUMENT_TOKEN, TRADE_SYMBOL, QUANTITY, ORDER_STATUS,UPDATED_ON,\
                    CURRENT_MKT_PRICE, STRATEGY_ID,TRIGGER_PRICE,STOCK_NAME,BUY_VALUE, TRADE_ACCOUNT, BUY_OR_SELL, \
                    BUY_ORDER_DATE, BUY_ORDER_PRICE, SIGNAL_ID, TARGET_PRICE, STOP_LOSS_PRICE,  \
                    HORIZON, TARGET_PERCENT, STOP_LOSS_PERCENT) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        
        insertVal.insert(0, str(ptDataDict['triggerDate']))       
        insertVal.insert(1, str(ptDataDict['instrumentToken']))
        insertVal.insert(2, str(ptDataDict['tradeSymbol']))
        insertVal.insert(3, str(ptDataDict['quantity']))
        insertVal.insert(4, str(ptDataDict['orderStatus']))
        insertVal.insert(5, str(updatedOn))       
        insertVal.insert(6, str(ptDataDict['lastPrice']))       
        insertVal.insert(7, str(ptDataDict['strategyId']))
        insertVal.insert(8, str(ptDataDict['triggerPrice']))
        insertVal.insert(9, str(ptDataDict['stockName']))
        insertVal.insert(10, str(ptDataDict['buyValue']))
        insertVal.insert(11, str(ptDataDict['accountId']))
        insertVal.insert(12, str(ptDataDict['transactionType']))
        insertVal.insert(13, str(updatedOn))
        insertVal.insert(14, str(ptDataDict['lastPrice']))        
        insertVal.insert(15, str(ptDataDict['signalId']))
        insertVal.insert(16, str(ptDataDict['targetPrice']))
        insertVal.insert(17, str(ptDataDict['stopLossPrice']))
        insertVal.insert(18, str(ptDataDict['horizon']))
        insertVal.insert(19, str(ptDataDict['targetPercent']))
        insertVal.insert(20, str(ptDataDict['stopLossPercent']))    

        mySQLCursor.execute(insertQuery, insertVal)
        cnx.commit()
    except Exception as e:
        logging.info("DB FAILURE: UNABLE TO INSERT: " + str(ptDataDict['tradeSymbol']))
        logging.info(str(e))   
        print(e)     


def insert_cash_order_details(cnx, mySQLCursor,tradeDataDict, productType):
    try: 
        insertVal = []
        updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        
        orderQuery = "INSERT INTO TRADE_TRANSACTIONS (SIGNAL_DATE, SIGNAL_ID, INSTRUMENT_TOKEN, TRADE_SYMBOL, CURRENT_MKT_PRICE, BUY_ORDER_PRICE, BUY_VALUE, QUANTITY, BUY_ORDER_STATUS, UPDATED_ON, \
                        ORDER_REMARKS, STRATEGY_ID, STOCK_NAME, TRADE_ACCOUNT, TGT_PROFIT_AMT, TGT_STOP_LOSS_AMT, \
                        TGT_PROFIT_PCT, TGT_STOP_LOSS_PCT, EXIT_STRATEGY_ID, TRADE_STATUS, BUY_ORDER_ID, BUY_ORDER_DATE, \
                        TRAILING_THRESHOLD_PCT, PRODUCT_TYPE, BASE_INSTRUMENT_TOKEN, EXCHANGE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        insertVal.insert(0, str(tradeDataDict['signalDate']))
        insertVal.insert(1, str(tradeDataDict['signalId']))
        insertVal.insert(2, str(tradeDataDict['instrumentToken']))
        insertVal.insert(3, str(tradeDataDict['tradeSymbol']))
        insertVal.insert(4, str(tradeDataDict['lastPrice']))              
        insertVal.insert(5, str(tradeDataDict['triggerPrice']))                
        insertVal.insert(6, str(tradeDataDict['buyValue']))            
        insertVal.insert(7, str(tradeDataDict['quantity']))
        insertVal.insert(8, str(tradeDataDict['orderStatus']))
        insertVal.insert(9, str(updatedOn))
        insertVal.insert(10, str(tradeDataDict['orderRemarks']))
        insertVal.insert(11, str(tradeDataDict['strategyId']))        
        insertVal.insert(12, str(tradeDataDict['stockName']))        
        insertVal.insert(13, str(tradeDataDict['accountId']))
        insertVal.insert(14, str(tradeDataDict['tgtProfitAmt']))
        insertVal.insert(15, str(tradeDataDict['tgtStopLossAmt']))
        insertVal.insert(16, str(tradeDataDict['tgtProfitPct']))                        
        insertVal.insert(17, str(tradeDataDict['tgtStopLossPct']))
        insertVal.insert(18, str(tradeDataDict['exitStrategyId']))      
        insertVal.insert(19, str(tradeDataDict['orderStatus']))
        insertVal.insert(20, str(tradeDataDict['orderId']))
        insertVal.insert(21, str(updatedOn))
        insertVal.insert(22, str(tradeDataDict['trailingThresholdPct']))
        insertVal.insert(23, str(productType))
        insertVal.insert(24, str(tradeDataDict['instrumentToken']))
        insertVal.insert(25, str(tradeDataDict['exchange']))
        mySQLCursor.execute(orderQuery, insertVal)
        cnx.commit()
    except Exception as e:
        logging.info("DB FAILURE: UNABLE TO INSERT: " + tradeDataDict['tradeSymbol'])
        logging.info(str(e))   
        print(e)     


def insert_options_order_details(cnx, mySQLCursor,tradeDataDict, productType):
    try: 
        insertVal = []
        updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        
        orderQuery = "INSERT INTO TRADE_TRANSACTIONS (SIGNAL_DATE, SIGNAL_ID, INSTRUMENT_TOKEN, TRADE_SYMBOL, CURRENT_MKT_PRICE, BUY_ORDER_PRICE, BUY_VALUE, STOCK_NAME, QUANTITY, BUY_ORDER_STATUS, UPDATED_ON, \
                        ORDER_REMARKS, STRATEGY_ID, TRADE_ACCOUNT, TGT_PROFIT_AMT, TGT_STOP_LOSS_AMT, \
                        TGT_PROFIT_PCT, TGT_STOP_LOSS_PCT, EXIT_STRATEGY_ID, TRADE_STATUS, BUY_ORDER_ID, BUY_ORDER_DATE, \
                        TRAILING_THRESHOLD_PCT, PRODUCT_TYPE, BASE_INSTRUMENT_TOKEN, EXCHANGE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        insertVal.insert(0, str(tradeDataDict['signalDate']))
        insertVal.insert(1, str(tradeDataDict['signalId']))
        insertVal.insert(2, str(tradeDataDict['optionsInstToken']))
        insertVal.insert(3, str(tradeDataDict['optionsTradeSymbol']))
        insertVal.insert(4, str(tradeDataDict['optionsLastPrice']))            
        insertVal.insert(5, str(tradeDataDict['optionsTriggerPrice']))                  
        insertVal.insert(6, str(tradeDataDict['optionsBuyValue']))
        # Stock Name
        insertVal.insert(7, str(tradeDataDict['optionsTradeSymbol']))
            
        insertVal.insert(8, str(tradeDataDict['quantity']))
        insertVal.insert(9, str(tradeDataDict['orderStatus']))
        insertVal.insert(10, str(updatedOn))
        insertVal.insert(11, str(tradeDataDict['orderRemarks']))
        insertVal.insert(12, str(tradeDataDict['strategyId']))  
        insertVal.insert(13, str(tradeDataDict['accountId']))
        insertVal.insert(14, str(tradeDataDict['tgtProfitAmt']))
        insertVal.insert(15, str(tradeDataDict['tgtStopLossAmt']))
        insertVal.insert(16, str(tradeDataDict['tgtProfitPct']))                        
        insertVal.insert(17, str(tradeDataDict['tgtStopLossPct']))
        insertVal.insert(18, str(tradeDataDict['exitStrategyId']))      
        insertVal.insert(19, str(tradeDataDict['orderStatus']))
        insertVal.insert(20, str(tradeDataDict['orderId']))
        insertVal.insert(21, str(updatedOn))
        insertVal.insert(22, str(tradeDataDict['trailingThresholdPct']))
        insertVal.insert(23, str(productType))
        insertVal.insert(24, str(tradeDataDict['instrumentToken']))
        insertVal.insert(25, str(tradeDataDict['exchange']))
        mySQLCursor.execute(orderQuery, insertVal)
        cnx.commit()
    except Exception as e:
        logging.info("DB FAILURE: UNABLE TO INSERT: " + tradeDataDict['tradeSymbol'])
        logging.info(str(e))   
     

def insert_future_order_details(cnx, mySQLCursor, tradeDataDict):
    try:
        insertSuccssFlag = False 
        insertVal = []
        updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        
        orderQuery = "INSERT INTO TRADE_TRANSACTIONS (SIGNAL_DATE, INSTRUMENT_TOKEN, TRADE_SYMBOL, CURRENT_MKT_PRICE, BUY_ORDER_PRICE, STOCK_NAME, BUY_VALUE, QUANTITY, BUY_ORDER_STATUS, UPDATED_ON, \
                        ORDER_REMARKS, STRATEGY_ID, TRADE_ACCOUNT, TGT_PROFIT_PCT, TGT_STOP_LOSS_PCT, EXIT_STRATEGY_ID, TRADE_STATUS, BUY_ORDER_ID, BUY_ORDER_DATE, \
                        TRAILING_THRESHOLD_PCT, BASE_INSTRUMENT_TOKEN, EXCHANGE, INSTRUMENT_TYPE, TRADE_DIRECTION, POSITIONS_GROUP_ID, UAT_FLAG, BROKER, EXPIRY_DATE, \
                        POSITION_DIRECTION, OPTION_STRIKE_PRICE, CONTRACT_TYPE, PRODUCT_TYPE, BASE_TRADE_SYMBOL, ROLL_OVER_FLAG, TRADE_SEQUENCE, ADJUSTMENT_ORDER_FLAG, EXCHANGE_TOKEN, BASE_EXCHANGE_TOKEN) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        insertVal.insert(0, str(tradeDataDict['signalDate']))
        insertVal.insert(1, str(tradeDataDict['futInstToken']))
        insertVal.insert(2, str(tradeDataDict['futTradeSymbol']))
        insertVal.insert(3, str(tradeDataDict['futLastPrice']))            
        insertVal.insert(4, str(tradeDataDict['futTriggerPrice']))  
        # Stock Name
        insertVal.insert(5, str(tradeDataDict['futTradeSymbol']))
        if (tradeDataDict['futTransactionType'] == 'SELL'):            
            insertVal.insert(6, str(tradeDataDict['futBuyValue'] * -1))
            insertVal.insert(7, str(tradeDataDict['quantity'] * -1))
        else:
            insertVal.insert(6, str(tradeDataDict['futBuyValue']))            
            insertVal.insert(7, str(tradeDataDict['quantity']))

        insertVal.insert(8, str(tradeDataDict['orderStatus']))
        insertVal.insert(9, str(updatedOn))		
        insertVal.insert(10, str(tradeDataDict['orderRemarks']))
        insertVal.insert(11, str(tradeDataDict['strategyId']))  
        insertVal.insert(12, str(tradeDataDict['accountId']))
        insertVal.insert(13, str(tradeDataDict['tgtProfitPct']))                        
        insertVal.insert(14, str(tradeDataDict['tgtStopLossPct']))
        insertVal.insert(15, str(tradeDataDict['exitStrategyId']))      
        insertVal.insert(16, str(tradeDataDict['orderStatus']))
        insertVal.insert(17, str(tradeDataDict['orderId']))
        insertVal.insert(18, str(updatedOn))
        insertVal.insert(19, str(tradeDataDict['trailingThresholdPct']))
        insertVal.insert(20, str(tradeDataDict['instrumentToken']))
        insertVal.insert(21, str(tradeDataDict['exchange']))

        insertVal.insert(22, str(tradeDataDict['instrumentType']))
        insertVal.insert(23, str(tradeDataDict['futTransactionType']))
        insertVal.insert(24, str(tradeDataDict['positionGroupId']))        
        
        if (tradeDataDict['uatFlag']):
            insertVal.insert(25, str('Y'))  
        else: 
            insertVal.insert(25, str('N'))  
        insertVal.insert(26, str(tradeDataDict['broker']))
        insertVal.insert(27, str(tradeDataDict['rawExpiry']))
        insertVal.insert(28, str(tradeDataDict['positionDirection']))

        insertVal.insert(29, str(tradeDataDict['strikePrice']))
        insertVal.insert(30, str(tradeDataDict['contractType']))
        insertVal.insert(31, str(tradeDataDict['productType']))
        insertVal.insert(32, str(tradeDataDict['baseTradeSymbol']))
        insertVal.insert(33, str(tradeDataDict['rollOverFlag']))
        insertVal.insert(34, str(tradeDataDict['tradeSequence']))
        insertVal.insert(35, str(tradeDataDict['adjustmentOrder']))
        insertVal.insert(36, str(tradeDataDict['futExchangeToken']))
        insertVal.insert(37, str(tradeDataDict['baseExchangeToken']))

        mySQLCursor.execute(orderQuery, insertVal)
        # commit records
        insertedRowId = mySQLCursor.lastrowid        
        cnx.commit()

        insert_additional_transactions_data(cnx, mySQLCursor, insertedRowId, tradeDataDict)
        insertSuccssFlag = True

    except Exception as e:
        logging.info("DB FAILURE: UNABLE TO INSERT: " + tradeDataDict['futTradeSymbol'])
        logging.info(str(e))
                
    return insertSuccssFlag
    

def insert_additional_transactions_data(cnx, mySQLCursor, insertedRowId, tradeDataDict):
    try: 
        updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        
        buildInsertQuery = []
        buildInsertData = []


        if "delta" in tradeDataDict:
            buildInsertQuery.append('TRADED_DELTA')
            buildInsertData.append(tradeDataDict['delta'])
        
        if "theta" in tradeDataDict:
            buildInsertQuery.append('TRADED_THETA')
            buildInsertData.append(tradeDataDict['theta'])
        
        if "vega" in tradeDataDict:
            buildInsertQuery.append('TRADED_VEGA')
            buildInsertData.append(tradeDataDict['vega'])
        
        if "gamma" in tradeDataDict:
            buildInsertQuery.append('TRADED_GAMMA')
            buildInsertData.append(tradeDataDict['gamma'])
        
        if "IMPLIED_VOLATILITY" in tradeDataDict:
            buildInsertQuery.append('TRADED_IMPLIED_VOLATILITY')
            buildInsertData.append(tradeDataDict['IMPLIED_VOLATILITY'])
        
        if "isItHedgePos" in tradeDataDict:
            buildInsertQuery.append('IS_IT_HEDGE_POS')
            buildInsertData.append(tradeDataDict['isItHedgePos'])
        
        if (len(buildInsertQuery) > 0):
            buildInsertQuery.append('TT_AUTO_ID')
            buildInsertData.append(insertedRowId)

            query_placeholders = ', '.join(['%s'] * len(buildInsertQuery))
            query_columns = ', '.join(buildInsertQuery)

            insertQuery = ''' INSERT INTO TRANSACTIONS_ADDITIONAL_DATA (%s) VALUES (%s) ''' %(query_columns, query_placeholders)

            mySQLCursor.execute(insertQuery, buildInsertData)
            cnx.commit()
    except Exception as e:
        logging.info("DB FAILURE: UNABLE TO INSERT IN insert_additional_transactions_data: " + str(insertedRowId))
        logging.info(str(e))   


def bsm_options_pricing(brokerApi, tradeDataDict):
    try:
        interestRate = 6
        futInstToken = str(tradeDataDict['futInstToken'])
        fnoStrikePrice = tradeDataDict['strikePrice'] 
        fnoExpiry = tradeDataDict['rawExpiry']
        stockClosePrice = tradeDataDict['CLOSE-PRICE']
        tradeSymbol = tradeDataDict['tradeSymbol']

        fDate = datetime.datetime.strptime(str(util.get_date_time_formatted("%Y-%m-%d")), "%Y-%m-%d")
        lDate = datetime.datetime.strptime(str(fnoExpiry), "%Y-%m-%d")
        daysToExpiry1 = 1 
        daysToExpiry = lDate - fDate

        if (daysToExpiry.days == 0):
            daysToExpiry1 = 1
        else:
            daysToExpiry1 = int(daysToExpiry.days)
    
        quoteData = prostocksApi.get_quotes(brokerApi, tradeDataDict['futExchangeToken'], exchange='NFO')

        fnoLastPrice = float(quoteData['lp'])
     
        
        if (float(fnoLastPrice) > 0):
            tradeDataDict['OI_VALUE'] = float(quoteData['oi']) * float(stockClosePrice)
            tradeDataDict['VOLUME_VALUE']= float(quoteData['v']) * float(stockClosePrice)
            if (tradeDataDict['instrumentType'] == 'PUT'):
                iv = mibian.BS([stockClosePrice, fnoStrikePrice, interestRate, daysToExpiry1], putPrice=fnoLastPrice)                
                c = mibian.BS([stockClosePrice, fnoStrikePrice, interestRate, daysToExpiry1], volatility=float("{:.2f}".format(iv.impliedVolatility)))
                tradeDataDict['delta'] = float("{:.2f}".format(c.putDelta))
                tradeDataDict['theta'] = float("{:.2f}".format(c.putTheta))
            else:
                iv = mibian.BS([stockClosePrice, fnoStrikePrice, interestRate, daysToExpiry1], callPrice=fnoLastPrice)
                c = mibian.BS([stockClosePrice, fnoStrikePrice, interestRate, daysToExpiry1], volatility=float("{:.2f}".format(iv.impliedVolatility)))
                tradeDataDict['delta'] = float("{:.2f}".format(c.callDelta))
                tradeDataDict['theta'] = float("{:.2f}".format(c.callTheta))
            
            tradeDataDict['vega'] = float("{:.2f}".format(c.vega))
            tradeDataDict['gamma'] = float("{:.5f}".format(c.gamma))

            tradeDataDict['IMPLIED_VOLATILITY']= float("{:.2f}".format(iv.impliedVolatility))
            tradeDataDict['daysToExpiry'] = daysToExpiry1
            tradeDataDict['status'] = 'success'
            tradeDataDict['remarks'] = 'success'               
        else:            
            tradeDataDict['status'] = 'failed'
            tradeDataDict['remarks'] = 'FNO Price is zero for ' + tradeSymbol

    except Exception as e:            
        tradeDataDict['status'] = 'error'
        tradeDataDict['remarks'] = "Errored while getting the BSM Options Pricing for " + tradeSymbol + ": "+ str(e)
        
    return tradeDataDict





def insert_order_details(cnx, mySQLCursor, orderId, orderRemarks, orderStatus, quantity, lastPrice, transactionType, instrumentToken, \
    tradeSymbol, triggerPrice, stockName, strategyId, buyValue, accountId, tgtProfitAmt, tgtProfitPct, tgtStopLossAmt, tgtStopLossPct, \
         exitStrategyId, signalDate, signalId, trailingThresholdPct):
    try: 
        insertVal = []
        updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        
        orderQuery = "INSERT INTO TRADE_TRANSACTIONS (SIGNAL_DATE, SIGNAL_ID, INSTRUMENT_TOKEN, TRADE_SYMBOL, QUANTITY, BUY_ORDER_STATUS, UPDATED_ON, \
                        ORDER_REMARKS, CURRENT_MKT_PRICE, STRATEGY_ID, STOCK_NAME, BUY_VALUE, TRADE_ACCOUNT, TGT_PROFIT_AMT, TGT_STOP_LOSS_AMT, \
                        TGT_PROFIT_PCT, TGT_STOP_LOSS_PCT, EXIT_STRATEGY_ID, TRADE_STATUS, BUY_ORDER_ID, BUY_ORDER_DATE, BUY_ORDER_PRICE, \
                        TRAILING_THRESHOLD_PCT, BASE_INSTRUMENT_TOKEN) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        
        insertVal.insert(0, str(signalDate))
        insertVal.insert(1, str(signalId))
        insertVal.insert(2, str(instrumentToken))
        insertVal.insert(3, str(tradeSymbol))
        insertVal.insert(4, str(quantity))
        insertVal.insert(5, str(orderStatus))
        insertVal.insert(6, str(updatedOn))
        insertVal.insert(7, str(orderRemarks))
        insertVal.insert(8, str(lastPrice))        
        insertVal.insert(9, str(strategyId))        
        insertVal.insert(10, str(stockName))
        insertVal.insert(11, str(buyValue))
        insertVal.insert(12, str(accountId))
        insertVal.insert(13, str(tgtProfitAmt))
        insertVal.insert(14, str(tgtStopLossAmt))
        insertVal.insert(15, str(tgtProfitPct))                        
        insertVal.insert(16, str(tgtStopLossPct))
        insertVal.insert(17, str(exitStrategyId))      
        insertVal.insert(18, str(orderStatus))
        insertVal.insert(19, str(orderId))
        insertVal.insert(20, str(updatedOn))
        insertVal.insert(21, str(lastPrice))                
        insertVal.insert(22, str(trailingThresholdPct))
        insertVal.insert(23, str(instrumentToken))
        mySQLCursor.execute(orderQuery, insertVal)
        cnx.commit()
    except Exception as e:
        logging.info("DB FAILURE: UNABLE TO INSERT: " + tradeSymbol)
        logging.info(str(e))   
        print(e)     

# return profit% and profit amount for given quantity
def get_profit(buyOrderPrice, sellOrderPrice, quantity):
    
    
    profitPercent = ((float(sellOrderPrice)*quantity) - (float(buyOrderPrice)*quantity)) / (quantity*float(buyOrderPrice)) * 100
    profitAmount = ((float(sellOrderPrice)*quantity) - (float(buyOrderPrice)*quantity))
    
    if (quantity < 0):
        profitPercent = profitPercent * -1

    return profitPercent, profitAmount


def get_buy_sell_decision(df, strategy):
    decision = ""
    df['DECISION'] = eval(strategy)
    # ST_RPPR_1
    # df['DECISION'] = np.where(((df['Cdl_Bullish'] > 0.5) & (
    #                             df['SMA3_STOCH_1'] > 25) & (
    #                             df['RSI_MA3_1'] < 35) & (
    #                             df['MACDSIGNAL_OBV'] > df['MACD_OBV'])), 'BUY', '')

    signalRecord = df.tail(1)
    decision = signalRecord['DECISION'].values[0]

    closePrice = signalRecord['close'].values[0]
    if ((decision is not None) and (decision != "") and (decision == "BUY" or decision == "SELL") and closePrice >= 20):        
        
        buySellScore, totalBuyScore, totalSellScore = ts.getBuySellScore(df)
        # totalBuyScore = 120
        # totalSellScore = 120
        if (decision == "BUY" and int(totalBuyScore) >= 100):            
            return signalRecord, decision, totalBuyScore
        elif (decision == "SELL" and int(totalSellScore) <= -100):
            return signalRecord, decision, totalSellScore
        else:
            return signalRecord, "", ""
    else:
        return signalRecord, "", ""
  


def send_alerts(logEnableFlag, cnx, mySQLCursor, chatId, eventId, eventDetails, telgUpdateFlag, strategyId=None, programName=None, stockName=None):

    try:  
        # Send Telegram updates only when the received flag is Y, otherwise ignore
        if (telgUpdateFlag == 'Y'):        
            try:
                token = '1599299147:AAEWO2kk_4trYo0lu12umeJMzMca-4r7IBQ'
                url = 'https://api.telegram.org/bot'+token+'/sendMessage?chat_id='+str(chatId)+'&parse_mode=markdown'+'&text='+str(eventDetails)
                response = requests.get(url)            
            except Exception as e:            
                logging.info('ERROR: Unable to send Telegram message: ' + str(e))

        
        # logger(logEnableFlag, 'info', eventDetails, eventId=eventId)
        logging.info(f"{eventId} : {eventDetails}")

     # This function is used for adding events to MySQL table SYS_ACTIVITY_DETAILS
        if (stockName is not None):     
            eventDetails = stockName

        updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        mySQLCursor.execute("INSERT INTO SYS_ACTIVITY_DETAILS (EVENT_ID, EVENT_DETAILS, UPDATED_ON, PROGRAM_NAME, STRATEGY_ID, USER_TYPE)  VALUES (%s,%s,%s,%s,%s,%s)", (eventId, eventDetails, updatedOn, programName, strategyId, 'SYSTEM'))
        cnx.commit()        
    except Exception as e:
        logging.info('ERROR: Unable to add activity to table (SYS_ACTIVITY_DETAILS): ' + str(e))


def send_usr_alerts(cnx, mySQLCursor, eventId, eventDetails, userName, chatId=None, telgUpdateFlag=None, strategyId=None, programName=None, stockName=None):
    # Send Telegram updates only when the received flag is Y, otherwise ignore
    # if (telgUpdateFlag == 'Y' and chatId is not None):        

    #     send_telegram_message(chatId, eventDetails)

    # This function is used for adding events to MySQL table SYS_ACTIVITY_DETAILS
    
    logging.info(f"{eventId} : {eventDetails}")

    try:   
        if (stockName is not None):     
            eventDetails = stockName

        updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        mySQLCursor.execute("INSERT INTO USR_ACTIVITY_DETAILS (EVENT_ID, EVENT_DETAILS, UPDATED_ON, STRATEGY_ID, TRADE_ACCOUNT, UPDATED_BY)  VALUES (%s,%s,%s,%s,%s,%s)", (eventId, eventDetails, updatedOn, strategyId, userName,'SYSTEM'))
        
        cnx.commit()        
    except Exception as e:
        logging.info('ERROR: Unable to add activity to table (SYS_ACTIVITY_DETAILS): ' + str(e))


def add_logs(cnx, mySQLCursor, eventId, eventDetails, tradeDataDict):
    try:
        strategyId = None
        logsPgmName= os.path.splitext(os.path.basename(__file__))[0]
        accountId = None

        if "programName" in tradeDataDict:
            logsPgmName = tradeDataDict['programName']   
            
        if "strategyId" in tradeDataDict:
            strategyId = tradeDataDict['strategyId']

        if "accountId" in tradeDataDict:
            accountId = tradeDataDict['accountId']
        
        if (strategyId != None and accountId != None):                
            logging.info(f"{eventId} : {logsPgmName} - {strategyId} - {accountId} - {eventDetails}")
        else:
            logging.info(f"{eventId} : {logsPgmName} - {eventDetails}")
    
        if ( eventId != 'INFO'):
            try:
                updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
                mySQLCursor.execute("INSERT INTO SYS_ACTIVITY_DETAILS (EVENT_ID, EVENT_DETAILS, UPDATED_ON, PROGRAM_NAME, STRATEGY_ID, TRADE_ACCOUNT)  VALUES (%s,%s,%s,%s,%s,%s)", (eventId, eventDetails, updatedOn, logsPgmName, strategyId, accountId))
                cnx.commit()
            except Exception as e:        
                logging.info(f"ERROR: Unable to add activity to table (SYS_ACTIVITY_DETAILS): {str(e)}")
            
            if ( eventId == 'ERROR' or eventId == 'ALERT'):            
                send_telegram_message(Config.TELG_ADMIN_ID, eventDetails)        
            
            elif (eventId == 'NOTIFY' and ('telegramUserNames' in tradeDataDict)):
                send_telegram_message(tradeDataDict['telegramUserNames'], eventDetails)
            
            elif (eventId == 'NOTIFY'):
                send_telegram_message(Config.TELG_ADMIN_ID, eventDetails)
    except Exception as e:
        logging.info('ERROR: something broken while updating the logs ' + str(e))
        pass

    
def add_alerts(cnx, mySQLCursor, eventId, eventDetails, telegramUserNames, programName, telgUpdateFlag, tradeDataDict, updateForUser='N', dbUpdateFlag='N'):    
    # Send Telegram updates only when the received flag is Y, otherwise ignore
    # if (telgUpdateFlag == 'Y'):
    #     send_telegram_message(telegramUserNames, eventDetails)

    # This function is used for adding events to MySQL table SYS_ACTIVITY_DETAILS
    
    logging.info(f"{eventId} : {os.path.splitext(os.path.basename(__file__))[0]} - ***{eventDetails}***")

    try:
        strategyId = ''
        accountId = ''

        if (len(tradeDataDict) > 0 and updateForUser == 'Y'):                 
            strategyId = tradeDataDict['strategyId']
            accountId = tradeDataDict['accountId']

        if (dbUpdateFlag == 'Y'):
            updatedOn = get_date_time_formatted("%Y-%m-%d %H:%M:%S")
            mySQLCursor.execute("INSERT INTO USR_ACTIVITY_DETAILS (PROGRAM_NAME, EVENT_ID, EVENT_DETAILS, UPDATED_ON, STRATEGY_ID, TRADE_ACCOUNT, UPDATED_BY)  VALUES (%s,%s,%s,%s,%s,%s,%s)", (programName, eventId, eventDetails, updatedOn, strategyId, accountId, 'SYSTEM'))        
            cnx.commit()
    except Exception as e:
        logging.info('ERROR: Unable to add activity to table (SYS_ACTIVITY_DETAILS): ' + str(e))

def logger(logEnableFlag, logLevel, logMessage, eventId = None):
    if (eventId == 'ALERT'):
        logLevel = 'warning'
    elif (eventId == 'ERROR'):
        logLevel = 'error'
    else:
        logLevel = 'info'

    if (logEnableFlag == True and logLevel == 'info'):
        logging.info(logLevel.upper() + ' : ' + str(logMessage))
    elif (logEnableFlag == True and logLevel == 'debug'):
        logging.debug(logLevel.upper() + ' : ' + str(logMessage))
    elif (logEnableFlag == True and logLevel == 'warning'):
        logging.warning(logLevel.upper() + ' : ' + str(logMessage))
    elif (logEnableFlag == True and logLevel == 'error'):
        logging.error(logLevel.upper() + ' : ' + str(logMessage))



def update_alert_flag(cnx, mySQLCursor,varName):      
    try:
        updateQuery = ("UPDATE SYSTEM_VARIABLES_TBL SET VALUE=1 WHERE VARIABLE='"+str(varName)+"'")
        mySQLCursor.execute(updateQuery)
        cnx.commit()         
        print("commited")   
            
    except Exception as e:    
        logging.info("Errored while updating the SYSTEM_VARIABLES_TBL details")
        logging.info(str(e))    

def load_constant_variables(mySQLCursor, tableName):
    selectStatment = "SELECT VARIABLE, VALUE FROM " + tableName

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    tradeVariables = {}
    # gets the number of rows affected by the command executed
    for row in results:
        tradeVariables[row[0]] = row[1]
    return tradeVariables

def disconnect_db(mySQLCursor, cnx):
    try:
        mySQLCursor.close()
        cnx.close()
    except:
        pass

# def delete_gtt_order(cnx, mySQLCursor, kite, accountId, strategyId, triggerId = None, broker = None):
#     selectStatment = ''    
#     if (triggerId != None):
#         selectStatment = "SELECT TRIGGER_ID FROM TRADE_TRANSACTIONS WHERE ORDER_STATUS ='ACTIVE' and TRADE_ACCOUNT = '"+str(accountId)+"' and STRATEGY_ID = '"+str(strategyId)+"' and TRIGGER_ID='"+str(triggerId)+"'"
#     else:
#         selectStatment = "SELECT TRIGGER_ID FROM TRADE_TRANSACTIONS WHERE ORDER_STATUS ='ACTIVE' and TRADE_ACCOUNT = '"+str(accountId)+"' and STRATEGY_ID = '"+str(strategyId)+"'"
    
#     mySQLCursor.execute(selectStatment)
#     results = mySQLCursor.fetchall()
#     # gets the number of rows affected by the command executed
#     rowCount = mySQLCursor.rowcount
#     deletedCount = 0

#     if rowCount != 0:        
#         for row in results:            
#             try:
#                 triggerId = row[0]
#                 kmo.deleteGTTOrder(kite, triggerId)
#                 deletedCount += 1
#                 updateQuery = ("UPDATE TRADE_TRANSACTIONS SET ORDER_STATUS='DELETED', ORDER_REMARKS='Deleted from website' WHERE TRIGGER_ID='"+str(triggerId)+"' AND ORDER_STATUS IN ('ACTIVE') AND TRADE_ACCOUNT = '"+str(accountId)+"' and STRATEGY_ID = '"+str(strategyId)+"'")
#                 mySQLCursor.execute(updateQuery)
#                 cnx.commit()
#                 logging.info("Deleted the trigger ID: "+ str(triggerId) +" of " + str(accountId) + " on strategy " + str(strategyId))
#             except Exception as e:
#                 deletedCount -= 1        
#                 logging.info("Unable to delete trigger ID: "+ str(triggerId) +" of " + str(accountId) + " on strategy " + str(strategyId) + ". The error is " + str(e))
#     else:
#         logging.info("No GTT Orders found in database for " + str(accountId) + " on strategy " + str(strategyId))     
    
#     return { "no_of_gtt_orders" : rowCount , "no_of_deleted_orders" : deletedCount }

def insert_trade_signals(cnx, mySQLCursor, tradeSignalsDict):
    try:
        insertVal = []
        triggerPriceLevel = "BELOW"
        curDateTime = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
        insertQuery = "INSERT INTO TRADE_SIGNALS(SIGNAL_ID, STRATEGY_ID, STOCK_NAME, TRADE_SYMBOL, INSTRUMENT_TOKEN, TRIGGER_DATE, \
	            CURRENT_MKT_PRICE, BUY_OR_SELL, TGT_PROFIT_PCT, TARGET_PRICE, TGT_STOP_LOSS_PCT, STOP_LOSS_PRICE, TGT_HORIZON, \
                SIGNAL_STATUS, TRIGGER_PRICE, SCORE, UPDATED_ON, TRIGGER_PRICE_LEVEL, ENTRY_PRICE, ENTRY_DATE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        
        if(float(tradeSignalsDict['triggerPrice']) > float(tradeSignalsDict['lastPrice'])):
            triggerPriceLevel = "ABOVE"

        insertVal.insert(0, str(get_unique_id(cnx, mySQLCursor, tradeSignalsDict['strategyId'], 'SIGNAL_SERVICE')))
        insertVal.insert(1, str(tradeSignalsDict['strategyId']))
        insertVal.insert(2, str(tradeSignalsDict['stockName']))
        insertVal.insert(3, str(tradeSignalsDict['tradeSymbol']))
        insertVal.insert(4, str(tradeSignalsDict['instrumentToken']))
        insertVal.insert(5, str(curDateTime))
        insertVal.insert(6, str(tradeSignalsDict['lastPrice']))
        insertVal.insert(7, str(tradeSignalsDict['transactionType']))
        insertVal.insert(8, str(tradeSignalsDict['targetPercent']))
        insertVal.insert(9, str(tradeSignalsDict['targetPrice']))
        insertVal.insert(10, str(tradeSignalsDict['stopLossPercent']))
        insertVal.insert(11, str(tradeSignalsDict['stopLossPrice']))
        insertVal.insert(12, str(tradeSignalsDict['horizon']))
        
        if (str(tradeSignalsDict['productOrderType']) == 'MKT'):
            insertVal.insert(13, 'OPEN')
            insertVal.insert(14, str(tradeSignalsDict['lastPrice']))  
        else:
            insertVal.insert(13, 'PENDING')
            insertVal.insert(14, str(tradeSignalsDict['triggerPrice']))  

        insertVal.insert(15, str(tradeSignalsDict['buySellScore']))
        insertVal.insert(16, str(curDateTime))      
        insertVal.insert(17, str(triggerPriceLevel))     
        # Entry Price and Entry Date   
        insertVal.insert(18, str(tradeSignalsDict['lastPrice']))
        insertVal.insert(19, str(curDateTime))        
        
        mySQLCursor.execute(insertQuery, insertVal)
        cnx.commit()
    except Exception as e:
        logging.info("DB FAILURE: UNABLE TO INSERT: " + tradeSignalsDict['stockName'])
        logging.info(str(e))   

  

""" Create a unique ID for each signal which will be used to identify each trade signals with the combination 
    of strategy and unique Signal ID. """

def get_unique_id(cnx, mySQLCursor, strategyId, lableName):

    selectStatment = "SELECT SEQUENCE_NO FROM UNIQUE_ID_SERVICE WHERE UNIQUE_LABLE_NAME = '"+lableName+"'"
    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()   
    seqNumber = 0     
    
    for row in results:
        seqNumber = row[0]        
        updateQuery = "UPDATE UNIQUE_ID_SERVICE SET SEQUENCE_NO = "+ str(int(seqNumber) + 1) +" WHERE UNIQUE_LABLE_NAME = '"+lableName+"'"
        mySQLCursor.execute(updateQuery) 
        cnx.commit()   

    return strategyId + "_" + str(seqNumber)

def get_date_difference(endDate):
    
    fromDate = datetime.datetime.strptime(str(util.get_date_time_formatted('%Y-%m-%d')), '%Y-%m-%d')
    endDate = datetime.datetime.strptime(str(endDate), "%Y-%m-%d")
    # difference = endDate - fromDate
    return np.busday_count(endDate.date(), fromDate.date()) # use numpy method to get date difference count


def get_fno_expiry_month():
    if (int((datetime.datetime.today().strftime('%d'))) > 21):
        return (((datetime.date.today() + relativedelta.relativedelta(months=1))).strftime("%y%b")).capitalize()
    else:
        return ((datetime.datetime.now()).strftime("%y%b")).capitalize()


""" This function is used for adding current running status to MySQL table PROGRAM_RUNNING_STATUS"""
def update_program_running_status(cnx, mySQLCursor, programName, programStatus):
    try:

        selectStatment = "SELECT * FROM PROGRAM_RUNNING_STATUS WHERE PROGRAM_NAME='"+programName+"'"
        
        mySQLCursor.execute(selectStatment)
        rowCount = mySQLCursor.rowcount                
        updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")

        if rowCount != 0:
            updateQuery = ""
            if (programStatus == 'ACTIVE'):
                updateQuery ="UPDATE PROGRAM_RUNNING_STATUS SET RUNNING_STATUS = 'ACTIVE', UPDATED_ON='" + str(updatedOn) + "',LAST_ACTIVE_ON='" + str(updatedOn) + "'  WHERE PROGRAM_NAME = '"+ str(programName) +"'"
            else: 
                updateQuery ="UPDATE PROGRAM_RUNNING_STATUS SET RUNNING_STATUS = 'INACTIVE', UPDATED_ON='" + str(updatedOn) + "' WHERE PROGRAM_NAME = '"+ str(programName) +"'"
            mySQLCursor.execute(updateQuery)
            cnx.commit()  
        else:
            insertVal = []
            updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
            insertQuery = "REPLACE INTO PROGRAM_RUNNING_STATUS (PROGRAM_NAME, RUNNING_STATUS, UPDATED_ON) VALUES (%s,%s,%s)"
            insertVal.insert(0, str(programName))
            insertVal.insert(1, str(programStatus))
            insertVal.insert(2, str(updatedOn))
            mySQLCursor.execute(insertQuery, insertVal)
            cnx.commit()        
    except Exception as e:        
        logging.info("Unable to add activity to table (PROGRAM_RUNNING_STATUS)" + str(e))



def check_order_quantity(mySQLCursor, instrumentToken = None, strategyId = None, accountId = None, transactionType = None):

    if (transactionType == 'BUY'):
        selectStatment = "SELECT QUANTITY, AUTO_ID, PROFIT_PERCENT FROM TRADE_TRANSACTIONS WHERE BASE_INSTRUMENT_TOKEN='" + \
            str(instrumentToken) + "' AND TRADE_ACCOUNT= '"+ str(accountId) + "' AND STRATEGY_ID= '"+ str(strategyId) + "' AND \
                ((TRADE_STATUS ='PENDING' OR TRADE_STATUS ='OPEN' OR TRADE_STATUS ='P-OPEN') OR SELL_ORDER_STATUS='PENDING')"
    else:
        selectStatment = "SELECT QUANTITY, AUTO_ID, PROFIT_PERCENT FROM TRADE_TRANSACTIONS WHERE BASE_INSTRUMENT_TOKEN='" + \
            str(instrumentToken) + "' AND TRADE_ACCOUNT= '"+ str(accountId) + "' AND STRATEGY_ID= '"+ str(strategyId) + "' AND \
                (TRADE_STATUS ='OPEN' AND (SELL_ORDER_STATUS IS NULL OR SELL_ORDER_STATUS= '' OR SELL_ORDER_STATUS ='ABANDONED'))"

    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    # gets the number of rows affected by the command executed
    rowCount = mySQLCursor.rowcount
    quantity = 0
    autoId = 0
    profitPercent = 0
    if rowCount != 0:                    
        for row in results:
            quantity = int(row[0])
            autoId = row[1]
            profitPercent = float(row[2])

    return quantity, autoId, profitPercent


def get_ti_df_in_trade_dict(lastRec, tradeDataDict):
    tradeDataDict['ROC'] = pd.to_numeric(lastRec['ROC1'].values.ravel())[0]
    tradeDataDict['RSI_2'] = pd.to_numeric(lastRec['RSI_2'].values.ravel())[0]
    tradeDataDict['ROC_1'] = pd.to_numeric(lastRec['ROC1'].values.ravel())[0]
    tradeDataDict['ROC1_Pre'] = pd.to_numeric(lastRec['ROC1_Pre'].values.ravel())[0]
    tradeDataDict['SMA200'] = pd.to_numeric(lastRec['SMA200'].values.ravel())[0]
    tradeDataDict['CLOSE-PRICE'] = pd.to_numeric(lastRec['close'].values.ravel())[0]

    # For Siva
    tradeDataDict['EMA3-CLOSE'] = pd.to_numeric(lastRec['EMA3-CLOSE'].values.ravel())[0]
    tradeDataDict['EMA15-CLOSE'] = pd.to_numeric(lastRec['EMA15-CLOSE'].values.ravel())[0]
    tradeDataDict['EMA3-PREV-CLOSE'] = pd.to_numeric(lastRec['EMA3-PREV-CLOSE'].values.ravel())[0]
    tradeDataDict['EMA15-PREV-CLOSE'] = pd.to_numeric(lastRec['EMA15-PREV-CLOSE'].values.ravel())[0]

    return tradeDataDict

def techinical_indicator_in_dict(brokerApi, tradeDataDict, interval):
    response = {}
    try:
        if (interval != None): 
            toDate = prostocksApi.get_lookup_date(0)
            fromDate = prostocksApi.get_from_date_based_interval(tradeDataDict['broker'], interval)    
            # histRecords = baf.get_historical_data(kite, tradeDataDict['instrumentToken'], fromDate, toDate, interval)    
            histRecords = prostocksApi.get_historical_data(brokerApi, tradeDataDict['tradeSymbol'], tradeDataDict['baseExchangeToken'], tradeDataDict['exchange'], fromDate, toDate, interval)
            
            histRecords = list(reversed(histRecords))

            df = pd.DataFrame.from_dict(histRecords)
            # df = pd.DataFrame(histRecords)``
            op = pd.to_numeric(df['into'])
            hi = pd.to_numeric(df['inth'])
            lo = pd.to_numeric(df['intl'])
            cl = pd.to_numeric(df['intc'])
            vol = pd.to_numeric(df['intv'])
            
            df = tsc.get_ti_for_bullish_reversal(df, op, hi, lo, cl, vol)
            
            lastRec = df.tail(1)
            
            tradeDataDict['ROC'] = pd.to_numeric(lastRec['ROC1'].values.ravel())[0]
            tradeDataDict['RSI_2'] = pd.to_numeric(lastRec['RSI_2'].values.ravel())[0]
            tradeDataDict['ROC_1'] = pd.to_numeric(lastRec['ROC1'].values.ravel())[0]
            tradeDataDict['ROC1_Pre'] = pd.to_numeric(lastRec['ROC1_Pre'].values.ravel())[0]
            tradeDataDict['ROC1_Pre_2'] = pd.to_numeric(lastRec['ROC1_Pre_2'].values.ravel())[0]
            tradeDataDict['SMA200'] = pd.to_numeric(lastRec['SMA200'].values.ravel())[0]
            tradeDataDict['CLOSE-PRICE'] = pd.to_numeric(lastRec['intl'].values.ravel())[0]
            tradeDataDict['RSI_2_Low'] = pd.to_numeric(lastRec['RSI_2_Low'].values.ravel())[0]
            tradeDataDict['RSI_2_High'] = pd.to_numeric(lastRec['RSI_2_High'].values.ravel())[0]
            tradeDataDict['RSI_2_Pre'] = pd.to_numeric(lastRec['RSI_2_Pre'].values.ravel())[0]
            tradeDataDict['RSI_2_Pre2'] = pd.to_numeric(lastRec['RSI_2_Pre2'].values.ravel())[0]



            # TTM Squeez
            tradeDataDict['TTMSLength'] = pd.to_numeric(lastRec['TTMSLength'].values.ravel())[0]
            tradeDataDict['EMA5'] = pd.to_numeric(lastRec['EMA5'].values.ravel())[0]
            tradeDataDict['EMA8'] = pd.to_numeric(lastRec['EMA8'].values.ravel())[0]    
            tradeDataDict['EMA21'] = pd.to_numeric(lastRec['EMA21'].values.ravel())[0]
            tradeDataDict['ATR'] = pd.to_numeric(lastRec['ATR'].values.ravel())[0]
            tradeDataDict['ATR_100_Pct'] = pd.to_numeric(lastRec['ATR_100_Pct'].values.ravel())[0]

            # ADX gapper
            tradeDataDict['ADX'] = pd.to_numeric(lastRec['ADX'].values.ravel())[0]
            tradeDataDict['MINUS_DI'] = pd.to_numeric(lastRec['MINUS_DI'].values.ravel())[0]
            tradeDataDict['PLUS_DI'] = pd.to_numeric(lastRec['PLUS_DI'].values.ravel())[0]
            tradeDataDict['OPEN'] = pd.to_numeric(lastRec['into'].values.ravel())[0]
            tradeDataDict['LOW_PRE'] = pd.to_numeric(lastRec['LOW_PRE'].values.ravel())[0]
            tradeDataDict['HIGH_PRE'] = pd.to_numeric(lastRec['HIGH_PRE'].values.ravel())[0]
            tradeDataDict['2D_LOW_PRE'] = pd.to_numeric(lastRec['2D_LOW_PRE'].values.ravel())[0]
            tradeDataDict['2D_HIGH_PRE'] = pd.to_numeric(lastRec['2D_HIGH_PRE'].values.ravel())[0]
            tradeDataDict['5D_LOW_PRE'] = pd.to_numeric(lastRec['5D_LOW_PRE'].values.ravel())[0]
            tradeDataDict['5D_HIGH_PRE'] = pd.to_numeric(lastRec['5D_HIGH_PRE'].values.ravel())[0]

            # For NR4 with Volatility filter
            tradeDataDict['NR4'] = pd.to_numeric(lastRec['NR4'].values.ravel())[0]
            tradeDataDict['INSIDE_DAY'] = pd.to_numeric(lastRec['INSIDE_DAY'].values.ravel())[0]
            tradeDataDict['STDDEV_RATIO_6of100'] = pd.to_numeric(lastRec['STDDEV_RATIO_6of100'].values.ravel())[0]

            # For Turtle Soup Strategy
            tradeDataDict['20D_HIGH_PRE'] = pd.to_numeric(lastRec['20D_HIGH_PRE'].values.ravel())[0]
            tradeDataDict['20D_LOW_PRE'] = pd.to_numeric(lastRec['20D_LOW_PRE'].values.ravel())[0]
            tradeDataDict['20D_HIGH_PRE_5D'] = pd.to_numeric(lastRec['20D_HIGH_PRE_5D'].values.ravel())[0]
            tradeDataDict['20D_LOW_PRE_5D'] = pd.to_numeric(lastRec['20D_LOW_PRE_5D'].values.ravel())[0]

            # 80/20 Strategy 
            tradeDataDict['LOW_PRE'] = pd.to_numeric(lastRec['LOW_PRE'].values.ravel())[0]
            tradeDataDict['LOW'] = pd.to_numeric(lastRec['intl'].values.ravel())[0]
            tradeDataDict['HIGH_PRE'] = pd.to_numeric(lastRec['HIGH_PRE'].values.ravel())[0]
            tradeDataDict['HIGH'] = pd.to_numeric(lastRec['inth'].values.ravel())[0]
            tradeDataDict['OPEN_RANGE_PRE'] = pd.to_numeric(lastRec['OPEN_RANGE_PRE'].values.ravel())[0]
            tradeDataDict['CLOSE_RANGE_PRE'] = pd.to_numeric(lastRec['CLOSE_RANGE_PRE'].values.ravel())[0]


            # For Siva
            tradeDataDict['EMA3-CLOSE'] = pd.to_numeric(lastRec['EMA3-CLOSE'].values.ravel())[0]
            tradeDataDict['EMA15-CLOSE'] = pd.to_numeric(lastRec['EMA15-CLOSE'].values.ravel())[0]
            tradeDataDict['EMA3-PREV-CLOSE'] = pd.to_numeric(lastRec['EMA3-PREV-CLOSE'].values.ravel())[0]
            tradeDataDict['EMA15-PREV-CLOSE'] = pd.to_numeric(lastRec['EMA15-PREV-CLOSE'].values.ravel())[0]
        
            response['status'] = 'success'
            response['remarks'] = 'Technical indicators are added to dictionary'
        else:
            response['status'] = 'success'
            response['remarks'] = 'No interval found, ignoring technical indicator evaluation'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = f"Unable to get the technical indicators: {str(e)}"

    return tradeDataDict, response 

def check_existing_exit_positions(mySQLCursor, tradeDataDict):

    orderExistFlag = False 

    selectStatment = f"SELECT INSTRUMENT_TOKEN FROM TRADE_TRANSACTIONS WHERE AUTO_ID={tradeDataDict['autoId']} AND (SELL_ORDER_STATUS ='PENDING' OR SELL_ORDER_STATUS ='COMPLETED')"
    mySQLCursor.execute(selectStatment)

    # gets the number of rows affected by the command executed
    rowCount = mySQLCursor.rowcount
    if rowCount != 0:        
        orderExistFlag = True   

    return orderExistFlag


def check_existing_entry_positions(mySQLCursor, tradeDataDict, adjustmentOrder = 'N'):

    orderExistFlag = False 
    selectStatment = f"SELECT INSTRUMENT_TOKEN FROM TRADE_TRANSACTIONS WHERE STRATEGY_ID='{tradeDataDict['strategyId']}' AND BASE_INSTRUMENT_TOKEN='{tradeDataDict['instrumentToken']}' \
        AND TRADE_ACCOUNT= '{tradeDataDict['accountId']}'  AND (TRADE_STATUS ='PENDING' OR TRADE_STATUS ='OPEN' OR TRADE_STATUS ='P-OPEN')"
    
    mySQLCursor.execute(selectStatment)
    # gets the number of rows affected by the command executed
    rowCount = mySQLCursor.rowcount
    if rowCount == 1:                  
        if (str(tradeDataDict['strategyType']).__contains__('HEDGE') or str(tradeDataDict['strategyType']).__contains__('STRANGLE')):
            orderExistFlag = False
        else:
            orderExistFlag = True
    elif (rowCount == 2 and adjustmentOrder == 'Y'):
            orderExistFlag = False
    elif rowCount > 1:
        orderExistFlag = True
        
    return orderExistFlag


def get_best_trigger_price(brokerApi, instToken, exchange, transactionType):

    quoteData = prostocksApi.get_quotes(brokerApi, instToken, exchange=exchange)
    triggerPrice = None

    if(transactionType == "BUY" or transactionType == "B"):                     
        if "bp1" in quoteData:
            triggerPrice = float(quoteData['bp1']) + 0.05
    else:                
        if "sp1" in quoteData:
            triggerPrice = float(quoteData['sp1']) - 0.05
    
    if (Config.TESTING_FLAG or abs(triggerPrice) == 0.05):
        triggerPrice = float(quoteData['lp'])

    return triggerPrice

def place_fno_orders(cnx, mySQLCursor, brokerApi, tradeDataDict):

    existingOrderFound, proStocksOrderId, proStocksTransType, proStocksQty = baf.get_prostocks_orders(brokerApi, tradeDataDict['futTradeSymbol'], inTransType=tradeDataDict['futTransactionType'])
    orderSuccessFlag = False
    if (not(existingOrderFound)):

        existingPositionFound = False
        if (tradeDataDict['duplicatePosAllowedFlag'] == 'N'):            
            existingPosFromBroker, existingOrderQuantity  = baf.get_prostocks_positions(brokerApi, tradeDataDict['futTradeSymbol'])                        
        
            existingPosFromDB = util.check_existing_entry_positions(mySQLCursor, tradeDataDict, adjustmentOrder=tradeDataDict['adjustmentOrder'])

            if (existingPosFromDB or existingPosFromBroker):
                existingPositionFound = True

        if (not(existingPositionFound)):
            # quoteData = baf.get_quote(brokerApi, tradeDataDict['futInstToken'])
            
            tradeDataDict['futTriggerPrice'] = util.get_best_trigger_price(brokerApi, tradeDataDict['futExchangeToken'], 'NFO', tradeDataDict['futTransactionType'])                

            if (tradeDataDict['futTriggerPrice'] != None):
                tradeDataDict['futBuyValue'] = tradeDataDict['quantity'] * float(tradeDataDict['futTriggerPrice'])  
                tradeDataDict['futLastPrice'] = tradeDataDict['futTriggerPrice']            

                utilizedCaptial = util.get_utilized_capital(mySQLCursor, tradeDataDict['accountId'], strategyId=tradeDataDict['strategyId'])
                
                capitalAllocation = float(tradeDataDict['capitalAllocation'])
    
                if ((abs(utilizedCaptial) + abs(float(tradeDataDict['futBuyValue']))) <= abs(capitalAllocation)):
                    
                    if (("dummyEntryFlag" in tradeDataDict) and (tradeDataDict['dummyEntryFlag'] == 'Y')):
                        orderId = 77777
                        orderRemarks = "STAGED ENTRY ORDER"
                    elif (("adjustmentOrder" in tradeDataDict) and (tradeDataDict['adjustmentOrder'] == 'Y')):
                        orderId = 88888
                        orderRemarks = "STAGED ADJUSTMENT ORDER"
                    elif (Config.TESTING_FLAG):
                        orderId = 99999
                        orderRemarks = "TESTING ORDERED"
                    else:                       
                        orderId, orderRemarks  = baf.place_future_buy_order(brokerApi, tradeDataDict)

                    
                
                    # insert the order details to the MySQL trade transactions table for the leg 1
                    if (int(orderId) > 0):
                        logging.info(f"Call: {str(tradeDataDict['futTransactionType'])} with order id {str(orderId)}")    
                        tradeDataDict['orderId']=orderId
                        tradeDataDict['orderRemarks']=orderRemarks
                        orderSuccessFlag = util.insert_future_order_details(cnx, mySQLCursor, tradeDataDict)                        
                        
                        alertMsg = f" Strategy: {tradeDataDict['strategyId']} \nTrade:  Entry {tradeDataDict['futTransactionType']} \nInstrument Name: {tradeDataDict['futTradeSymbol']} \nEntry Price: {str('%.2f' % tradeDataDict['futLastPrice'])} \nTraded Value:  {str('%.2f' % tradeDataDict['futBuyValue'])} \nTrade Initialized At: {tradeDataDict['signalDate']}"                        
                        # util.add_alerts(cnx, mySQLCursor, 'ALERT', alertMsg, tradeDataDict['telegramUserNames'], tradeDataDict['programName'], telgUpdateFlag='Y', tradeDataDict=tradeDataDict, updateForUser='Y', dbUpdateFlag='Y')
                        util.add_logs(cnx, mySQLCursor, 'NOTIFY', alertMsg, tradeDataDict)
                        
                else:
                    alertMsg = f"Capital allocation is fully used"
                    try:
                        updateQuery = (f"UPDATE USR_STRATEGY_SUBSCRIPTIONS SET ALLOCATED_CASH_EXCEEDED_FLAG='Y' WHERE STRATEGY_ID ='{tradeDataDict['strategyId']}' AND TRADE_ACCOUNT='{tradeDataDict['accountId']}'")
                        mySQLCursor.execute(updateQuery)
                        cnx.commit()
                    except Exception as e:
                        util.add_logs(cnx, mySQLCursor, 'ERROR', f"Unable to update the ALLOCATED_CASH_EXCEEDED_FLAG flag: {str(e)}", tradeDataDict)    

                    util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, tradeDataDict)
        else:
            alertMsg = f"Existing position found in existingPosFromDB ({existingPosFromDB})  or existingPosFromBroker ({existingPosFromBroker}) for ({tradeDataDict['futTradeSymbol']})"
            util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, tradeDataDict)
    else:
        alertMsg = f"Existing order found ({tradeDataDict['futTradeSymbol']})"
        util.add_logs(cnx, mySQLCursor, 'UPDATE', alertMsg, tradeDataDict)
        
    return orderSuccessFlag


def get_monthly_options_instruments(mySQLCursor, tradeDataDict, tradeSymbol, optionsType, stockClosePrice, adjustOrderFlag, minDaysToExpiry = 1, strikeAdjust=0):

    currentDay = int(datetime.datetime.today().strftime('%d'))
    lastExpiryDay = util.get_current_fno_expiry_day()
    
    expiryMonth = ""
    if (currentDay > (lastExpiryDay - minDaysToExpiry)):
        expiryMonth = (((datetime.date.today() + relativedelta.relativedelta(months=1))).strftime("%y%b")).upper()
    else:
        expiryMonth= ((datetime.datetime.now()).strftime("%y%b")).upper()
    
    if (adjustOrderFlag):
        instSearchString = str(tradeSymbol) + str(tradeDataDict['expiryDate'].strftime("%y%b")).upper()
        # instSearchString = datetime.datetime.strptime(str(tradeDataDict['expiryDate']),'%Y-%m-%d').strftime("%y%b").capitalize()
    else:
        instSearchString = str(tradeSymbol) + str(expiryMonth)

    if (optionsType == 'PUT'):
        instrumentType = 'PE'        
        stockClosePrice = get_round_of_value(float(stockClosePrice - strikeAdjust))
        selectStatment = f"SELECT tradingsymbol, instrument_token, lot_size, expiry, strike, exchange_token FROM INSTRUMENTS WHERE name = '{str(tradeSymbol)}' AND \
            tradingsymbol LIKE '{instSearchString}%' AND exchange='NFO' AND instrument_type='{instrumentType}' and strike >= {str(stockClosePrice)} order by strike limit 1"
    else:
        instrumentType = 'CE'
        stockClosePrice = get_round_of_value(float(stockClosePrice + strikeAdjust))
        selectStatment = f"SELECT tradingsymbol, instrument_token, lot_size, expiry, strike, exchange_token FROM INSTRUMENTS WHERE name = '{str(tradeSymbol)}' AND \
            tradingsymbol LIKE '{instSearchString}%' AND exchange='NFO' AND instrument_type='{instrumentType}' and strike <= {str(stockClosePrice)} order by strike DESC limit 1"


    mySQLCursor.execute(selectStatment)
    results = mySQLCursor.fetchall()
    futInstName = ''
    futInstToken = ''
    lotSize = 0
    expDate = ''
    strikePrice = 0
    rawExpiry = ''
    exchangeToken = ''

    for row in results:
        futInstName = row[0]
        futInstToken = row[1]
        lotSize = row[2]
        expDate = str(row[3].strftime("%d%b%y")).upper()
        strikePrice = float(row[4])
        rawExpiry = str(row[3].strftime("%Y-%m-%d")).upper()
        exchangeToken = str(row[5])

    return futInstName, futInstToken, lotSize, expDate, strikePrice, rawExpiry, exchangeToken

# Works for only Bank nifty
def get_strike_by_delta(mySQLCursor, brokerApi, closePrice, tradeSymbol, instrumentType, requiredDelta, minDaysToExpiry = 1, expiry=None, adjustOrderFlag = False, contractType = 'MONTHLY'):
    try:
        if (contractType == "MONTHLY"):     
            currentDay = int(datetime.datetime.today().strftime('%d'))
            lastExpiryDay = util.get_current_fno_expiry_day()               
            
            if (adjustOrderFlag):
                instSearchString = str(tradeSymbol) + str(expiry.strftime("%y%b")).upper()
            else:
                # if (expiry == None):
                #     expiry = (datetime.datetime.now() + datetime.timedelta(days=minDaysToExpiry)).strftime("%Y-%m-%d")

                if (currentDay > (lastExpiryDay - minDaysToExpiry)):
                    expiryMonth = (((datetime.date.today() + relativedelta.relativedelta(months=1))).strftime("%y%b")).upper()
                else:
                    expiryMonth= ((datetime.datetime.now()).strftime("%y%b")).upper()

                instSearchString = str(tradeSymbol) + str(expiryMonth)


            if ( instrumentType == 'CALL' ):
                selectStatment = f"SELECT instrument_token, strike, tradingsymbol, lot_size, expiry \
                    FROM INSTRUMENTS WHERE tradingsymbol LIKE '{instSearchString}%' and name='{tradeSymbol}' \
                        AND exchange='NFO' AND instrument_type='CE' AND strike <= '{closePrice + (closePrice * 0.03)}' ORDER BY strike DESC LIMIT 15"
            else:
                selectStatment = f"SELECT instrument_token, strike, tradingsymbol, lot_size, expiry, exchange_token \
                    FROM INSTRUMENTS WHERE tradingsymbol LIKE '{instSearchString}%' and name='{tradeSymbol}' \
                        AND exchange='NFO' AND instrument_type='PE' AND strike >= '{closePrice - (closePrice * 0.03)}' ORDER BY strike ASC LIMIT 15"

        else:
            if (adjustOrderFlag):
                expiry = expiry.strftime("%Y-%m-%d")
            else:
                expiry = (datetime.datetime.now() + datetime.timedelta(days=minDaysToExpiry)).strftime("%Y-%m-%d")

            if (instrumentType == 'CALL'):
                selectStatment = f"SELECT instrument_token, strike, tradingsymbol, lot_size, expiry, exchange_token FROM INSTRUMENTS \
                    WHERE expiry >= '{expiry}' and name = '{str(tradeSymbol)}' AND exchange='NFO' AND instrument_type='CE' \
                    and strike <= {closePrice - (closePrice * 0.025)} order by expiry, strike desc limit 20"
            else:            
                selectStatment = f"SELECT instrument_token, strike, tradingsymbol, lot_size, expiry, exchange_token FROM INSTRUMENTS WHERE \
                    expiry >= '{expiry}' and name = '{str(tradeSymbol)}' AND exchange='NFO' AND instrument_type='PE' \
                        and strike >= {closePrice + (closePrice * 0.025)} order by expiry, strike limit 20"


        mySQLCursor.execute(selectStatment)
        results = mySQLCursor.fetchall()
        deltaCallList = []
        thetaCallList = []
        gammaList = []
        vegaList = []
        strikePriceList = []

        impliedVolList = []
        fnoTradingSymbolList = []
        fnoInstrumentTokenList = []
        lotSize = 1

        expiry = None
        for row in results:
        
            oInstrumentToken = str(row[0])
            oStrikePrice = row[1]
            fnoTradingSymbol = row[2]
            lotSize = row[3]
            expiry = row[4]
            oInstrumentToken = str(row[5])
            
            strikePriceList.append(oStrikePrice)
            fnoTradingSymbolList.append(fnoTradingSymbol)
            fnoInstrumentTokenList.append(oInstrumentToken)

            if (instrumentType == 'PUT'):
                bsmDataDict = bsm_options_pricing_new(brokerApi, oInstrumentToken, oStrikePrice, expiry, closePrice, fnoTradingSymbol, 'PUT')
                
            else:                    
                bsmDataDict = bsm_options_pricing_new(brokerApi, oInstrumentToken, oStrikePrice, expiry, closePrice, fnoTradingSymbol, 'CALL')
            
            
            impliedVolList.append(bsmDataDict['IMPLIED_VOLATILITY'])
            deltaCallList.append(bsmDataDict['delta'])
            thetaCallList.append(bsmDataDict['theta'])
            gammaList.append(bsmDataDict['gamma'])
            vegaList.append(bsmDataDict['vega'])


        if (len(strikePriceList) > 0):
            # Get the closest delta value to 0.40 (40)
            if (instrumentType == 'PUT'):
                closestStrikeIndex = min(range(len(deltaCallList)), key=lambda i: abs(deltaCallList[i] + abs(requiredDelta)))
            else:
                closestStrikeIndex = min(range(len(deltaCallList)), key=lambda i: abs(deltaCallList[i] - abs(requiredDelta)))
            
            logging.info(f"deltaCallList: {deltaCallList}")
            selectedDelta = deltaCallList[closestStrikeIndex]
            selectedStrikePrice = strikePriceList[closestStrikeIndex]
            selectedImpliedVol = impliedVolList[closestStrikeIndex]
            selectedTradingSymbol =  fnoTradingSymbolList[closestStrikeIndex]
            selectedInsturmentToken = fnoInstrumentTokenList[closestStrikeIndex]

            # Get the day to expiry
            fDate = datetime.datetime.strptime(str(util.get_date_time_formatted("%Y-%m-%d")), "%Y-%m-%d")
            lDate = datetime.datetime.strptime(str(expiry), "%Y-%m-%d")

            daysToExpiry = lDate - fDate
            
            if (daysToExpiry.days == 0):
                daysToExpiry = 1
            else:
                daysToExpiry = int(daysToExpiry.days)

            response = {}   
            response['status'] = 'success'
            response['remarks'] = 'success'              
            response['selectedInstToken'] = selectedInsturmentToken            
            response['selectedTradeSymbol'] = selectedTradingSymbol            
            response['selectedImpliedVol'] = selectedImpliedVol
            response['selectedStrikePrice'] = selectedStrikePrice
            response['selectedDelta'] = selectedDelta
            response['expiry'] = str(expiry.strftime("%d%b%y")).upper()
            response['rawExpiry'] = expiry
            
            response['lotSize'] = lotSize

            logging.info(f"Selected DELTA response : {response}")
  
        else:  
            response = {}   
            response['status'] = 'failed'
            response['remarks'] = 'Strike Price List doesn\'t have any value for ' + tradeSymbol
            

    except Exception as e:            
        response = {}   
        response['status'] = 'failed'
        response['remarks'] = f"Errored while getting the BSM CALL Options Pricing for {tradeSymbol} : {str(e)}"
        return response
    
    return response

def bsm_options_pricing_new(brokerApi, instToken, strikePrice, expiry, closePrice, tradeSymbol, instrumentType):
    bsmDataDict = {}
    try:
        interestRate = 6
        fDate = datetime.datetime.strptime(str(util.get_date_time_formatted("%Y-%m-%d")), "%Y-%m-%d")
        lDate = datetime.datetime.strptime(str(expiry), "%Y-%m-%d")
        daysToExpiry1 = 1 
        daysToExpiry = lDate - fDate

        if (daysToExpiry.days == 0):
            daysToExpiry1 = 1
        else:
            daysToExpiry1 = int(daysToExpiry.days)
    
        # quoteData = prostocksApi.get_quotes(brokerApi, instToken)
        quoteData = prostocksApi.get_quotes(brokerApi, instToken, exchange='NFO')

        fnoLastPrice = float(quoteData['lp'])

        if (float(fnoLastPrice) > 0):
            # bsmDataDict['OI_VALUE'] = float(quoteData[instToken]['oi']) * float(closePrice)
            bsmDataDict['VOLUME_VALUE']= float(quoteData['v']) * float(closePrice)
            if (instrumentType == 'PUT'):
                iv = mibian.BS([closePrice, strikePrice, interestRate, daysToExpiry1], putPrice=fnoLastPrice)                
                c = mibian.BS([closePrice, strikePrice, interestRate, daysToExpiry1], volatility=float("{:.2f}".format(iv.impliedVolatility)))
                bsmDataDict['delta'] = float("{:.2f}".format(c.putDelta))
                bsmDataDict['theta'] = float("{:.2f}".format(c.putTheta))
            else:
                iv = mibian.BS([closePrice, strikePrice, interestRate, daysToExpiry1], callPrice=fnoLastPrice)
                c = mibian.BS([closePrice, strikePrice, interestRate, daysToExpiry1], volatility=float("{:.2f}".format(iv.impliedVolatility)))
                bsmDataDict['delta'] = float("{:.2f}".format(c.callDelta))
                bsmDataDict['theta'] = float("{:.2f}".format(c.callTheta))
            
            bsmDataDict['vega'] = float("{:.2f}".format(c.vega))
            bsmDataDict['gamma'] = float("{:.5f}".format(c.gamma))

            bsmDataDict['IMPLIED_VOLATILITY']= float("{:.2f}".format(iv.impliedVolatility))
            bsmDataDict['daysToExpiry'] = daysToExpiry1
            bsmDataDict['status'] = 'success'
            bsmDataDict['remarks'] = 'success'               
        else:            
            bsmDataDict['status'] = 'failed'
            bsmDataDict['remarks'] = 'FNO Price is zero for ' + tradeSymbol

    except Exception as e:            
        bsmDataDict['status'] = 'failed'
        bsmDataDict['remarks'] = "Errored while getting the BSM Options Pricing for " + tradeSymbol + ": "+ str(e)
    
    return bsmDataDict

def get_all_pattern_signals(cnx, mySQLCursor, brokerApi, tradeDataDict, interval):       
    try:
        if (tradeDataDict['revPatternCheckFlag'] == 'Y'):        
            toDate = prostocksApi.get_lookup_date(0)
            fromDate = prostocksApi.get_from_date_based_interval(tradeDataDict['broker'], interval)

            # histRecords = baf.get_historical_data(kite, tradeDataDict['instrumentToken'], fromDate, toDate, interval)
            
            histRecords = prostocksApi.get_historical_data(brokerApi, tradeDataDict['tradeSymbol'], tradeDataDict['baseExchangeToken'], tradeDataDict['exchange'], fromDate, toDate, interval)
            df = pd.DataFrame.from_dict(histRecords)

            # df = pd.DataFrame(histRecords)

            extrema, prices, smooth_extrema, smooth_prices = cp.find_extrema(
                df, bw="cv_ls"
            )
            patterns, patternDict  = cp.find_patterns(extrema, prices, max_bars=0)
            tradeDataDict.update(patternDict)  

    except Exception as e:
        util.add_logs(cnx, mySQLCursor, 'ERROR', 'Unable to get the pattern signals', tradeDataDict)

    return tradeDataDict    


def get_broker_specfic_interval(inInterval, broker):
    switcher = {}
    if (broker == 'PROSTOCKS'):
        switcher = {
            'minute' : '1',
            '3minute': '3',
            '5minute': '5',
            '10minute': '10',
            '15minute': '15',
            '30minute': '30',
            '60minute': '60',
            '2hour': '120',
            'day': 'day'
        }
    return switcher.get(inInterval, '30')

def copy_user_data_to_dict(strategyId, user):
    tradeDataDict = {}
    tradeDataDict['programName']= os.path.splitext(os.path.basename(__file__))[0]
    tradeDataDict['signalDate'] = util.get_date_time_formatted('%d-%m-%Y %H:%M:%S')
    tradeDataDict['telegramAdminIds'] = Config.TELG_ADMIN_ID
    tradeDataDict['strategyId'] = strategyId
    tradeDataDict['exchange'] = 'NSE'
    tradeDataDict['orderStatus']= 'PENDING'    
    tradeDataDict['futOrderType'] = 'LMT'
    tradeDataDict['positionGroupId'] = 0
    tradeDataDict['rawExpiry'] = '2099-12-31'
    tradeDataDict['positionDirection'] = ''
    tradeDataDict['strikePrice'] = 0
    tradeDataDict['tradeSequence'] = ''
    tradeDataDict['patternDirection'] = ''
    tradeDataDict['strikeAdjust'] = 0
    tradeDataDict['adjustmentOrder'] = 'N'    
    tradeDataDict['accountId'] = user['TRADE_ACCOUNT']
    tradeDataDict['broker'] = user['BROKER']    
    tradeDataDict['tgtProfitPct'] = user['TGT_PROFIT_PCT']
    tradeDataDict['tgtStopLossPct'] = user['TGT_STOP_LOSS_PCT']
    tradeDataDict['trailingThresholdPct'] = user['TRAILING_THRESHOLD_PCT']  
    tradeDataDict['exitStrategyId'] = user['EXIT_STRATEGY_ID']                                   
    tradeDataDict['exitStrategy'] = user['EXIT_STRATEGY_ID']

    tradeDataDict['availableMargin'] = user['AVAILABLE_MARGIN']
    tradeDataDict['capitalAllocation'] = user['CAPITAL_ALLOCATION']
    tradeDataDict['uatFlag'] = True if user['UAT_FLAG'] == 'Y' else False                            
    tradeDataDict['entryBuyCondition'] = user['ENTRY_BUY_CONDITION']
    tradeDataDict['entrySellCondition'] = user['ENTRY_SELL_CONDITION']
    tradeDataDict['exitBuyCondition'] = user['EXIT_BUY_CONDITION']
    tradeDataDict['exitSellCondition'] = user['EXIT_SELL_CONDITION']                     
    
    tradeDataDict['strategyType'] = user['STRATEGY_TYPE']
    tradeDataDict['entryDirection'] = user['ENTRY_DIRECTION']                                
    tradeDataDict['contractType'] = user['CONTRACT_TYPE']
    tradeDataDict['productType'] = user['PRODUCT_TYPE']
    tradeDataDict['lotSizeMultiplier'] = int(user['LOT_SIZE_MULTIPLIER'])
    tradeDataDict['rollOverFlag'] = user['ROLL_OVER_FLAG']
    tradeDataDict['strikeSelectByPercent'] = float(user['STRIKE_SELECT_BY_PERCENT'])
    tradeDataDict['strikeSelectByATM'] = user['STRIKE_SELECT_BY_ATM']
    
    tradeDataDict['techIndicatorInterval'] = util.get_broker_specfic_interval(user['TECH_INDICATOR_INTERVAL'], user['BROKER'])
    tradeDataDict['exitInterval'] = util.get_broker_specfic_interval(user['EXIT_INTERVAL_TIME'], user['BROKER'])
    tradeDataDict['revPatternCheckFlag'] = user['REV_PATTERN_CHECK_FLAG']                                
    tradeDataDict['strikeType'] = user['STRIKE_TYPE']     
    tradeDataDict['optionsEligibilityConditions'] = user['OPTIONS_ELIGIBILITY_CONDITIONS']     
    tradeDataDict['duplicatePosAllowedFlag'] = user['DUPLICATE_POS_ALLOWED_FLAG']
    tradeDataDict['minDaysToExpiry'] = int(user['MIN_DAYS_TO_EXPIRY_FOR_NEW_TRADE'])
    tradeDataDict['hedgingRequiredFlag'] = user['HEDGING_REQUIRED_FLAG']
    tradeDataDict['hedgingStrikeSelectPercent'] = float(user['HEDGING_STRIKE_SELECT_PERCENT'])
    tradeDataDict['eodExitFlag'] = user['EOD_EXIT_FLAG']    


    if (user['TELEGRAM_USER_NAME'] != None):
        tradeDataDict['telegramUserNames'] = user['TELEGRAM_USER_NAME']
    else:
        tradeDataDict['telegramUserNames'] = Config.TELG_ADMIN_ID    
    return tradeDataDict    

