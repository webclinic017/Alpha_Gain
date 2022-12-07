import pandas as pd
import datetime
import time
import mysql.connector
import talib
import numpy as np
from utils import chart_patterns as cp



pd.options.mode.chained_assignment = None
# Add constant values as global for indicator weightage
trendWeight = 1
coWeight = 1
reversalWeight = 1
quantiyWeight = 1
cdlWeight = 1
# Add constant values as global for indicator score
trendScore = 10
coScore = 10
reversalScore = 10
quantiyScore = 10
cdlScore = 10
commentsHtml = ""

def getCandleStickPatterns(df, op, hi, lo, cl, vol):
    df['CDL2CROWS'] = talib.CDL2CROWS(
        op, hi, lo, cl)                            # Best Indicator
    df['CDL3BLACKCROWS'] = talib.CDL3BLACKCROWS(
        op, hi, lo, cl)                 # Best Indicator
    df['CDL3INSIDE'] = talib.CDL3INSIDE(
        op, hi, lo, cl)                          # Best Indicator
    df['CDL3LINESTRIKE'] = talib.CDL3LINESTRIKE(
        op, hi, lo, cl)                 # Best Indicator
    df['CDL3OUTSIDE'] = talib.CDL3OUTSIDE(
        op, hi, lo, cl)                        # Best Indicator
    df['CDL3STARSINSOUTH'] = talib.CDL3STARSINSOUTH(
        op, hi, lo, cl)                # Best Indicator
    df['CDL3WHITESOLDIERS'] = talib.CDL3WHITESOLDIERS(
        op, hi, lo, cl)            # Best Indicator
    df['CDLABANDONEDBABY'] = talib.CDLABANDONEDBABY(
        op, hi, lo, cl, penetration=0)   # Best Indicator
    df['CDLADVANCEBLOCK'] = talib.CDLADVANCEBLOCK(
        op, hi, lo, cl)                       # Best Indicator
    df['CDLBELTHOLD'] = talib.CDLBELTHOLD(
        op, hi, lo, cl)                            # Best Indicator
    df['CDLBREAKAWAY'] = talib.CDLBREAKAWAY(
        op, hi, lo, cl)                              # Best Indicator
    df['CDLCLOSINGMARUBOZU'] = talib.CDLCLOSINGMARUBOZU(op, hi, lo, cl)
    df['CDLCONCEALBABYSWALL'] = talib.CDLCONCEALBABYSWALL(
        op, hi, lo, cl)                # Best Indicator
    df['CDLCOUNTERATTACK'] = talib.CDLCOUNTERATTACK(op, hi, lo, cl)
    df['CDLDARKCLOUDCOVER'] = talib.CDLDARKCLOUDCOVER(
        op, hi, lo, cl, penetration=0)
    df['CDLDOJI'] = talib.CDLDOJI(op, hi, lo, cl)
    df['CDLDOJISTAR'] = talib.CDLDOJISTAR(
        op, hi, lo, cl)                                    # Best Indicator
    df['CDLDRAGONFLYDOJI'] = talib.CDLDRAGONFLYDOJI(op, hi, lo, cl)
    df['CDLENGULFING'] = talib.CDLENGULFING(
        op, hi, lo, cl)                                 # Best Indicator
    df['CDLEVENINGDOJISTAR'] = talib.CDLEVENINGDOJISTAR(
        op, hi, lo, cl, penetration=0)          # Best Indicator
    df['CDLEVENINGSTAR'] = talib.CDLEVENINGSTAR(
        op, hi, lo, cl, penetration=0)           # Best Indicator
    df['CDLGAPSIDESIDEWHITE'] = talib.CDLGAPSIDESIDEWHITE(
        op, hi, lo, cl)                    # Best Indicator
    df['CDLGRAVESTONEDOJI'] = talib.CDLGRAVESTONEDOJI(op, hi, lo, cl)
    # Best Indicator
    df['CDLHAMMER'] = talib.CDLHAMMER(op, hi, lo, cl)
    df['CDLHANGINGMAN'] = talib.CDLHANGINGMAN(op, hi, lo, cl)
    df['CDLHARAMI'] = talib.CDLHARAMI(op, hi, lo, cl)
    df['CDLHARAMICROSS'] = talib.CDLHARAMICROSS(op, hi, lo, cl)
    df['CDLHIGHWAVE'] = talib.CDLHIGHWAVE(op, hi, lo, cl)
    df['CDLHIKKAKE'] = talib.CDLHIKKAKE(op, hi, lo, cl)
    df['CDLHIKKAKEMOD'] = talib.CDLHIKKAKEMOD(op, hi, lo, cl)
    df['CDLHOMINGPIGEON'] = talib.CDLHOMINGPIGEON(op, hi, lo, cl)
    df['CDLIDENTICAL3CROWS'] = talib.CDLIDENTICAL3CROWS(
        op, hi, lo, cl)                  # Best Indicator
    # Best Indicator
    df['CDLINNECK'] = talib.CDLINNECK(op, hi, lo, cl)
    df['CDLINVERTEDHAMMER'] = talib.CDLINVERTEDHAMMER(op, hi, lo, cl)
    # Best Indicator
    df['CDLKICKING'] = talib.CDLKICKING(op, hi, lo, cl)
    df['CDLKICKINGBYLENGTH'] = talib.CDLKICKINGBYLENGTH(op, hi, lo, cl)
    df['CDLLADDERBOTTOM'] = talib.CDLLADDERBOTTOM(
        op, hi, lo, cl)                        # Best Indicator
    df['CDLLONGLEGGEDDOJI'] = talib.CDLLONGLEGGEDDOJI(op, hi, lo, cl)
    df['CDLLONGLINE'] = talib.CDLLONGLINE(op, hi, lo, cl)
    df['CDLMARUBOZU'] = talib.CDLMARUBOZU(op, hi, lo, cl)
    df['CDLMATCHINGLOW'] = talib.CDLMATCHINGLOW(
        op, hi, lo, cl)                      # Best Indicator
    df['CDLMATHOLD'] = talib.CDLMATHOLD(
        op, hi, lo, cl, penetration=0)                   # Best Indicator
    df['CDLMORNINGDOJISTAR'] = talib.CDLMORNINGDOJISTAR(
        op, hi, lo, cl, penetration=0)       # Best Indicator
    df['CDLMORNINGSTAR'] = talib.CDLMORNINGSTAR(
        op, hi, lo, cl, penetration=0)           # Best Indicator
    # Best Indicator
    df['CDLONNECK'] = talib.CDLONNECK(op, hi, lo, cl)
    df['CDLPIERCING'] = talib.CDLPIERCING(
        op, hi, lo, cl)                                # Best Indicator
    df['CDLRICKSHAWMAN'] = talib.CDLRICKSHAWMAN(op, hi, lo, cl)
    df['CDLRISEFALL3METHODS'] = talib.CDLRISEFALL3METHODS(
        op, hi, lo, cl)                # Best Indicator
    df['CDLSEPARATINGLINES'] = talib.CDLSEPARATINGLINES(
        op, hi, lo, cl)                  # Best Indicator
    df['CDLSHOOTINGSTAR'] = talib.CDLSHOOTINGSTAR(op, hi, lo, cl)
    df['CDLSHORTLINE'] = talib.CDLSHORTLINE(op, hi, lo, cl)
    df['CDLSPINNINGTOP'] = talib.CDLSPINNINGTOP(op, hi, lo, cl)
    df['CDLSTALLEDPATTERN'] = talib.CDLSTALLEDPATTERN(op, hi, lo, cl)
    df['CDLSTICKSANDWICH'] = talib.CDLSTICKSANDWICH(
        op, hi, lo, cl)                     # Best Indicator
    df['CDLTAKURI'] = talib.CDLTAKURI(op, hi, lo, cl)
    df['CDLTASUKIGAP'] = talib.CDLTASUKIGAP(
        op, hi, lo, cl)                          # Best Indicator
    df['CDLTHRUSTING'] = talib.CDLTHRUSTING(
        op, hi, lo, cl)                              # Best Indicator
    df['CDLTRISTAR'] = talib.CDLTRISTAR(
        op, hi, lo, cl)                                  # Best Indicator
    df['CDLUNIQUE3RIVER'] = talib.CDLUNIQUE3RIVER(op, hi, lo, cl)
    df['CDLUPSIDEGAP2CROWS'] = talib.CDLUPSIDEGAP2CROWS(op, hi, lo, cl)
    df['CDLXSIDEGAP3METHODS'] = talib.CDLXSIDEGAP3METHODS(op, hi, lo, cl)

    return df


def getTechnicalIndicators(df, op, hi, lo, cl, vol, index):
    df['AD'] = talib.AD(hi, lo, cl, vol)
    df['ADOSC'] = talib.ADOSC(hi, lo, cl, vol, fastperiod=5, slowperiod=14)

    df['AVG_ADOSC'] = talib.SMA(df['ADOSC'], timeperiod=14)

    df['ADX'] = talib.ADX(hi, lo, cl, timeperiod=14)
    df['ADXR'] = talib.ADXR(hi, lo, cl, timeperiod=14)
    df['AROON_DOWN'], df['AROON_UP'] = talib.AROON(hi, lo, timeperiod=14)
    df['AROONOSC'] = talib.AROONOSC(hi, lo, timeperiod=14)
    df['ATR'] = talib.ATR(hi, lo, cl, timeperiod=14)
    df['ATR_28D'] = talib.ATR(hi, lo, cl, timeperiod=28)
    df['DX'] = talib.DX(hi, lo, cl, timeperiod=14)
    df['MINUS_DI'] = talib.MINUS_DI(hi, lo, cl, timeperiod=14)
    df['MINUS_DM'] = talib.MINUS_DM(hi, lo, timeperiod=14)
    df['PLUS_DI'] = talib.PLUS_DI(hi, lo, cl, timeperiod=14)
    
    df['PLUS_DM'] = talib.PLUS_DM(hi, lo, timeperiod=14)
    df['RSI'] = talib.RSI(cl, timeperiod=14)
    df['STOCH_SLOWK'], df['STOCH_SLOWD'] = talib.STOCH(
        hi, lo, cl, fastk_period=5, slowk_period=14, slowk_matype=0, slowd_period=9, slowd_matype=0)
    df['STOCHF_FASTK'], df['STOCHF_FASTD'] = talib.STOCHF(
        hi, lo, cl, fastk_period=5, fastd_period=14, fastd_matype=0)
    df['STOCHRSI_FASTK'], df['STOCHRSI_FASTD'] = talib.STOCHRSI(
        cl, timeperiod=14, fastk_period=9, fastd_period=3, fastd_matype=0)
    df['ULTOSC'] = talib.ULTOSC(
        hi, lo, cl, timeperiod1=6, timeperiod2=12, timeperiod3=18)
    df['WILLR'] = talib.WILLR(hi, lo, cl, timeperiod=14)
    df['BBANDS_UPPER'], df['BBANDS_MIDDLE'], df['BBANDS_LOWER'] = talib.BBANDS(
        cl, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)    # Changed timeperiod from 5 to 20




    df['CCI'] = talib.CCI(hi, lo, cl, timeperiod=20)
    df['CMO'] = talib.CMO(cl, timeperiod=14)  # Changed timeperiod from 14 to 9
    df['MFI'] = talib.MFI(hi, lo, cl, vol, timeperiod=14)
    df['OBV'] = talib.OBV(cl, vol)
    df['SAR'] = talib.SAR(hi, lo, acceleration=0.02, maximum=0.2)
    df['SMA10'] = talib.SMA(cl, timeperiod=10)
    df['SMA50'] = talib.SMA(cl, timeperiod=50)
    df['SMA200'] = talib.SMA(cl, timeperiod=200)
    # df['ROC1'] = talib.ROC(cl, timeperiod=1)  # newly added
    # df['ROC5'] = talib.ROC(cl, timeperiod=5)  # newly added
    # df['ROC22'] = talib.ROC(cl, timeperiod=22)  # newly added
    # df['ROC66'] = talib.ROC(cl, timeperiod=66)  # newly added
    # df['ROC126'] = talib.ROC(cl, timeperiod=126)  # newly added
    # df['ROC252'] = talib.ROC(cl, timeperiod=252)  # newly added
    df['MACD'], df['MACDSIGNAL'], df['MACDHIST'] = talib.MACD(
        cl, fastperiod=12, slowperiod=26, signalperiod=9)  # newly added
#Efficiency ratio  - to filter out the noise 
    # df['SUMROC'] = talib.SUM(df['ROC1'], timeperiod=30) 
    # df['ROC30'] = talib.ROC(cl, timeperiod=30)
    # df['ER'] = df['ROC30'] / df['SUMROC']
    df['StockBeta'] = talib.BETA(cl, index, timeperiod=60)

# MACD over On Balance Volume 
    df['MACD_OBV'], df['MACDSIGNAL_OBV'], df['MACDHIST_OBV'] = talib.MACD(
        df['OBV'], fastperiod=12, slowperiod=26, signalperiod=9)

    df['MACD_ATR'], df['MACDSIGNAL_ATR'], df['MACDHIST_OBV_ATR'] = talib.MACD(
    df['ATR'], fastperiod=12, slowperiod=26, signalperiod=9)

#Strongness of the candles 
    df["Cdl_Bullish"] = ((cl - lo) / (hi - lo))
    df["Cdl_Bearish"] = ((hi - cl) / (hi - lo))    
#Linear Regression slope
    # df['Linear_Slope'] = talib.BETALINEARREG_SLOPE(cl, timeperiod=9)
    # df['Corre_Coeff'] = talib.CORREL(hi, lo, timeperiod=9)
    #    


    # df['STDDEV3'] = talib.STDDEV(df['ROC1'], timeperiod=3, nbdev=1)
    # df['STDDEV10'] = talib.STDDEV(df['ROC1'], timeperiod=10, nbdev=1)
    # df['STDDEV14'] = talib.STDDEV(df['ROC1'], timeperiod=14, nbdev=1)
    # df['STDDEV60'] = talib.STDDEV(df['ROC1'], timeperiod=60, nbdev=1)
    # df['TSF'] = talib.TSF(cl, timeperiod=14)
    # df['ATR_3'] = talib.ATR(hi, lo, cl, timeperiod=3)
    # df['AVGPRICE'] = talib.AVGPRICE (op, hi, lo, cl)
    # df['BBW%'] = ((df['BBANDS_UPPER'] - df['BBANDS_LOWER']) / df['BBANDS_MIDDLE'])*100
    # df['BBW%_SMA3'] = talib.SMA(df['BBW%'], timeperiod=3)
    # df['BBW%_SMA14'] = talib.SMA(df['BBW%'], timeperiod=14)

    # Newly added
    # df['52W_Low'], df['52W_High'] = talib.MINMAX(cl, timeperiod=252)
    df['26W_Low'], df['26W_High'] = talib.MINMAX(cl, timeperiod=126)
    # df['Up_52W_Low'] = ((cl /df['52W_Low'])-1)*100
    # df['Down_52W_high'] = ((cl /df['52W_High'])-1)*100
    # df['Up_26W_Low'] = ((cl /df['26W_Low'])-1)*100
    # df['Down_26W_high'] = ((cl /df['26W_High'])-1)*100
    # df['Price_Momentum_52w'] = ((cl - df['52W_Low'])/(df['52W_High'] - df['52W_Low']))*100
    df['Price_Momentum_26w'] = ((cl - df['26W_Low'])/(df['26W_High'] - df['26W_Low']))*100
    # df['ROC1_OBV'] = talib.ROC(df['OBV'], timeperiod=1)
    # df['ROC5_OBV'] = talib.ROC(df['OBV'], timeperiod=5)
    # df['ROC22_OBV'] = talib.ROC(df['OBV'], timeperiod=22)
    # df['ROC66_OBV'] = talib.ROC(df['OBV'], timeperiod=66)
    # df['ROC126_OBV'] = talib.ROC(df['OBV'], timeperiod=126)
    # df['ROC252_OBV'] = talib.ROC(df['OBV'], timeperiod=252)
    df['26W_Low_OBV'], df['26W_High_OBV'] = talib.MINMAX(df['OBV'], timeperiod=126)
    df['OBV_Momentum_26w'] = (
        (df['OBV'] - df['26W_Low_OBV'])/(df['26W_High_OBV'] - df['26W_Low_OBV']))*100
    # df['DistancetoBBU'] = ((df['BBANDS_UPPER'] / cl)-1)*100
    # df['DistancetoBBL'] = ((df['BBANDS_LOWER'] / cl)-1)*100
    # df['WSD'] = ((df['STDDEV3']) + (df['STDDEV10']) + (df['STDDEV14']) + (df['STDDEV60']) ) / 4
    # df['ROC1_WSD'] = talib.ROC(df['WSD'], timeperiod=1)
    # df['ROC5_WSD'] = talib.ROC(df['WSD'], timeperiod=5)
    # df['ROC22_WSD'] = talib.ROC(df['WSD'], timeperiod=22)
    # df['ROC66_WSD'] = talib.ROC(df['WSD'], timeperiod=66)
    # df['ROC126_WSD'] = talib.ROC(df['WSD'], timeperiod=126)
    # df['ROC252_WSD'] = talib.ROC(df['WSD'], timeperiod=252)
    # df['26W_Low_WSD'], df['26W_High_WSD'] = talib.MINMAX(df['WSD'], timeperiod=126)
    # df['WSD_Momentum_26w'] = ((df['WSD'] - df['26W_Low_WSD'])/(df['26W_High_WSD'] - df['26W_Low_WSD']))*100
    # df['Price_Strength'] = (((df['ROC1']) + (((df['ROC5'] - df['ROC1'])/4))  + (((df['ROC22'] - df['ROC5'])/17)) + ((( df['ROC66'] - df['ROC22'])/44))) / ((((df['ROC126'] - df['ROC66'])/60) * 2) + (((df['ROC252'] - df['ROC126'])/126) * 2)))
    # df['OBV_Strength'] = (((df['ROC1_OBV']) + (((df['ROC5_OBV'] - df['ROC1_OBV'])/4))  + (((df['ROC22_OBV'] - df['ROC5_OBV'])/17)) + ((( df['ROC66_OBV'] - df['ROC22_OBV'])/44))) / ((((df['ROC126_OBV'] - df['ROC66_OBV'])/60) * 2) + (((df['ROC252_OBV'] - df['ROC126_OBV'])/126) * 2)))
    # df['WSD_Strength'] = (((df['ROC1_WSD']) + (((df['ROC5_WSD'] - df['ROC1_WSD'])/4))  + (((df['ROC22_WSD'] - df['ROC5_WSD'])/17)) + ((( df['ROC66_WSD'] - df['ROC22_WSD'])/44))) / ((((df['ROC126_WSD'] - df['ROC66_WSD'])/60) * 2) + (((df['ROC252_WSD'] - df['ROC126_WSD'])/126) * 2)))
    # df['SAR_Gap'] = ((df['SAR'] / cl) -1) *100
    # df["R1"] = ( (df['AVGPRICE'] * 2) - lo)
    # df["R2"] = (df['AVGPRICE'] + (hi - lo) )
    # df["R3"] = (hi + (2 * (df['AVGPRICE'] - lo) ) )
    # df["S1"] = ( (df['AVGPRICE'] * 2) - hi)
    # df["S2"] = (df['AVGPRICE'] - (hi - lo) )
    # df["S3"] = (lo - (2 * (hi - df['AVGPRICE']) ) )
    # df["Cdl_Bullish"] = ((cl - lo) / (hi - lo))
    # df["Cdl_Bearish"] = ((hi - cl) / (hi - lo))
    # df['TRANGE'] = talib.TRANGE(hi, lo, cl)
    # df['SMA10_OBV'] = talib.SMA(df['OBV'], timeperiod=10)
    # df['SMA100_OBV'] = talib.SMA(df['OBV'], timeperiod=100)
    # df['R_Vol'] = (df['SMA10_OBV'] / df['SMA100_OBV'])
    # df['R_TR'] = df['TRANGE'] / df['ATR_3']
    # df['BETA_PV100'] = talib.BETA(cl, df['OBV'], timeperiod=100)
    # df['BETA_PV20'] = talib.BETA(cl, df['OBV'], timeperiod=20)
    # df['StockBeta'] = talib.BETA(cl, index, timeperiod=252)
    # df['AbsRS'] = (cl / index)
    # df['ROC1_RS'] = talib.ROC(df['AbsRS'], timeperiod=1)
    # df['SMA20_RS'] = talib.SMA(df['ROC1_RS'], timeperiod=10)
    # df['SMA100_RS'] = talib.SMA(df['ROC1_RS'], timeperiod=50)


# INDICTORS FOR TEST
    # df['HT_DCPERIOD'] = talib.HT_DCPERIOD(cl)
    # df['HT_DCPHASE'] = talib.HT_DCPHASE(cl)
    # df['HT_Inphase'], df['HT_Quadrature'] = talib.HT_PHASOR(cl)
    # df['HT_sine'], df['HT_Leadsine'] = talib.HT_SINE(cl)
    df['HT_TRENDMODE'] = talib.HT_TRENDMODE(cl)
    return df



def getTrendIdNew(lastRec, last1Rec, last2Rec, ID):
    IDVal = pd.to_numeric(lastRec[ID].values.ravel())
    prevIDVal = pd.to_numeric(last1Rec[ID].head(1).values.ravel())
    prevIDVal2 = pd.to_numeric(last2Rec[ID].head(1).values.ravel())

    curTrendId = ""
    if (float(IDVal) > float(prevIDVal)):
        curTrendId = "UP"
    elif (float(IDVal) < float(prevIDVal)):
        curTrendId = "DOWN"

    prevTrendId = ""
    if (float(prevIDVal) > float(prevIDVal2)):
        prevTrendId = "UP"
    elif (float(prevIDVal) < float(prevIDVal2)):
        prevTrendId = "DOWN"
    return curTrendId, prevTrendId


def getTrendSARId(lastRec, ID, prevFlag):
    IDVal = ""
    price = ""
    if (prevFlag != "Y"):
        IDVal = pd.to_numeric(lastRec[ID].values.ravel())
        price = pd.to_numeric(lastRec['close'].values.ravel())
    else:
        IDVal = pd.to_numeric(lastRec[ID].head(1).values.ravel())
        price = pd.to_numeric(lastRec['close'].head(1).values.ravel())
    trendId = ""
    if (IDVal > price):
        trendId = "DOWN"
    elif (IDVal < price):
        trendId = "UP"
    return trendId


def getTrendMACDId(lastRec, last1Rec, ID, prevFlag):
    MACD = ""
    MACDSIGNAL = ""
    MACDHIST = ""

    if (prevFlag != "Y"):
        MACD = lastRec['MACD'].values.ravel()
        MACDSIGNAL = lastRec['MACDSIGNAL'].values.ravel()
        MACDHIST = lastRec['MACDHIST'].values.ravel()
    else:
        MACD = last1Rec['MACD'].head(1).values.ravel()
        MACDSIGNAL = last1Rec['MACDSIGNAL'].head(1).values.ravel()
        MACDHIST = last1Rec['MACDHIST'].head(1).values.ravel()

    trendMACD = ""
    if ((MACDSIGNAL > MACD)):
        trendMACD = "UP"
    elif ((MACDSIGNAL < MACD)):
        trendMACD = "DOWN"
    return trendMACD


def getTrendSMAId(lastRec, ID, prevFlag):
    IDVal = ""
    price = ""

    if (prevFlag != "Y"):
        IDVal = pd.to_numeric(lastRec[ID].values.ravel())
        price = pd.to_numeric(lastRec['close'].values.ravel())
    else:
        IDVal = pd.to_numeric(lastRec[ID].head(1).values.ravel())
        price = pd.to_numeric(lastRec['close'].head(1).values.ravel())

    trendId = ""

    if (ID == "SMA200"):
        if (price > IDVal):
            trendId = "UP"
        elif (price < IDVal):
            trendId = "DOWN"
    elif (ID == "SMA50"):
        if (price > IDVal):
            trendId = "UP"
        elif (price < IDVal):
            trendId = "DOWN"
    elif (ID == "SMA10"):
        if (price > IDVal):
            trendId = "UP"
        elif (price < IDVal):
            trendId = "DOWN"

    return trendId


def getTrendADOSC(lastRec, last5Rec, ID, AVG_ID, prevFlag):
    IDVal = ""
    avgRec = ""

    if (prevFlag != "Y"):
        IDVal = pd.to_numeric(lastRec[ID].values.ravel())
        avgRec = last5Rec[AVG_ID].tail(1).values.ravel()
    else:
        IDVal = pd.to_numeric(lastRec[ID].head(1).values.ravel())
        avgRec = last5Rec[AVG_ID].tail(1).values.ravel()

    trendId = ""

    if ((IDVal > 0) and IDVal > avgRec):
        trendId = "UP"
    elif (IDVal < 0 and IDVal < avgRec):
        trendId = "DOWN"
    return trendId


def getQuantityId(lastRec, last1Rec, ID, overBht, overSld, prevFlag):
    IDVal = ""
    if (prevFlag != "Y"):
        IDVal = pd.to_numeric(lastRec[ID].values.ravel())
    else:
        IDVal = pd.to_numeric(last1Rec[ID].head(1).values.ravel())

    quantityId = ""
    if (IDVal > overBht):
        quantityId = "OVER_BOUGHT"
    elif (IDVal < overSld):
        quantityId = "OVER_SOLD"
    return quantityId


def getCrossOverId(lastRec, last1Rec, last2Rec, ID, coNo, prevFlag):
    IDVal = ""
    crossOverVal = ""

    if (prevFlag != "Y"):
        IDVal = lastRec[ID].values.ravel()
        crossOverVal = last1Rec[ID].head(1).values.ravel()
    else:
        IDVal = pd.to_numeric(last1Rec[ID].head(1).values.ravel())
        crossOverVal = last2Rec[ID].head(1).values.ravel()

    crossOverId = ""

    if (IDVal > coNo and crossOverVal < coNo):
        crossOverId = "UP"
    elif (crossOverVal > coNo and IDVal < coNo):
        crossOverId = "DOWN"
    return crossOverId


def getReversalId(lastRec, last1Rec, last2Rec, ID, revUp, revDown, prevFlag):
    IDVal = ""
    reversalVal = ""

    if (prevFlag != "Y"):
        IDVal = lastRec[ID].values.ravel()
        reversalVal = last1Rec[ID].head(1).values.ravel()
    else:
        IDVal = pd.to_numeric(last1Rec[ID].head(1).values.ravel())
        reversalVal = last2Rec[ID].head(1).values.ravel()

    reversalId = ""
    if (IDVal > revUp and reversalVal < revUp):
        reversalId = "UP"
    elif (reversalVal > revDown and IDVal < revDown):
        reversalId = "DOWN"
    return reversalId


def getSignalScoreAROON_MINUS(indScore, indWeight, curScore, prevScore):
    buyScore = 0
    sellScore = 0
    # Calculate Buy Score
    if (curScore == "DOWN"):
        buyScore = indScore * indWeight
    if (prevScore == "DOWN"):
        buyScore = buyScore + (indScore * indWeight)

    # Calculate Sell Score
    if (curScore == "UP"):
        sellScore = indScore * indWeight
    if (prevScore == "UP"):
        sellScore = sellScore + (indScore * indWeight)

    # Return both buy score and sell score
    return buyScore, (-1 * sellScore)


def getSignalScore(indScore, indWeight, curScore, prevScore):
    buyScore = 0
    sellScore = 0
    # Calculate Buy Score
    if (curScore == "UP" or curScore == "OVER_BOUGHT"):
        buyScore = indScore * indWeight
    if (prevScore == "UP" or prevScore == "OVER_BOUGHT"):
        buyScore = buyScore + (indScore * indWeight)

    # Calculate Sell Score
    if (curScore == "DOWN" or curScore == "OVER_SOLD"):
        sellScore = indScore * indWeight
    if (prevScore == "DOWN" or prevScore == "OVER_SOLD"):
        sellScore = sellScore + (indScore * indWeight)

    # Return both buy score and sell score
    return buyScore, (-1 * sellScore)


def getCDLSignalScore(lastRec, last1Rec, indScore, indWeight):
    buyScore = 0
    sellScore = 0

    # Iterate over the sequence of column names
    for columnName in lastRec:
        # Select column contents by column name using [] operator
        columnVal = lastRec[columnName].values.ravel()
        if(str(columnName).startswith('CDL', 0, 3)):
            if (columnVal == 100):
                buyScore = buyScore + (indScore * indWeight)
            if (columnVal == -100):
                sellScore = sellScore + (indScore * indWeight)

    # Iterate over the sequence of column names
    for columnName in last1Rec.head(1):
        # Select column contents by column name using [] operator
        columnVal = last1Rec[columnName].head(1).values.ravel()
        if(str(columnName).startswith('CDL', 0, 3)):
            if (columnVal == 100):
                buyScore = buyScore + (indScore * indWeight)
            if (columnVal == -100):
                sellScore = sellScore + (indScore * indWeight)

    return buyScore, (-1 * sellScore)

def getBuySellScore(df):

    global trendWeight
    # Checked
    if (df.shape[0] > 200):
        op = df['open']
        hi = df['high']
        lo = df['low']
        cl = df['close']
        vol = df['volume']
        index = df['index']

        df = getCandleStickPatterns(df, op, hi, lo, cl, vol)
        df = getTechnicalIndicators(df, op, hi, lo, cl, vol, index)

        lastRec = df.tail(1)
        last1Rec = df.tail(2)
        last2Rec = df.tail(3)
        last5Rec = df.tail(5)
        last6Rec = df.tail(6)
        last6Rec = last6Rec.head(5)
        last5Rec['AVG_ADOSC'] = talib.SMA(last5Rec['ADOSC'], timeperiod=5)
        
        # Obtain trends for MACD
        trendMACD = getTrendMACDId(lastRec, last1Rec, "MACD", "N")
        prevTrendMACD = getTrendMACDId(lastRec, last1Rec, "MACD", "Y")
        crossOverMACD = getCrossOverId(
            lastRec, last1Rec, last2Rec, "MACDHIST", 0, "N")
        prevCrossOverMACD = getCrossOverId(
            lastRec, last1Rec, last2Rec, "MACDHIST", 0, "Y")
        # Obtain trends for RSI
        trendRSI, prevTrendRSI = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "RSI")
        quantityRSI = getQuantityId(lastRec, last1Rec, "RSI", 70, 30, "N")
        prevQuantityRSI = getQuantityId(lastRec, last1Rec, "RSI", 70, 30, "Y")
        crossOverRSI = getCrossOverId(
            lastRec, last1Rec, last2Rec, "RSI", 50, "N")
        prevCrossOverRSI = getCrossOverId(
            lastRec, last1Rec, last2Rec, "RSI", 50, "Y")
        reversalRSI = getReversalId(
            lastRec, last1Rec, last2Rec, "RSI", 30, 70, "N")
        prevReversalRSI = getReversalId(
            lastRec, last1Rec, last2Rec, "RSI", 30, 70, "Y")

        # Obtain trends for CMO
        trendCMO, prevTrendCMO = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "CMO")
        quantityCMO = getQuantityId(lastRec, last1Rec, "CMO", 50, -50, "N")
        prevQuantityCMO = getQuantityId(lastRec, last1Rec, "CMO", 50, -50, "Y")
        crossOverCMO = getCrossOverId(
            lastRec, last1Rec, last2Rec, "CMO", 0, "N")
        prevCrossOverCMO = getCrossOverId(
            lastRec, last1Rec, last2Rec, "CMO", 0, "Y")
        reversalCMO = getReversalId(
            lastRec, last1Rec, last2Rec, "CMO", -50, 50, "N")
        prevReversalCMO = getReversalId(
            lastRec, last1Rec, last2Rec, "CMO", -50, 50, "Y")

        # Obtain trends for MFI
        trendMFI, prevTrendMFI = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "MFI")
        quantityMFI = getQuantityId(lastRec, last1Rec, "MFI", 80, 20, "N")
        prevQuantityMFI = getQuantityId(lastRec, last1Rec, "MFI", 80, 20, "Y")
        crossOverMFI = getCrossOverId(
            lastRec, last1Rec, last2Rec, "MFI", 50, "N")
        prevCrossOverMFI = getCrossOverId(
            lastRec, last1Rec, last2Rec, "MFI", 50, "Y")
        reversalMFI = getReversalId(
            lastRec, last1Rec, last2Rec, "MFI", 20, 80, "N")
        prevReversalMFI = getReversalId(
            lastRec, last1Rec, last2Rec, "MFI", 20, 80, "Y")

        # Obtain trends for STOCH_SLOWD
        trendSTOCH_SLOWD, prevTrendSTOCH_SLOWD = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "STOCH_SLOWD")
        crossOverSTOCH_SLOWD = getCrossOverId(
            lastRec, last1Rec, last2Rec, "STOCH_SLOWD", 50, "N")
        prevCrossOverSTOCH_SLOWD = getCrossOverId(
            lastRec, last1Rec, last2Rec, "STOCH_SLOWD", 50, "Y")
        quantitySTOCH_SLOWD = getQuantityId(
            lastRec, last1Rec, "STOCH_SLOWD", 80, 20, "N")
        prevQuantitySTOCH_SLOWD = getQuantityId(
            lastRec, last1Rec, "STOCH_SLOWD", 80, 20, "Y")
        reversalSTOCH_SLOWD = getReversalId(
            lastRec, last1Rec, last2Rec, "STOCH_SLOWD", 20, 80, "N")
        prevReversalSTOCH_SLOWD = getReversalId(
            lastRec, last1Rec, last2Rec, "STOCH_SLOWD", 20, 80, "Y")

        # Obtain trends for ULTOSC
        trendULTOSC, prevTrendULTOSC = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "ULTOSC")
        quantityULTOSC = getQuantityId(
            lastRec, last1Rec, "ULTOSC", 70, 30, "N")
        prevQuantityULTOSC = getQuantityId(
            lastRec, last1Rec, "ULTOSC", 70, 30, "Y")
        crossOverULTOSC = getCrossOverId(
            lastRec, last1Rec, last2Rec, "ULTOSC", 50, "N")
        prevCrossOverULTOSC = getCrossOverId(
            lastRec, last1Rec, last2Rec, "ULTOSC", 50, "Y")
        reversalULTOSC = getReversalId(
            lastRec, last1Rec, last2Rec, "ULTOSC", 30, 70, "N")
        prevReversalULTOSC = getReversalId(
            lastRec, last1Rec, last2Rec, "ULTOSC", 30, 70, "Y")

        # Obtain trends for WILLR
        trendWILLR, prevTrendWILLR = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "WILLR")
        quantityWILLR = getQuantityId(
            lastRec, last1Rec, "WILLR", -20, -80, "N")
        prevQuantityWILLR = getQuantityId(
            lastRec, last1Rec, "WILLR", -20, -80, "Y")
        crossOverWILLR = getCrossOverId(
            lastRec, last1Rec, last2Rec, "WILLR", -50, "N")
        prevCrossOverWILLR = getCrossOverId(
            lastRec, last1Rec, last2Rec, "WILLR", -50, "Y")
        reversalWILLR = getReversalId(
            lastRec, last1Rec, last2Rec, "WILLR", -80, -20, "N")
        prevReversalWILLR = getReversalId(
            lastRec, last1Rec, last2Rec, "WILLR", -80, -20, "Y")

        # Obtain trends for WILLR
        trendCCI, prevTrendCCI = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "CCI")
        quantityCCI = getQuantityId(lastRec, last1Rec, "CCI", 100, -100, "N")
        prevQuantityCCI = getQuantityId(
            lastRec, last1Rec, "CCI", 100, -100, "Y")
        crossOverCCI = getCrossOverId(
            lastRec, last1Rec, last2Rec, "CCI", 0, "N")
        prevCrossOverCCI = getCrossOverId(
            lastRec, last1Rec, last2Rec, "CCI", 0, "Y")
        reversalCCI = getReversalId(
            lastRec, last1Rec, last2Rec, "CCI", 100, -100, "N")
        prevReversalCCI = getReversalId(
            lastRec, last1Rec, last2Rec, "CCI", 100, -100, "Y")

        # Obtain trends for ADOSC
        trendADOSC = getTrendADOSC(lastRec, last5Rec, "ADOSC", "AVG_ADOSC", "")
        prevTrendADOSC = getTrendADOSC(
            last1Rec, last6Rec, "ADOSC", "AVG_ADOSC", "Y")
        crossOverADOSC = getCrossOverId(
            lastRec, last1Rec, last2Rec, "ADOSC", 0, "N")
        prevCrossOverADOSC = getCrossOverId(
            lastRec, last1Rec, last2Rec, "ADOSC", 0, "Y")
        reversalADOSC = getReversalId(
            lastRec, last1Rec, last2Rec, "ADOSC", 30, 70, "N")
        prevReversalADOSC = getReversalId(
            lastRec, last1Rec, last2Rec, "ADOSC", 30, 70, "Y")
        # Obtain trends for SAR
        trendSAR = getTrendSARId(lastRec, "SAR", "N")
        prevTrendSAR = getTrendSARId(last1Rec, "SAR", "Y")
        # Obtain trends for SMA
        trendSMA200 = getTrendSMAId(lastRec, "SMA200", "")
        trendSMA50 = getTrendSMAId(lastRec, "SMA50", "")
        trendSMA10 = getTrendSMAId(lastRec, "SMA10", "")
        prevTrendSMA200 = getTrendSMAId(last1Rec, "SMA200", "Y")
        prevTrendSMA50 = getTrendSMAId(last1Rec, "SMA50", "Y")
        prevTrendSMA10 = getTrendSMAId(last1Rec, "SMA10", "Y")

        # AROON_UP and DOWN
        trendAROON_UP, prevTrendAROON_UP = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "AROON_UP")
        trendAROON_DOWN, prevTrendAROON_DOWN = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "AROON_DOWN")
        trendOBV, prevTrendOBV = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "OBV")
        trendAD, prevTrendAD = getTrendIdNew(lastRec, last1Rec, last2Rec, "AD")

        # STOCHRSI_FASTD
        trendSTOCHRSI_FASTD, prevTrendSTOCHRSI_FASTD = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "STOCHRSI_FASTD")
        quantitySTOCHRSI_FASTD = getQuantityId(
            lastRec, last1Rec, "STOCHRSI_FASTD", 80, 20, "N")
        prevQuantitySTOCHRSI_FASTD = getQuantityId(
            lastRec, last1Rec, "STOCHRSI_FASTD", 80, 20, "Y")
        crossOverSTOCHRSI_FASTD = getCrossOverId(
            lastRec, last1Rec, last2Rec, "STOCHRSI_FASTD", 50, "N")
        prevCrossOverSTOCHRSI_FASTD = getCrossOverId(
            lastRec, last1Rec, last2Rec, "STOCHRSI_FASTD", 50, "Y")
        reversalSTOCHRSI_FASTD = getReversalId(
            lastRec, last1Rec, last2Rec, "STOCHRSI_FASTD", 20, 80, "N")
        prevReversalSTOCHRSI_FASTD = getReversalId(
            lastRec, last1Rec, last2Rec, "STOCHRSI_FASTD", 20, 80, "Y")

        # PLUS_DI AND MINUS_DI
        trendPLUS_DI, prevTrendPLUS_DI = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "PLUS_DI")
        trendMINUS_DI, prevTrendMINUS_DI = getTrendIdNew(
            lastRec, last1Rec, last2Rec, "MINUS_DI")


        # Calculate Buy and Sell Scores for Trends
        # *************************START*************************************
        buyTRScoreMACD, sellTRScoreMACD = getSignalScore(
            trendScore, trendWeight, trendMACD, prevTrendMACD)
        buyTRScoreRSI, sellTRScoreRSI = getSignalScore(
            trendScore, trendWeight, trendRSI, prevTrendRSI)
        buyTRScoreCMO, sellTRScoreCMO = getSignalScore(
            trendScore, trendWeight, trendCMO, prevTrendCMO)
        buyTRScoreMFI, sellTRScoreMFI = getSignalScore(
            trendScore, trendWeight, trendMFI, prevTrendMFI)
        buyTRScoreULTOSC, sellTRScoreULTOSC = getSignalScore(
            trendScore, trendWeight, trendULTOSC, prevTrendULTOSC)
        buyTRScoreWILLR, sellTRScoreWILLR = getSignalScore(
            trendScore, trendWeight, trendWILLR, prevTrendWILLR)
        buyTRScoreCCI, sellTRScoreCCI = getSignalScore(
            trendScore, trendWeight, trendCCI, prevTrendCCI)
        buyTRScoreAROON_UP, sellTRScoreAROON_UP = getSignalScore(
            trendScore, trendWeight, trendAROON_UP, prevTrendAROON_UP)
        buyTRScoreAROON_DOWN, sellTRScoreAROON_DOWN = getSignalScoreAROON_MINUS(
            trendScore, trendWeight, trendAROON_DOWN, prevTrendAROON_DOWN)
        buyTRScoreADOSC, sellTRScoreADOSC = getSignalScore(
            trendScore, trendWeight, trendADOSC, prevTrendADOSC)
        buyTRScoreOBV, sellTRScoreOBV = getSignalScore(
            trendScore, trendWeight, trendOBV, prevTrendOBV)
        buyTRScoreAD, sellTRScoreAD = getSignalScore(
            trendScore, trendWeight, trendAD, prevTrendAD)
        buyTRScoreSTOCHRSI_FASTD, sellTRScoreSTOCHRSI_FASTD = getSignalScore(
            trendScore, trendWeight, trendSTOCHRSI_FASTD, prevTrendSTOCHRSI_FASTD)
        buyTRScorePLUS_DI, sellTRScorePLUS_DI = getSignalScore(
            trendScore, trendWeight, trendPLUS_DI, prevTrendPLUS_DI)
        buyTRScoreMINUS_DI, sellTRScoreMINUS_DI = getSignalScoreAROON_MINUS(
            trendScore, trendWeight, trendMINUS_DI, prevTrendMINUS_DI)
        buyTRScoreSAR, sellTRScoreSAR = getSignalScore(
            trendScore, trendWeight, trendSAR, prevTrendSAR)
        buyTRScoreSTOCH_SLOWD, sellTRScoreSTOCH_SLOWD = getSignalScore(
            trendScore, trendWeight, trendSTOCH_SLOWD, prevTrendSTOCH_SLOWD)
        buyTRScoreSMA200, sellTRScoreSMA200 = getSignalScore(
            trendScore, trendWeight, trendSMA200, prevTrendSMA200)
        buyTRScoreSMA50, sellTRScoreSMA50 = getSignalScore(
            trendScore, trendWeight, trendSMA50, prevTrendSMA50)
        buyTRScoreSMA10, sellTRScoreSMA10 = getSignalScore(
            trendScore, trendWeight, trendSMA10, prevTrendSMA10)

        # Calculate total score of all Trends for both Buy and Sell
        trendBuyScore = buyTRScoreRSI + buyTRScoreCMO + buyTRScoreMACD + buyTRScoreMFI + buyTRScoreSTOCH_SLOWD + buyTRScoreULTOSC + buyTRScoreWILLR + buyTRScoreCCI + buyTRScoreADOSC + buyTRScorePLUS_DI + \
            buyTRScoreMINUS_DI + buyTRScoreSAR + buyTRScoreAROON_UP + buyTRScoreAROON_DOWN + buyTRScoreOBV + \
            buyTRScoreAD + buyTRScoreSTOCHRSI_FASTD + \
            buyTRScoreSMA200 + buyTRScoreSMA50 + buyTRScoreSMA10
        trendSellScore = sellTRScoreRSI + sellTRScoreCMO + sellTRScoreMACD + sellTRScoreMFI + sellTRScoreSTOCH_SLOWD + sellTRScoreULTOSC + sellTRScoreWILLR + sellTRScoreCCI + sellTRScoreADOSC + sellTRScorePLUS_DI + \
            sellTRScoreMINUS_DI + sellTRScoreSAR + sellTRScoreAROON_UP + sellTRScoreAROON_DOWN + sellTRScoreOBV + \
            sellTRScoreAD + sellTRScoreSTOCHRSI_FASTD + \
            sellTRScoreSMA200 + sellTRScoreSMA50 + sellTRScoreSMA10
        # *************************END*************************************

        # Calculate Buy and Sell Scores for Cross Over Midpoint
        # *************************START*************************************
        buyCOScoreMACD, sellCOScoreMACD = getSignalScore(
            coScore, coWeight, crossOverMACD, prevCrossOverMACD)
        buyCOScoreRSI, sellCOScoreRSI = getSignalScore(
            coScore, coWeight, crossOverRSI, prevCrossOverRSI)
        buyCOScoreCMO, sellCOScoreCMO = getSignalScore(
            coScore, coWeight, crossOverCMO, prevCrossOverCMO)
        buyCOScoreMFI, sellCOScoreMFI = getSignalScore(
            coScore, coWeight, crossOverMFI, prevCrossOverMFI)
        buyCOScoreSTOCH_SLOWD, sellCOScoreSTOCH_SLOWD = getSignalScore(
            coScore, coWeight, crossOverSTOCH_SLOWD, prevCrossOverSTOCH_SLOWD)
        buyCOScoreULTOSC, sellCOScoreULTOSC = getSignalScore(
            coScore, coWeight, crossOverULTOSC, prevCrossOverULTOSC)
        buyCOScoreWILLR, sellCOScoreWILLR = getSignalScore(
            coScore, coWeight, crossOverWILLR, prevCrossOverWILLR)
        buyCOScoreCCI, sellCOScoreCCI = getSignalScore(
            coScore, coWeight, crossOverCCI, prevCrossOverCCI)
        buyCOScoreADOSC, sellCOScoreADOSC = getSignalScore(
            coScore, coWeight, crossOverADOSC, prevCrossOverADOSC)
        buyCOScoreSTOCHRSI_FASTD, sellCOScoreSTOCHRSI_FASTD = getSignalScore(
            coScore, coWeight, crossOverSTOCHRSI_FASTD, prevCrossOverSTOCHRSI_FASTD)

        # Calculate total score of all Cross Overs for both Buy and Sell
        coBuyScore = buyCOScoreSTOCHRSI_FASTD + buyCOScoreMACD + buyCOScoreRSI + buyCOScoreCMO + buyCOScoreMFI + \
            buyCOScoreSTOCH_SLOWD + buyCOScoreULTOSC + \
            buyCOScoreWILLR + buyCOScoreCCI + buyCOScoreADOSC
        coSellScore = sellCOScoreSTOCHRSI_FASTD + sellCOScoreMACD + sellCOScoreRSI + sellCOScoreCMO + sellCOScoreMFI + \
            sellCOScoreSTOCH_SLOWD + sellCOScoreULTOSC + \
            sellCOScoreWILLR + sellCOScoreCCI + sellCOScoreADOSC
        # *************************END*************************************

        # Calculate Buy and Sell Scores for Reversal Indicator
        # *************************START*************************************
        buyReversalScoreRSI, sellReversalScoreRSI = getSignalScore(
            reversalScore, reversalWeight, reversalRSI, prevReversalRSI)
        buyReversalScoreCMO, sellReversalScoreCMO = getSignalScore(
            reversalScore, reversalWeight, reversalCMO, prevReversalCMO)
        buyReversalScoreMFI, sellReversalScoreMFI = getSignalScore(
            reversalScore, reversalWeight, reversalMFI, prevReversalMFI)
        buyReversalScoreSTOCH_SLOWD, sellReversalScoreSTOCH_SLOWD = getSignalScore(
            reversalScore, reversalWeight, reversalSTOCH_SLOWD, prevReversalSTOCH_SLOWD)
        buyReversalScoreSTOCHRSI_FASTD, sellReversalScoreSTOCHRSI_FASTD = getSignalScore(
            reversalScore, reversalWeight, reversalSTOCHRSI_FASTD, prevReversalSTOCHRSI_FASTD)
        buyReversalScoreULTOSC, sellReversalScoreULTOSC = getSignalScore(
            reversalScore, reversalWeight, reversalULTOSC, prevReversalULTOSC)
        buyReversalScoreWILLR, sellReversalScoreWILLR = getSignalScore(
            reversalScore, reversalWeight, reversalWILLR, prevReversalWILLR)
        buyReversalScoreCCI, sellReversalScoreCCI = getSignalScore(
            reversalScore, reversalWeight, reversalCCI, prevReversalCCI)
        buyReversalScoreADOSC, sellReversalScoreADOSC = getSignalScore(
            reversalScore, reversalWeight, reversalADOSC, prevReversalADOSC)

        # Calculate total score of all Reversal Indicator for both Buy and Sell
        reversalBuyScore = buyReversalScoreRSI + buyReversalScoreCMO + buyReversalScoreMFI + buyReversalScoreSTOCHRSI_FASTD + \
            buyReversalScoreSTOCH_SLOWD + buyReversalScoreULTOSC + \
            buyReversalScoreWILLR + buyReversalScoreCCI + buyReversalScoreADOSC
        reversalSellScore = sellReversalScoreRSI + sellReversalScoreCMO + sellReversalScoreMFI + sellReversalScoreSTOCHRSI_FASTD + \
            sellReversalScoreSTOCH_SLOWD + sellReversalScoreULTOSC + \
            sellReversalScoreWILLR + sellReversalScoreCCI + sellReversalScoreADOSC
        # *************************END*************************************

        # Calculate Buy and Sell Scores for Over Bought and Over Sold
        # *************************START*************************************
        buyQuantityScoreRSI, sellQuantityScoreRSI = getSignalScore(
            quantiyScore, quantiyWeight, quantityRSI, prevQuantityRSI)
        buyQuantityScoreCMO, sellQuantityScoreCMO = getSignalScore(
            quantiyScore, quantiyWeight, quantityCMO, prevQuantityCMO)
        buyQuantityScoreMFI, sellQuantityScoreMFI = getSignalScore(
            quantiyScore, quantiyWeight, quantityMFI, prevQuantityMFI)
        buyQuantityScoreSTOCHRSI_FASTD, sellQuantityScoreSTOCHRSI_FASTD = getSignalScore(
            quantiyScore, quantiyWeight, quantitySTOCHRSI_FASTD, prevQuantitySTOCHRSI_FASTD)
        buyQuantityScoreSTOCH_SLOWD, sellQuantityScoreSTOCH_SLOWD = getSignalScore(
            quantiyScore, quantiyWeight, quantitySTOCH_SLOWD, prevQuantitySTOCH_SLOWD)
        buyQuantityScoreULTOSC, sellQuantityScoreULTOSC = getSignalScore(
            quantiyScore, quantiyWeight, quantityULTOSC, prevQuantityULTOSC)
        buyQuantityScoreWILLR, sellQuantityScoreWILLR = getSignalScore(
            quantiyScore, quantiyWeight, quantityWILLR, prevQuantityWILLR)
        buyQuantityScoreCCI, sellQuantityScoreCCI = getSignalScore(
            quantiyScore, quantiyWeight, quantityCCI, prevQuantityCCI)

        # Calculate total score of all Reversal Indicator for both Buy and Sell
        quantityBuyScore = buyQuantityScoreRSI + buyQuantityScoreCMO + buyQuantityScoreMFI + buyQuantityScoreSTOCHRSI_FASTD + \
            buyQuantityScoreSTOCH_SLOWD + buyQuantityScoreULTOSC + \
            buyQuantityScoreWILLR + buyQuantityScoreCCI
        quantitySellScore = sellQuantityScoreRSI + sellQuantityScoreCMO + sellQuantityScoreMFI + sellQuantityScoreSTOCHRSI_FASTD + \
            sellQuantityScoreSTOCH_SLOWD + sellQuantityScoreULTOSC + \
            sellQuantityScoreWILLR + sellQuantityScoreCCI
        # *************************END*************************************

        # Calculate Buy and Sell Scores for Candlestick Patterns
        # *************************START*************************************
        cdlBuyScore, cdlSellScore = getCDLSignalScore(
            lastRec, last1Rec, cdlScore, cdlWeight)
        # *************************END*************************************

        totalBuyScore = trendBuyScore + coBuyScore + \
            reversalBuyScore + quantityBuyScore + cdlBuyScore
        totalSellScore = trendSellScore + coSellScore + \
            reversalSellScore + quantitySellScore + cdlSellScore
        netScore = totalBuyScore + totalSellScore

        return netScore, totalBuyScore,totalSellScore
    else:
        return 0, 0, 0





# Get the candle stick patterns for buillish reversal
def get_csp_for_bullish_reversal(df, op, hi, lo, cl, vol):
    df['CDLHAMMER'] = talib.CDLHAMMER(op, hi, lo, cl)
    df['CDLPIERCING'] = talib.CDLPIERCING(op, hi, lo, cl) # Best Indicator
    df['CDLENGULFING'] = talib.CDLENGULFING(op, hi, lo, cl) # Best Indicator
    df['CDLMORNINGSTAR'] = talib.CDLMORNINGSTAR(op, hi, lo, cl, penetration=0) # Best Indicator
    df['CDLMORNINGDOJISTAR'] = talib.CDLMORNINGDOJISTAR(op, hi, lo, cl, penetration=0) # Best Indicator   
    df['CDL3WHITESOLDIERS'] = talib.CDL3WHITESOLDIERS(op, hi, lo, cl)  # Best Indicator
    df['CDLMARUBOZU'] = talib.CDLMARUBOZU(op, hi, lo, cl)
    df['CDL3INSIDE'] = talib.CDL3INSIDE(op, hi, lo, cl)
    df['CDLHARAMI'] = talib.CDLHARAMI(op, hi, lo, cl)
    df['CDLMATCHINGLOW'] = talib.CDLMATCHINGLOW(op, hi, lo, cl) # Best Indicator
    df['CDLINVERTEDHAMMER'] = talib.CDLINVERTEDHAMMER(op, hi, lo, cl) # Best Indicator
    df['CDL3OUTSIDE'] = talib.CDL3OUTSIDE(op, hi, lo, cl) # Best Indicator
    df['CDLONNECK'] = talib.CDLONNECK(op, hi, lo, cl)
    df['CDLCOUNTERATTACK'] = talib.CDLCOUNTERATTACK(op, hi, lo, cl) 
    df['CDLABANDONEDBABY'] = talib.CDLABANDONEDBABY(op, hi, lo, cl, penetration=0)   # Best Indicator 
    df['CDLBELTHOLD'] = talib.CDLBELTHOLD(op, hi, lo, cl)  
    df['CDLDOJI'] = talib.CDLDOJI(op, hi, lo, cl)
    df['CDLKICKING'] = talib.CDLKICKING(op, hi, lo, cl)
    df['CDLHOMINGPIGEON'] = talib.CDLHOMINGPIGEON(op, hi, lo, cl)
    df['CDLTHRUSTING'] = talib.CDLTHRUSTING(op, hi, lo, cl)  # Best Indicator
    df['CDLINNECK'] = talib.CDLINNECK(op, hi, lo, cl)
    df['CDLTASUKIGAP'] = talib.CDLTASUKIGAP(op, hi, lo, cl)  # Best Indicator

    return df

# cREATED FOR POC
def roc(price, period=1, shift=0):
    val = talib.ROC(price, timeperiod=period).shift(shift).tail(1)
    return pd.to_numeric(val.values.ravel())[0]

def sma(price, period, shift=0):
    val = talib.SMA(price, timeperiod=period).shift(shift).tail(1)
    return pd.to_numeric(val.values.ravel())[0]

def ad(high, low, close, volume, shift=0):
    val = talib.AD(high, low, close, volume).shift(shift).tail(1) 
    return pd.to_numeric(val.values.ravel())[0]

def adosc(high, low, close, volume, fastperiod=5, slowperiod=14, shift=0):
    val = talib.ADOSC(high, low, close, volume, fastperiod=fastperiod, slowperiod=slowperiod).shift(shift).tail(1) 
    return pd.to_numeric(val.values.ravel())[0]

def adx(high, low, close, timeperiod=14, shift=0):
    val = talib.ADX(high, low, close, timeperiod=timeperiod).shift(shift).tail(1)
    return pd.to_numeric(val.values.ravel())[0]

def aroon_down(high, low, timeperiod=14, shift=0):
    val, _ = talib.ADX(high, low, timeperiod=timeperiod).shift(shift).tail(1)
    return pd.to_numeric(val.values.ravel())[0]

def aroon_up(high, low, timeperiod=14, shift=0):
    _, val = talib.ADX(high, low, timeperiod=timeperiod).shift(shift).tail(1)
    return pd.to_numeric(val.values.ravel())[0]



# Get the technical indicators for buillish reversal
def get_ti_for_bullish_reversal(df, op, hi, lo, cl, vol):
    # df['OPEN'] = df['op']
    # df['CLOSE'] = df['cl']
    # df['HIGH'] = df['hi']
    # df['LOW'] = df['lo']
    df['OPEN_PRE'] = op.shift(1)
    df['CLOSE_PRE'] = cl.shift(1)
    df['HIGH_PRE'] = hi.shift(1)
    df['LOW_PRE'] = lo.shift(1)
    df['OPEN_PRE_2'] = op.shift(2)
    df['CLOSE_PRE_2'] = cl.shift(2)
    df['HIGH_PRE_2'] = hi.shift(2)
    df['LOW_PRE_2'] = lo.shift(2)
    df["Cdl_Range"] = (hi - lo)
    df["Cdl_Range_1"] = (hi - lo).shift(1)
    df["Cdl_Range_2"] = (hi - lo).shift(2)
    df["Cdl_Range_3"] = (hi - lo).shift(3)
    df["Cdl_Range_4"] = (hi - lo).shift(4)     
    # -------------------------------testing functions----------------------
    print(adosc(hi, lo, cl, vol, fastperiod=5, slowperiod=14, shift=0))
    print(adosc(hi, lo, cl, vol, fastperiod=10, slowperiod=15, shift=1))
    # -------------------------------testing functions----------------------

    def NR4(df):
        NR4 = 0
        if (df['Cdl_Range_1'] < df['Cdl_Range_2']) and (df['Cdl_Range_1'] < df['Cdl_Range_3']) and (df['Cdl_Range_1'] < df['Cdl_Range_4']) :
            NR4 = 1
        else : 
            NR4 = 0 
        return NR4
    def INSIDE_DAY(df):
        insideDay = 0
        if (df['HIGH_PRE'] < df['HIGH_PRE_2']) and (df['LOW_PRE'] > df['LOW_PRE_2']) :
            insideDay = 1
        else : 
            insideDay = 0 
        return insideDay
    
    df['NR4'] = df.apply(NR4, axis=1)
    df['INSIDE_DAY'] = df.apply(INSIDE_DAY, axis=1)
    df['ROC1'] = talib.ROC(cl, timeperiod=1)
    df['ROC1_Pre'] = talib.ROC(cl, timeperiod=1).shift(1)   
    df['ROC1_Pre_2'] = talib.ROC(cl, timeperiod=1).shift(2)  
   
    df['STDDEV_6'] = talib.STDDEV(df['ROC1'], timeperiod=6, nbdev=1)
    df['STDDEV_100'] = talib.STDDEV(df['ROC1'], timeperiod=100, nbdev=1)
    df['STDDEV_RATIO_6of100'] = df['STDDEV_6'] / df['STDDEV_100'] * 100
    
    if (((roc(cl, period=1, shift=0)) < 5) and (sma(price=cl, period=10) > sma(price=hi, period=20))):
        print('True')
    else:
        print('false')

    df['SMA10'] = talib.SMA(cl, timeperiod=10)
    df['SMA20'] = talib.SMA(cl, timeperiod=20)
    df['SMA50'] = talib.SMA(cl, timeperiod=50)
    df['SMA100'] = talib.SMA(cl, timeperiod=100)
    df['SMA200'] = talib.SMA(cl, timeperiod=200)
    df['EMA3-CLOSE'] = talib.EMA(cl, timeperiod=3)
    df['EMA15-CLOSE'] = talib.EMA(cl, timeperiod=15)
    df['EMA3-PREV-CLOSE'] = talib.EMA(cl, timeperiod=3).shift(1)
    df['EMA15-PREV-CLOSE'] = talib.EMA(cl, timeperiod=15).shift(1)

    df['ATR_100D'] = talib.ATR(hi, lo, cl, timeperiod=100)
    df['ATR_100_Pct'] = (df['ATR_100D'] / df['SMA100']) * 100

    df['EMA5'] = talib.EMA(cl, timeperiod=5)
    df['EMA8'] = talib.EMA(cl, timeperiod=8)
    df['EMA21'] = talib.EMA(cl, timeperiod=21)
    df['EMA34'] = talib.EMA(cl, timeperiod=34)
    df['EMA55'] = talib.EMA(cl, timeperiod=55)
    df['EMA89'] = talib.EMA(cl, timeperiod=89)
    
    df['RSI_2_Pre'] = talib.RSI(cl, timeperiod=2).shift(1)
    df['RSI_2_Pre2'] = talib.RSI(cl, timeperiod=2).shift(2)
    df['RSI_2'] = talib.RSI(cl, timeperiod=2)
    
    df['RSI_2_Low'], df['RSI_2_High'] = talib.MINMAX(df['RSI_2'], timeperiod=3)
    df['RSI'] = talib.RSI(cl, timeperiod=14)
    
    df['RSI_Low'], df['RSI_High'] = talib.MINMAX(df['RSI'], timeperiod=5)
    df['52W_Low'], df['52W_High'] = talib.MINMAX(cl, timeperiod=252)
    df['52W_Range'] = df['52W_High'] - df['52W_Low']
    df['52W_Range_Pct'] =((df['52W_High'] /df['52W_Low'])-1)*100
    df['Up_52W_Low'] = ((cl /df['52W_Low'])-1)*100
    df['ADX'] = talib.ADX(hi, lo, cl, timeperiod=14)
    df['MINUS_DI'] = talib.MINUS_DI(hi, lo, cl, timeperiod=14)
    df['PLUS_DI'] = talib.PLUS_DI(hi, lo, cl, timeperiod=14)

    df['STOCH_SLOWK'], df['STOCH_SLOWD'] = talib.STOCH(
        hi, lo, cl, fastk_period=7, slowk_period=10, slowk_matype=0, slowd_period=9, slowd_matype=0)
    df['STOCH_SLOWD_Low'], df['STOCH_SLOWD_High'] = talib.MINMAX(df['STOCH_SLOWD'], timeperiod=5)
    
    df['5D_Low'], df['5D_High'] = talib.MINMAX(cl, timeperiod=5)
    df['Down_52W_high'] = ((df['5D_Low'] /df['52W_High'])-1)*100

    df['FIBO_Level_23']  = df['52W_High'] - (df['52W_Range'] * 0.236)   
    df['FIBO_Level_38']  = df['52W_High'] - (df['52W_Range'] * 0.382)  
    df['FIBO_Level_50']  = df['52W_High'] - (df['52W_Range'] * 0.5)     
    df['FIBO_Level_62']  = df['52W_High'] - (df['52W_Range'] * 0.618)
    df['FIBO_Ratio']  = (df['52W_High'] - df['5D_Low'] ) / (df['52W_High'] - df['52W_Range']) * 100
    

    df['VOLUME_BBANDS_UPPER'], df['VOLUME_BBANDS_MIDDLE'], df['VOLUME_BBANDS_LOWER'] = talib.BBANDS(
        vol, timeperiod=50, nbdevup=2, nbdevdn=2, matype=0)
    df['VOLUME_Low5'], df['VOLUME_High5'] = talib.MINMAX(vol, timeperiod=5) 
    
   
    df['TRANGE'] = talib.TRANGE(hi, lo, cl)
    df['TRANGE_Low5'], df['TRANGE_High5'] = talib.MINMAX(df['TRANGE'], timeperiod=5) 
    df['TRANGE_HIGH_PCT'] = (df['TRANGE_High5'] / cl)*100
    df['TRANGE_LOW_PCT'] = (df['TRANGE_Low5'] / cl)*100
 

    df['AVGPRICE'] = talib.AVGPRICE (op, hi, lo, cl)
    df["R1"] = ( (df['AVGPRICE'] * 2) - lo)
    df["R2"] = (df['AVGPRICE'] + (hi - lo) )
    df["R3"] = (hi + (2 * (df['AVGPRICE'] - lo) ) )
    df["S1"] = ( (df['AVGPRICE'] * 2) - hi)
    df["S2"] = (df['AVGPRICE'] - (hi - lo) )
    df["S3"] = (lo - (2 * (hi - df['AVGPRICE']) ) )
    
    # df['HT_DCPERIOD'] = talib.HT_DCPERIOD(cl)
    # df['HT_DCPHASE'] = talib.HT_DCPHASE(cl)
    # df['HT_Inphase'], df['HT_Quadrature'] = talib.HT_PHASOR(cl)
    df['HT_sine'], df['HT_Leadsine'] = talib.HT_SINE(cl)
    df['HT_TRENDMODE'] = talib.HT_TRENDMODE(cl)
    df['HT_sine_Low5'], df['HT_sine_High5'] = talib.MINMAX(df['HT_sine'], timeperiod=5) 

    df['BBANDS_UPPER'], df['BBANDS_MIDDLE'], df['BBANDS_LOWER'] = talib.BBANDS(cl, timeperiod=10, nbdevup=1, nbdevdn=1, matype=0) 
    #Keltner Channel
    df['TYPPRICE'] = talib.TYPPRICE(hi, lo, cl)
    df['ATR'] = talib.ATR(hi, lo, cl, timeperiod=10)

    df['EMA20_TYPPRICE'] = talib.EMA(df['TYPPRICE'], timeperiod=10)
    df['KC_UpperLine'] = df['EMA20_TYPPRICE'] + (1*df['ATR'])
    df['KC_LowerLine'] = df['EMA20_TYPPRICE'] - (1*df['ATR'])
    df['buyExitPrice'] = df['EMA20_TYPPRICE'] + (3*df['ATR'])
    df['sellExitPrice'] = df['EMA20_TYPPRICE'] - (3*df['ATR'])
    
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

    df['2D_HIGH_PRE'] = talib.MAX(hi, timeperiod=2).shift(1)
    df['2D_LOW_PRE'] = talib.MIN(lo, timeperiod=2).shift(1)
    df['5D_HIGH_PRE'] = talib.MAX(hi, timeperiod=5).shift(1)
    df['5D_LOW_PRE'] = talib.MIN(lo, timeperiod=5).shift(1)
    df['20D_HIGH_PRE'] = talib.MAX(hi, timeperiod=20).shift(1)
    df['20D_LOW_PRE'] = talib.MIN(lo, timeperiod=20).shift(1)
    df['20D_HIGH_PRE_5D'] = talib.MAX(hi, timeperiod=20).shift(5)
    df['20D_LOW_PRE_5D'] = talib.MIN(lo, timeperiod=20).shift(5)

    df['DONCHIAN_ML'] = (df['20D_HIGH_PRE'] + df['20D_LOW_PRE'])/2
    df['TTM_MOM'] = (cl - ((df['SMA20'] + df['DONCHIAN_ML'])/2))
    df['TTM_MOM_REG'] = talib.LINEARREG(df['TTM_MOM'], timeperiod=9)
    df['EMA_Average'] = (df['EMA8'] + df['EMA21'] + df['EMA34'] + df['EMA55'])/4
    
    def ema_in_Squeeze(df):
        EmaSqueeze = 0
        if ((max(df['EMA8'], df['EMA21'], df['EMA34'], df['EMA55']) - min(df['EMA8'], df['EMA21'] , df['EMA34'], df['EMA55'])) < (df['ATR'] * 2)):
            EmaSqueeze = 1
        else : 
            EmaSqueeze = 0 
        return EmaSqueeze
    df['EMASqueeze'] = df.apply(ema_in_Squeeze, axis=1)
    df['EMASLength'] = talib.SUM(abs(df['EMASqueeze']), timeperiod=4)

    df['Min_Buy_Price_level'] = df['EMA_Average'] - (1*df['ATR'])
    df['Max_Buy_Price_level'] = df['EMA_Average'] + (2*df['ATR'])
    df['Min_Sell_Price_level'] = df['EMA_Average'] + (1*df['ATR'])
    df['Max_Sell_Price_level'] = df['EMA_Average'] - (2*df['ATR'])
    df["Cdl_Bullish"] = ((cl - lo) / (hi - lo))
    df["Cdl_Bearish"] = ((hi - cl) / (hi - lo))
    df["Cdl_Range"] = (hi - lo)   
    df["OPEN_RANGE_PRE"] = ((hi - op) / (hi - lo)).shift(1)
    df["CLOSE_RANGE_PRE"] = ((cl - lo) / (hi - lo)).shift(1)    
    return df

# Return score is 20 maximum
def get_preceding_trend_score(lastRec, interval):
    
    range52WPctVal = pd.to_numeric(lastRec['52W_Range_Pct'].head(1).values.ravel())        
    downFrom52WHighVal = pd.to_numeric(lastRec['Down_52W_high'].head(1).values.ravel())
    
    score = 0
    commentsHtml_1 = ""
    if range52WPctVal > 0:
        commentsHtml_1 = str("""
        <tr>
            <td>The stock's 52 week range in percentage is </td>
            <td>%s</td>
        </tr>""" %str(range52WPctVal))
        commentsHtml_1 = commentsHtml_1 
        
        commentsHtml_1 = commentsHtml_1 + str("""
        <tr>
            <td>The stock is down from its 52 week high </td>
            <td>%s</td>
        </tr>""" %str(downFrom52WHighVal))

    if range52WPctVal > 100 :
        score = 10
    elif range52WPctVal > 70 :
        score = 5


    if (downFrom52WHighVal < -20 and downFrom52WHighVal > -30 ) : # The value should come from average drawdown on the stock
        score = score + 10

    elif (downFrom52WHighVal < -15 and downFrom52WHighVal > -20 )  : # The value should come from average drawdown on the stock
        score = score + 5

    return score, commentsHtml_1

def get_sma_score(ID, lastRec):
    IDVal = pd.to_numeric(lastRec[ID].head(1).values.ravel())
    price = pd.to_numeric(lastRec['close'].head(1).values.ravel())
    score = 0
    commentsHtml = ""
    if (ID == "SMA200" or ID == "SMA10"):
        if (price > IDVal):
            score = 10
            commentsHtml = str("""
                <tr>
                    <td>The current market price is above</td>
                    <td>%s</td>
                </tr>""" %str(ID))
            
    elif (ID == "SMA50"):        
        if (price < IDVal):
            score = 10                
            commentsHtml = str("""
                <tr>
                    <td>The current market price is below</td>
                    <td>%s</td>
                </tr>""" %str(ID))


    return score, commentsHtml

# Return score is 30 maximum
def get_trend_level_score(lastRec):
    commentsHtml1 = ""
    commentsHtml2 = ""
    commentsHtml3 = ""
    smaScore1 = 0 
    smaScore2 = 0
    smaScore3 = 0

    smaScore1, commentsHtml1 = get_sma_score('SMA200', lastRec)    
    smaScore2, commentsHtml2 = get_sma_score('SMA50', lastRec)
    smaScore3, commentsHtml3 = get_sma_score('SMA10', lastRec)

    commentsHtml  = commentsHtml1 + commentsHtml2 + commentsHtml3
    smaScore = smaScore1 + smaScore2 + smaScore3
    
    return smaScore, commentsHtml

# Return score is 20 maximum
def get_over_extension_score(lastRec):
    
    rsiLowVal = pd.to_numeric(lastRec['RSI_Low'].head(1).values.ravel())        
    rsiVal = pd.to_numeric(lastRec['RSI'].head(1).values.ravel())
    
    stochLowVal = pd.to_numeric(lastRec['STOCH_SLOWD_Low'].head(1).values.ravel())        
    stochVal = pd.to_numeric(lastRec['STOCH_SLOWD'].head(1).values.ravel())

    score = 0
    commentsHtml_2 = ""
    if rsiLowVal > 0:
        commentsHtml_2 = str("""
        <tr>
            <td>The stock's recent low RSI is </td>
            <td>%s</td>
        </tr>""" %str(rsiLowVal))
        commentsHtml_2 = commentsHtml_2 
        commentsHtml_2 = commentsHtml_2  + str("""
        <tr>
            <td>The stock's current RSI is </td>
            <td>%s</td>
        </tr>""" %str(rsiVal))
        commentsHtml_2 = commentsHtml_2 

    if (rsiLowVal < 35 and rsiVal > 35 ):
        score = 10
    elif (rsiLowVal < 45 and rsiVal > 45 ):
        score = 5
    
    if (stochLowVal < 20 and stochVal > 20 ):
        score = score + 10

    return score, commentsHtml_2
# Return score is 20 maximum
def get_over_extension_sell_score(lastRec):
    
    rsiHighVal = pd.to_numeric(lastRec['RSI_High'].head(1).values.ravel())        
    rsiVal = pd.to_numeric(lastRec['RSI'].head(1).values.ravel())
    
    stochHighVal = pd.to_numeric(lastRec['STOCH_SLOWD_High'].head(1).values.ravel())        
    stochVal = pd.to_numeric(lastRec['STOCH_SLOWD'].head(1).values.ravel())

    score = 0
    if (rsiHighVal > 70 and rsiVal < 70 ):
        score = 10
   
    if (stochHighVal > 80 and stochVal < 80 ):
        score = score + 10

    return score

# Return score is 20 maximum
def get_retracement_level_score(lastRec):

    fiveDLowVal = pd.to_numeric(lastRec['5D_Low'].head(1).values.ravel())        
    fiboLevel38Val = pd.to_numeric(lastRec['FIBO_Level_38'].head(1).values.ravel())
    
    fiboLevel50Val = pd.to_numeric(lastRec['FIBO_Level_50'].head(1).values.ravel())        
    fiboLevel62Val = pd.to_numeric(lastRec['FIBO_Level_62'].head(1).values.ravel())    
    fiboLevel = pd.to_numeric(lastRec['FIBO_Ratio'].head(1).values.ravel()) 

    score = 0
    commentsHtml_3 = ""
    if fiboLevel > 0:
        commentsHtml_3 = str("""
        <tr>
            <td>The stock's fibbonaci level </td>
            <td>%s</td>
        </tr>""" %str(fiboLevel))
        commentsHtml_3 = commentsHtml_3 
    if ((fiveDLowVal < fiboLevel38Val) and (fiveDLowVal > fiboLevel50Val) ):
        score = 10
    elif ((fiveDLowVal < fiboLevel50Val) and (fiveDLowVal > fiboLevel62Val) ):
        score = 20
   
    return score , commentsHtml_3 

# Return score is 20 maximum
def get_volume_openinterst_score(lastRec):

    volHigh5Val = pd.to_numeric(lastRec['VOLUME_High5'].head(1).values.ravel())        
    volBbandsUpperVal = pd.to_numeric(lastRec['VOLUME_BBANDS_UPPER'].head(1).values.ravel())
    volLow5Val = pd.to_numeric(lastRec['VOLUME_Low5'].head(1).values.ravel())        
    volBbandsMiddleVal = pd.to_numeric(lastRec['VOLUME_BBANDS_MIDDLE'].head(1).values.ravel())    
    
    score = 0    
    if (volHigh5Val > volBbandsUpperVal * 0.75 ):
        score = 10
    if (volLow5Val < volBbandsMiddleVal* 0.75 ):
        score = score + 10
    return score

 # Return score is 20 maximum
def get_support_score(lastDf1Rec):
    frequency = pd.to_numeric(lastDf1Rec['frequency'].head(1).values.ravel())        
    score = 0    
    if ( (frequency[0] > 0)):
        score = min((frequency[0]*5),20)
    return score

# Return score is 20 maximum
def get_bullish_divergence_score(lastDf1Rec):
    priceSlope = pd.to_numeric(lastDf1Rec['linear_slope_price'].head(1).values.ravel())   
    RSISlope = pd.to_numeric(lastDf1Rec['linear_slope_rsi'].head(1).values.ravel())
    recentReversalPrice = pd.to_numeric(lastDf1Rec['recent_price_level'].head(1).values.ravel())
    lastPrice = pd.to_numeric(lastDf1Rec['last_price'].head(1).values.ravel())       
    score = 0 
    if (recentReversalPrice < lastPrice):   
        if ( RSISlope > 50 and abs(priceSlope) <= 10): #  RSI sloped up and Price in sideways
            score = 20
        elif (abs(RSISlope) <= 10 and priceSlope < -50): # RSI in sideways and price sloped down
            score = 20
        elif ( RSISlope > 50 and priceSlope < -50 ): # RSI in Sloped up and price sloped down
            score = 20
    
    return score

# Return score is 10 maximum
def get_selling_climax_score(lastRec):
    trangeHighPctVal = pd.to_numeric(lastRec['TRANGE_HIGH_PCT'].head(1).values.ravel())        
    trangeHigh5Val = pd.to_numeric(lastRec['TRANGE_High5'].head(1).values.ravel())
    trangeLowPctVal = pd.to_numeric(lastRec['TRANGE_LOW_PCT'].head(1).values.ravel())        
    trangeLow5Val = pd.to_numeric(lastRec['TRANGE_Low5'].head(1).values.ravel())
  
    atrVal = pd.to_numeric(lastRec['ATR'].head(1).values.ravel())
    
    score = 0    
    if ( (trangeHighPctVal > 5) and (trangeHigh5Val > 1.5 * atrVal) ):
        score = 10
    if ( (trangeLowPctVal < 2) and (trangeLow5Val < atrVal * 0.5) ):
        score = score + 10
    return score

    # Return score is 20 maximum
def get_ht_cycle_score(lastRec):
    htTrendModeVal = pd.to_numeric(lastRec['HT_TRENDMODE'].head(1).values.ravel())        
    htLeadSineVal = pd.to_numeric(lastRec['HT_Leadsine'].head(1).values.ravel())
    htSineVal = pd.to_numeric(lastRec['HT_sine'].head(1).values.ravel())        
    htSineLow5Val = pd.to_numeric(lastRec['HT_sine_Low5'].head(1).values.ravel())
    
    score = 0    
    if (htTrendModeVal == 0) and (htSineLow5Val <= -0.85):
        score = 10
    if ( (htLeadSineVal > htSineVal) and (htLeadSineVal >= -0.85) ):
        score = score + 10
    return score

    # Return score is 20 maximum
def get_stacked_EMA_score(lastRec):
    price = pd.to_numeric(lastRec['close'].head(1).values.ravel())
    ema8Val = pd.to_numeric(lastRec['EMA8'].head(1).values.ravel())        
    ema21Val = pd.to_numeric(lastRec['EMA21'].head(1).values.ravel())
    ema34Val = pd.to_numeric(lastRec['EMA34'].head(1).values.ravel())        
    ema55Val = pd.to_numeric(lastRec['EMA55'].head(1).values.ravel())
    ema89Val = pd.to_numeric(lastRec['EMA89'].head(1).values.ravel())
    minBuyPriceLevel = pd.to_numeric(lastRec['Min_Buy_Price_level'].head(1).values.ravel())
    maxBuyPriceLevel = pd.to_numeric(lastRec['Max_Buy_Price_level'].head(1).values.ravel())
    buyExitPrice = pd.to_numeric(lastRec['buyExitPrice'].head(1).values.ravel())
    EMASLength = pd.to_numeric(lastRec['EMASLength'].head(1).values.ravel())
    bullishCandlePct = pd.to_numeric(lastRec['Cdl_Bullish'].head(1).values.ravel())
    buyCandleRange = pd.to_numeric(lastRec['Cdl_Range'].head(1).values.ravel())
    buyATRVal = pd.to_numeric(lastRec['ATR'].head(1).values.ravel())
    ROC1Pre = pd.to_numeric(lastRec['ROC1_Pre'].head(1).values.ravel())

    score = 0 
    belowBuyPrice = 0 
    ATRbasedBuyExit = 0 
    
    if (price > ema8Val) and (ema8Val > ema21Val):
        score = 5
    if (ema21Val > ema34Val):
        score = score + 5
    if (ema34Val > ema55Val):
        score = score + 5
    if (ema55Val > ema89Val):
        score = score + 5
    if ((price > minBuyPriceLevel) and (price < maxBuyPriceLevel) and (bullishCandlePct >= 0.80) and (buyCandleRange > (0.5 * buyATRVal)) and (ROC1Pre > 0.25)):
        belowBuyPrice = 1
    if (price > buyExitPrice):
        ATRbasedBuyExit = 1

    return score, belowBuyPrice, ATRbasedBuyExit, EMASLength[0], bullishCandlePct[0], buyCandleRange, buyATRVal



def get_stacked_EMA_Sell_score(lastRec):
    price = pd.to_numeric(lastRec['close'].head(1).values.ravel())
    ema8Val = pd.to_numeric(lastRec['EMA8'].head(1).values.ravel())        
    ema21Val = pd.to_numeric(lastRec['EMA21'].head(1).values.ravel())
    ema34Val = pd.to_numeric(lastRec['EMA34'].head(1).values.ravel())        
    ema55Val = pd.to_numeric(lastRec['EMA55'].head(1).values.ravel())
    ema89Val = pd.to_numeric(lastRec['EMA89'].head(1).values.ravel())
    minSellPriceLevel = pd.to_numeric(lastRec['Min_Sell_Price_level'].head(1).values.ravel())
    maxSellPriceLevel = pd.to_numeric(lastRec['Max_Sell_Price_level'].head(1).values.ravel())
    sellExitPrice = pd.to_numeric(lastRec['sellExitPrice'].head(1).values.ravel())
    bearishCandlePct = pd.to_numeric(lastRec['Cdl_Bearish'].head(1).values.ravel())
    sellCandleRange = pd.to_numeric(lastRec['Cdl_Range'].head(1).values.ravel())
    sellATRVal = pd.to_numeric(lastRec['ATR'].head(1).values.ravel())
    ROC1Pre = pd.to_numeric(lastRec['ROC1_Pre'].head(1).values.ravel())
    
    score = 0   
    aboveSellPrice = 0
    ATRbasedSellExit = 0
    if (price < ema8Val) and (ema8Val < ema21Val):
        score = 5
    if (ema21Val < ema34Val):
        score = score + 5
    if (ema34Val < ema55Val):
        score = score + 5
    if (ema55Val < ema89Val):
        score = score + 5

    if ((price < minSellPriceLevel) and (price > maxSellPriceLevel) and (bearishCandlePct >= 0.80) and (sellCandleRange > (0.5 * sellATRVal)) and (ROC1Pre < -0.25)):
        aboveSellPrice = 1

    if (price < sellExitPrice):
        ATRbasedSellExit = 1

    return score, aboveSellPrice , ATRbasedSellExit, bearishCandlePct[0], sellCandleRange, sellATRVal

 # Return score is 20 maximum
def get_TTMS_score(lastRec):
    TTMSLengthVal = pd.to_numeric(lastRec['TTMSLength'].head(1).values.ravel())        
    CurrentTTMS = pd.to_numeric(lastRec['TTMSqueeze'].head(1).values.ravel())
    PrevTTMS = pd.to_numeric(lastRec['TTMSqueeze1'].head(1).values.ravel())        
    TTMSMomendum = pd.to_numeric(lastRec['TTM_MOM_REG'].head(1).values.ravel())

    score = 0    
    if (TTMSLengthVal >= 5):
        score = TTMSLengthVal[0]
    if (CurrentTTMS == 0) and (PrevTTMS == 1) :
        score = score + 5
    if (TTMSMomendum > 0):
        score = score + 5
    return score, TTMSLengthVal[0]

def get_buy_sell_signal(lastRec):
    TTMSLengthVal = pd.to_numeric(lastRec['TTMSLength'].head(1).values.ravel())            
    ema8Val = pd.to_numeric(lastRec['EMA8'].head(1).values.ravel())        
    ema21Val = pd.to_numeric(lastRec['EMA21'].head(1).values.ravel())
    ATRVal = pd.to_numeric(lastRec['ATR'].head(1).values.ravel())
    sma200 = pd.to_numeric(lastRec['SMA200'].head(1).values.ravel())
    price = pd.to_numeric(lastRec['close'].head(1).values.ravel())
    rsiHighVal = pd.to_numeric(lastRec['RSI_High'].head(1).values.ravel()) 

    buySellSignal = ""    
    if (TTMSLengthVal >= 8) and (price > (ema8Val + ATRVal)) and (price > (ema21Val + ATRVal)) and rsiHighVal < 60:
        buySellSignal = "Buy"
    elif (TTMSLengthVal >= 8) and (price < (ema8Val - ATRVal)) and (price < (ema21Val - ATRVal)) and rsiHighVal > 40:
        buySellSignal = "Sell"
    return buySellSignal 


def get_RSI2_buy_sell_signal(lastRec):
    rsi2 = pd.to_numeric(lastRec['RSI_2'].head(1).values.ravel())  
    roc1 = pd.to_numeric(lastRec['ROC1'].head(1).values.ravel())        
    roc1Pre = pd.to_numeric(lastRec['ROC1_Pre'].head(1).values.ravel())
    sma200 = pd.to_numeric(lastRec['SMA200'].head(1).values.ravel())
    price = pd.to_numeric(lastRec['close'].head(1).values.ravel())

    RSISellSignal = ""    
    if ((rsi2 >= 90) and (price < sma200) and (roc1 > 3) and (price > 50 and price < 5000)):
        RSISellSignal = "Sell"
    elif ((rsi2 <= 10) and (price > sma200) and (roc1  < -3) and (price > 50 and price < 5000)):
        RSISellSignal = "Buy"

    return RSISellSignal 
# Failed Breakout Entries:

def get_cdl_reversal_score(lastRec, last1Rec, last2Rec, last3Rec, last4Rec):


    def get_cdl_score(record, commentsHtml):
        tmpScore = 0
        
        # Iterate over the sequence of column names
        for columnName in record:
            # Select column contents by column name using [] operator
            columnVal = record[columnName].values.ravel()
            cdlDate = record['date'].values.ravel()

            if(str(columnName).startswith('CDL', 0, 3)):
                if (columnVal == 100):
                    tmpScore = tmpScore + 5
                    commentsHtml = commentsHtml + str(f"""
                        <tr>
                            <td>{columnName}</td>
                            <td>{str(cdlDate)}</td>
                        </tr>""")
                                    

                  
        return tmpScore, commentsHtml

    maxScore = 0
    commentsHtml = ""
    lastRecScore, commentsHtml = get_cdl_score(lastRec.head(1), commentsHtml)
    last1RecScore, commentsHtml = get_cdl_score(last1Rec.head(1), commentsHtml)
    last2RecScore, commentsHtml = get_cdl_score(last2Rec.head(1), commentsHtml)
    last3RecScore, commentsHtml = get_cdl_score(last3Rec.head(1), commentsHtml)
    last4RecScore, commentsHtml = get_cdl_score(last4Rec.head(1), commentsHtml)
    scoreList = [lastRecScore, last1RecScore, last2RecScore, last3RecScore, last4RecScore]
    maxScore = max(scoreList) 
    totalScore =  sum(scoreList)

    return maxScore , totalScore, commentsHtml

def count_df_occurances(list1, l, r):
	c = 0
	# traverse in the list1
	for x in list1:
		# condition check
		if x > float(l) and x < float(r):
			c+= 1	
	return c

def get_support_analysis(extrema, prices):

    df1 = extrema.to_frame()
    df2 = prices.to_frame()
    df2.set_index('close', inplace=False)

    df1['price_level'] = df1['close']
    df1['lower_band']  =  df1['close'] * 0.98
    df1['upper_band']  =  df1['close'] * 1.02
    df1["rank"] = df1["price_level"].rank()
    df1['RSI'] = talib.RSI(df2['close'], timeperiod=14)
    
    df1 = df1.reset_index()

    for index, row in df1.iterrows():               
        df1.loc[index, 'frequency'] = count_df_occurances(df1['price_level'], row['lower_band'],  row['upper_band'])-1
        df1.loc[index, 'recent_price_level'] = df1['price_level'][df1.index[-1]]
        df1.loc[index, 'current_rank'] = df1['rank'][df1.index[-1]]
        df1.loc[index, 'last_price'] = df2['close'][df2.index[-1]]
        
    
    df1["distance"]= (df1['price_level']/ df1['price_level'][df1.index[-1]]-1)*100
    df1['avg_distance_res'] = (df1[(df1['rank']> df1['current_rank']) & (df1['rank'] <= (df1['current_rank']+5))])['distance'].mean()
    df1['max_distance_res'] = (df1[(df1['rank']> df1['current_rank'])])['distance'].max()
    df1['avg_distance_sub'] = (df1[(df1['rank']< df1['current_rank']) & (df1['rank'] >= (df1['current_rank']-5))])['distance'].mean()
    df1['max_distance_sub'] = (df1[(df1['rank']< df1['current_rank'])])['distance'].min()
    df1['linear_slope_price'] = talib.LINEARREG_SLOPE(df1['price_level'], timeperiod=3)
    df1['linear_slope_rsi'] = talib.LINEARREG_SLOPE(df1['RSI'], timeperiod=3)    
    df1['target_pct'] = (df1['avg_distance_res'] + df1['max_distance_res']) /2

    return df1




def get_bullish_reversal_score(df, extrema, prices, interval, stock_name):
    reversalScoreDict = {}
       # Checked
    if (df.shape[0] > 260):
        op = df['open']
        hi = df['high']
        lo = df['low']
        cl = df['close']
        vol = df['volume']


        df = get_csp_for_bullish_reversal(df, op, hi, lo, cl, vol)
        df = get_ti_for_bullish_reversal(df, op, hi, lo, cl, vol)
        df1 = get_support_analysis(extrema, prices)

        df['interval'] = interval
        df['stock_name'] = stock_name
        # df.to_csv("TIScore.csv", index=False, mode='a', header=True)

        lastRec = df.tail(1)
        last1Rec = df.tail(2)
        last2Rec = df.tail(3)
        last3Rec = df.tail(4)
        last4Rec = df.tail(5)
        lastDf1Rec = df1.tail(1)
     
        
        commentsHtml = ""
        precedingtrendscore, commentsHtml_1 = get_preceding_trend_score(lastRec, interval)
        trendLevelScore, commentsHtml = get_trend_level_score(lastRec)
        overExtensionScore, commentsHtml_2 = get_over_extension_score(lastRec)
        overExtensionSellScore = get_over_extension_sell_score(lastRec) 
    
        retracementLevelScore, commentsHtml_3  = get_retracement_level_score(lastRec)
        volumeOpenInterstScore = get_volume_openinterst_score(lastRec)
        sellingClimaxScore = get_selling_climax_score(lastRec)
        htCycleScore = get_ht_cycle_score(lastRec)
        stackedEMAScore, belowBuyPrice, ATRbasedBuyExit, EMASLength, bullishCandlePct, buyCandleRange, buyATRVal = get_stacked_EMA_score(lastRec)
        stackedEMASellScore, aboveSellPrice, ATRbasedSellExit, bearishCandlePct, sellCandleRange, sellATRVal = get_stacked_EMA_Sell_score(lastRec)
        TTMSscore, TTMSLengthVal = get_TTMS_score(lastRec)
        supportScore = get_support_score(lastDf1Rec)
        
        rsiBuySellSignal = get_RSI2_buy_sell_signal(lastRec)        
        
        roc1 = pd.to_numeric(lastRec['ROC1'].head(1).values.ravel())


        bullishDivergenceScore = get_bullish_divergence_score(lastDf1Rec)
        cdlMaxScore , cdlTotalScore, cdlComments = get_cdl_reversal_score(lastRec, last1Rec, last2Rec, last3Rec, last4Rec )
        buySellSignal = get_buy_sell_signal(lastRec)

        commentsHtml = commentsHtml_1 + commentsHtml_2 + commentsHtml_3 + commentsHtml + cdlComments
        
        netScore = precedingtrendscore + trendLevelScore + overExtensionScore + retracementLevelScore + \
                    volumeOpenInterstScore + sellingClimaxScore + htCycleScore + stackedEMAScore + TTMSscore + supportScore + bullishDivergenceScore + min(max(cdlMaxScore,cdlTotalScore),20) 
        netScorePct = (netScore / 230) * 100
        
        if (interval == 'day'):
            netScorePct = (netScore / 260) * 100
        
        reversalScoreDict = {}
        reversalScoreDict['netScorePct'] = netScorePct
        reversalScoreDict['netScore'] = netScore
        reversalScoreDict['precedingtrendscore'] =precedingtrendscore
        reversalScoreDict['trendLevelScore'] = trendLevelScore
        reversalScoreDict['overExtensionScore'] = overExtensionScore
        reversalScoreDict['overExtensionSellScore'] = overExtensionSellScore
        
        reversalScoreDict['retracementLevelScore'] = retracementLevelScore
        reversalScoreDict['volumeOpenInterstScore'] = volumeOpenInterstScore
        reversalScoreDict['sellingClimaxScore'] = sellingClimaxScore
        reversalScoreDict['htCycleScore'] = htCycleScore
        reversalScoreDict['cdlMaxScore'] = cdlMaxScore
        reversalScoreDict['cdlTotalScore'] = cdlTotalScore
        reversalScoreDict['stackedEMAScore'] = stackedEMAScore
        reversalScoreDict['commentsHtml'] = commentsHtml
        reversalScoreDict['TTMSscore'] = TTMSscore
        reversalScoreDict['supportScore'] = supportScore
        reversalScoreDict['bullishDivergenceScore'] = bullishDivergenceScore
        reversalScoreDict['TTMSLengthVal'] = TTMSLengthVal
        reversalScoreDict['stackedEMASellScore'] = stackedEMASellScore
        reversalScoreDict['belowBuyPrice'] = belowBuyPrice 
        reversalScoreDict['aboveSellPrice'] = aboveSellPrice 
        reversalScoreDict['ATRbasedBuyExit'] = ATRbasedBuyExit 
        reversalScoreDict['ATRbasedSellExit'] = ATRbasedSellExit 
        reversalScoreDict['EMASLength'] = EMASLength 
        reversalScoreDict['bullishCandlePct'] = bullishCandlePct 
        reversalScoreDict['bearishCandlePct'] = bearishCandlePct 
        reversalScoreDict['bearishCandlePct'] = bearishCandlePct 
        reversalScoreDict['sellCandleRange'] = sellCandleRange 
        reversalScoreDict['sellATRVal'] = sellATRVal 
        reversalScoreDict['buyCandleRange'] = buyCandleRange 
        reversalScoreDict['buyATRVal'] = buyATRVal 
        reversalScoreDict['target_pct']= round(lastDf1Rec['target_pct'].values.ravel()[0], 2)
        reversalScoreDict['buySellSignal'] = buySellSignal  
        reversalScoreDict['rsiBuySellSignal'] = rsiBuySellSignal  
        reversalScoreDict['ROC'] = pd.to_numeric(lastRec['ROC1'].values.ravel())      
        reversalScoreDict['RSI_2'] = pd.to_numeric(lastRec['RSI_2'].values.ravel())
        reversalScoreDict['ROC_1'] = pd.to_numeric(lastRec['ROC1'].values.ravel())
        reversalScoreDict['SMA200'] = pd.to_numeric(lastRec['SMA200'].values.ravel())
        reversalScoreDict['CLOSE'] = pd.to_numeric(lastRec['close'].values.ravel())
        # For Siva
        reversalScoreDict['EMA3-CLOSE'] = pd.to_numeric(lastRec['EMA3-CLOSE'].values.ravel())[0]
        reversalScoreDict['EMA15-CLOSE'] = pd.to_numeric(lastRec['EMA15-CLOSE'].values.ravel())[0]
        reversalScoreDict['EMA3-PREV-CLOSE'] = pd.to_numeric(lastRec['EMA3-PREV-CLOSE'].values.ravel())[0]
        reversalScoreDict['EMA15-PREV-CLOSE'] = pd.to_numeric(lastRec['EMA15-PREV-CLOSE'].values.ravel())[0]
     

        

        return reversalScoreDict

    else:
        return reversalScoreDict
