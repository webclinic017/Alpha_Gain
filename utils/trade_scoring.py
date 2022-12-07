import pandas as pd
import datetime
import time
import mysql.connector
import talib
import numpy as np


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
