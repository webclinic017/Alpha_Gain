import matplotlib
import matplotlib.pyplot as plt
import quantstats as qs

# extend pandas functionality with metrics, etc.
qs.extend_pandas()

# fetch the daily returns for a stock
stock = qs.utils.download_returns('FB')

# show sharpe ratio
avg_return = qs.stats.avg_return(stock)
print("avg_return : " + str(avg_return))

avg_win = qs.stats.avg_win(stock)
print("avg_win : " + str(avg_win))

avg_loss= qs.stats.avg_loss(stock)
print("avg_loss : " + str(avg_loss))

cagr = qs.stats.cagr(stock)
print("cagr : " + str(cagr))

expected_return = qs.stats.expected_return(stock)
print("expected_return : " + str(expected_return))

expected_shortfall = qs.stats.expected_shortfall(stock)
print("expected_shortfall : " + str(expected_shortfall))

# consecutive_losses = qs.stats.consecutive_losses(stock)
# print("consecutive_losses : " + str(consecutive_losses))

# consecutive_wins = qs.stats.consecutive_wins(stock)
# print("consecutive_wins : " + str(consecutive_wins))

gain_to_pain_ratio = qs.stats.gain_to_pain_ratio(stock)
print("gain_to_pain_ratio : " + str(gain_to_pain_ratio))

profit_factor = qs.stats.profit_factor(stock)
print("profit_factor : " + str(profit_factor))

risk_of_ruin = qs.stats.risk_of_ruin(stock)
print("risk_of_ruin : " + str(risk_of_ruin))

win_rate = qs.stats.win_rate(stock)
print("win_rate : " + str(win_rate))

sharpeRatio = qs.stats.sharpe(stock)
print("sharpeRatio : " + str(sharpeRatio))

sortino = qs.stats.sortino(stock)
print("sortino : " + str(sortino))

calmar = qs.stats.calmar(stock)
print("calmar : " + str(calmar))

# information_ratio = qs.stats.information_ratio(stock)
# print("information_ratio : " + str(information_ratio))

kurtosis = qs.stats.kurtosis(stock)
print("kurtosis : " + str(kurtosis))

skew = qs.stats.skew(stock)
print("skew : " + str(skew))

value_at_risk = qs.stats.value_at_risk(stock)
print("value_at_risk : " + str(value_at_risk))

conditional_value_at_risk = qs.stats.conditional_value_at_risk(stock)
print("conditional_value_at_risk : " + str(conditional_value_at_risk))

volatility = qs.stats.volatility(stock)
print("volatility : " + str(volatility))

outlier_loss_ratio = qs.stats.outlier_loss_ratio(stock)
print("outlier_loss_ratio : " + str(outlier_loss_ratio))

outlier_win_ratio = qs.stats.outlier_win_ratio(stock)
print("outlier_win_ratio : " + str(outlier_win_ratio))

max_drawdown = qs.stats.max_drawdown(stock)
print("max_drawdown : " + str(max_drawdown))

risk_return_ratio = qs.stats.risk_return_ratio(stock)
print("risk_return_ratio : " + str(risk_return_ratio))

worst = qs.stats.worst(stock)
print("worst : " + str(worst))

best = qs.stats.best(stock)
print("best : " + str(best))

kelly_criterion = qs.stats.kelly_criterion(stock)
print("kelly_criterion : " + str(kelly_criterion))

win_loss_ratio = qs.stats.win_loss_ratio(stock)
print("win_loss_ratio : " + str(win_loss_ratio))

ulcer_index = qs.stats.ulcer_index(stock)
print("ulcer_index : " + str(ulcer_index))

ulcer_performance_index = qs.stats.ulcer_performance_index(stock)
print("ulcer_performance_index : " + str(ulcer_performance_index))

runID = 0
sysTime = kcf.getCurrDateTime()
stockName = ""
strategy = ""
startDate = ""
endDate = ""
duration = ""
equityStartValue = 0
EquityEndValue = 0





def addPerformanceData(cnx):    
    """ This function is used for adding events to QUESTDB table BT_PERFORMANCE_SUMMARY"""
    try:
        insertVal = []
       
        insertQuery = "INSERT INTO BT_PERFORMANCE_SUMMARY (RUN_ID, SYS_TIME, STOCK_NAME, STRATEGY, START_DATE, END_DATE, DURATION,\
        EQUITY_START_VALUE, EQUITY_END_VALUE, AVG_RETURN, AVG_WIN, AVG_LOSS, CAGR, EXPECTED_RETURN, EXPECTED_SHORTFALL, CONSECUTIVE_LOSSES,\
        CONSECUTIVE_WINS, GAIN_TO_PAIN_RATIO, PROFIT_FACTOR, RISK_OF_RUIN, WIN_RATE, SHARPE_RATIO, SORTINO_RATIO, CALMAR_RATIO, INFORMATION_RATIO,\
        KURTOSIS, SKEW, VALUE_AT_RISK, CONDITIONAL_VALUE_AT_RISK, VOLATILITY, OUTLIER_LOSS_RATIO, OUTLIER_WIN_RATIO, MAX_DRAWDOWN,\
        RISK_RETURN_RATIO, WORST, BEST, KELLY_CRITERION, WIN_LOSS_RATIO, ULCER_INDEX, ULCER_PERFORMANCE_INDEX) VALUES (\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s,\
        %s,%s,%s,%s,%s)"
        insertVal.insert(0, str(runID))
        insertVal.insert(1, str(sysTime))
        insertVal.insert(2, str(stockName))
        insertVal.insert(3, str(strategy))
        insertVal.insert(4, str(startDate))
        insertVal.insert(5, str(endDate))
        insertVal.insert(6, str(duration))
        insertVal.insert(7, str(equityStartValue))
        insertVal.insert(8, str(EquityEndValue))
        insertVal.insert(9, str(avg_return))
        insertVal.insert(10, str(avg_win))
        insertVal.insert(11, str(avg_loss))
        insertVal.insert(12, str(cagr))
        insertVal.insert(13, str(expected_return))
        insertVal.insert(14, str(expected_shortfall))
        insertVal.insert(15, str(consecutive_losses))
        insertVal.insert(16, str(consecutive_wins))
        insertVal.insert(17, str(gain_to_pain_ratio))
        insertVal.insert(18, str(profit_factor))
        insertVal.insert(19, str(risk_of_ruin))
        insertVal.insert(20, str(win_rate))
        insertVal.insert(21, str(sharpeRatio))
        insertVal.insert(22, str(sortino))
        insertVal.insert(23, str(calmar))
        insertVal.insert(24, str(information_ratio))
        insertVal.insert(25, str(kurtosis))
        insertVal.insert(26, str(skew))
        insertVal.insert(27, str(value_at_risk))
        insertVal.insert(28, str(conditional_value_at_risk))
        insertVal.insert(29, str(volatility))
        insertVal.insert(30, str(outlier_loss_ratio))
        insertVal.insert(31, str(outlier_win_ratio))
        insertVal.insert(32, str(max_drawdown))
        insertVal.insert(33, str(risk_return_ratio))
        insertVal.insert(34, str(worst))
        insertVal.insert(35, str(best))
        insertVal.insert(36, str(kelly_criterion))
        insertVal.insert(37, str(win_loss_ratio))
        insertVal.insert(38, str(ulcer_index))
        insertVal.insert(39, str(ulcer_performance_index))


        mySQLCursor.execute(insertQuery, insertVal)
        cnx.commit()        
    except:
        logging.info("Unable to add activity to table (ACTIVITY_DETAILS_TBL)")
