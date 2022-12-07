import matplotlib as mpl
import matplotlib.pyplot as plt
import quantstats as qs

# extend pandas functionality with metrics, etc.
qs.extend_pandas()

# fetch the daily returns for a stock
stock = qs.utils.download_returns('AAPL')
print(stock)
# qs.plots.snapshot(stock, title='Facebook Performance')
# stock.plot_snapshot(title='Facebook Performance')
qs.reports.html(stock)
qs.reports.full(stock)
plt.show(block=False)
plt.pause(300)
plt.close()
# show sharpe ratio
# qs.stats.sharpe(stock)

# # or using extend_pandas() :)
# stock.sharpe()