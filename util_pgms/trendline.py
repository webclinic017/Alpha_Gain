import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import linregress

x1 = np.array([145,157,240])
y1 = np.array([1106,1012, 1000])
slope1, intercept1, r_value1, p_value1, std_err1 = linregress(x1, y1)
print("slope1: %f, intercept1: %f" % (slope1, intercept1))
print("R-squared1: %f" % r_value1**2)


z1 = slope1 * 240 + intercept1
print(z1)

x2 = np.array([145,157, 240])
y2 = np.array([71.61,48.94,40])
slope2, intercept2, r_value2, p_value2, std_err2 = linregress(x2, y2)
print("slope2: %f, intercept2: %f" % (slope2, intercept2))
print("R-squared2: %f" % r_value2**2)
z2 = slope2 * 240 + intercept2
print(z2)

plt.figure(figsize=(5, 5))
plt.plot(x1, y1, 'o', label='Price')
x5 = intercept1 + slope1*x1
plt.plot(x1, x5, 'r', label='price line')
plt.show()
plt.plot(x2, y2, 'o', label='RSI')
plt.plot(x1, intercept2 + slope2*x2, 'r', label='RSI line')
plt.legend()
plt.grid()
plt.show()