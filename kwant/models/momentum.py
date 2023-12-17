import numpy as np
from sklearn.linear_model import LinearRegression


def momentum(prices):
    y = np.array([np.log(i) for i in prices]).reshape(-1, 1)
    X = np.array([i for i in range(0, len(y))]).reshape(-1, 1)
    reg = LinearRegression().fit(X, y)
    slope = reg.coef_[0][0]
    r2 = reg.score(X, y)

    return slope * r2
