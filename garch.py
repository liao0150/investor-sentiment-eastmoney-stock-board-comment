#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import numpy as np
from scipy.stats.stats import pearsonr
import statsmodels.tsa.stattools as ts
import math
import statsmodels.api as sm
from scipy import stats
from arch.univariate import LS
import warnings
from arch.univariate import arch_model
from arch.univariate import ARCH, GARCH


warnings.filterwarnings("ignore", message="numpy.dtype size changed")
inputfile = 'model_month.csv'
outputfile = 'dimention_reducted.csv'


data = pd.read_csv(inputfile)

data = data.dropna()
data = data[0:-1]
print(len(data))
# print(data.info())
x_column = data[['PCA_index_t-1','CPI','PPI_increase']]
y_column = data['ln_return']
# _ = plt.plot(x_column)
# _ = plt.plot(y_column)
# plt.show()
x = pd.DataFrame(x_column)
y = np.array(y_column)

ls = LS(y,x=x)
ls.volatility = GARCH(p=1, q=1)
res = ls.fit()
summary = res.summary()
print(summary)
print(res.params)


