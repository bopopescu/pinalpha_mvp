import pandas as pd
from pandas import Series
from matplotlib import pyplot


def read_Data(filename):
    df = pd.read_csv(filename)
    return df

# create a differenced series
def difference(dataset, interval=1):
    diff = list()
    for i in range(interval, len(dataset)):
        value = dataset[i] - dataset[i - interval]
        diff.append(value)
    return Series(diff)

df = read_Data("sentiment_dbs_tradewar.csv")
diff = difference(df['sentiment'])
pyplot.plot(diff)
pyplot.show()