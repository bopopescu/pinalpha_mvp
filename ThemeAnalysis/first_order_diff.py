import pandas as pd
from pandas import Series
from matplotlib import pyplot,pylab
import statsmodels.api as sm
import numpy as np
from statsmodels.tsa.api import ExponentialSmoothing, SimpleExpSmoothing, Holt
from sklearn import preprocessing
import DBConn.mongoCon as mc

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

def get_moving_average(dataset):
    moving_avg = dataset.rolling(window=4).mean()
    return moving_avg

def plot_histo(dataset):
    pyplot.hist(dataset,bins='auto')
    pyplot.title("histogram of DBS Trade War")
    pyplot.show()

def exponential_smoothing(panda_series, alpha_value):
    ouput=sum([alpha_value * (1 - alpha_value) ** i * x for i, x in
                enumerate(reversed(panda_series))])
    return ouput

def simple_smoothing(data,company):
    fit1 = SimpleExpSmoothing(data).fit(smoothing_level=0.1, optimized=False)
    fcast1 = fit1.forecast(1).rename(r'$\alpha=0.1$')
    print(fit1['fittedvalues'])
    fcast1.plot(marker='o', color='blue', legend=True)
    fit1.fittedvalues.plot(marker='o', color='blue')
    pyplot.title("Trade War Impact - "+company)
    pyplot.show()

def aggregate_sentiment(df):
    df = df.groupby('date', as_index=False, sort=True)['sentiment'].sum()
    return df

def raw_plot(data,company):
    pyplot.plot(data['date'],data['sentiment'])
    pyplot.title("Trade War_"+company)
    pyplot.show()



def plot_all_graphs(data,company):
    i=0
    for df in data:
        fit1 = SimpleExpSmoothing(df['sentiment']).fit(smoothing_level=1, optimized=False)
        fcast1 = fit1.forecast(1).rename(r'$\alpha=0.1$')
        fcast1.plot(marker='o', color='blue', legend=True)
        fit1.fittedvalues.plot(marker='o', color='blue')
        pyplot.title("Trade War Impact - " + company[i])
        i=i+1
        pyplot.show()


def change_scale(df):
    df['sentiment'] = df['sentiment'] * -1
    x = df['sentiment'].values  # returns a numpy array
    print(x)
    min_max_scaler = preprocessing.MinMaxScaler(feature_range=(0,80))
    x_scaled = min_max_scaler.fit_transform(x.reshape(-1, 1))
    print(x_scaled)
    df['sentiment'] = pd.DataFrame(x_scaled)
    return df

def get_mv_avg_df(df):
    df_mvg = pd.DataFrame(get_moving_average(df['sentiment']))
    df_mvg = pd.concat([df['date'].reset_index(drop=True), df_mvg], axis=1)
    return df_mvg

def write_impact_file(theme,df):
    df = pd.DataFrame(df)
    filename = "../Data/impact_"+theme+".csv"
    df.to_csv(filename)

def check_if_impact_exist(db,findQuery):
    try:
        len_news = db.dailyThemeImpact.find(findQuery).count()
        print(len_news)
        if len_news != 0:
            return True
        else:
            return False
    except:
        print("Error with MongoDB Search")
        return False

def write_mongo(theme,df):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpact_collection = db.dailyThemeImpact
    for idx,item in df.iterrows():
        findQuery = {"theme":theme,"date":item['date']}
        #print(findQuery)
        impact_exist = check_if_impact_exist(db,findQuery)
        if impact_exist:
            print("Theme Impact for Day Exists")
        else:
            query = {"theme": theme, "date": item['date'], "impact": item['sentiment']}
            try:
                dailyThemeImpact_collection.insert(query)
                print("Insert Done")
            except:
                print("Insert Error for Sentences")
    mongoCon.close()
    return True

def check_if_intermediate_impact_exist(db,findQuery):
    try:
        len_news = db.dailyThemeImpactIntermediate.find(findQuery).count()
        print(len_news)
        if len_news != 0:
            return True
        else:
            return False
    except:
        print("Error with MongoDB Search")
        return False

def write_intermediate_results_mongo(theme,df):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpactIntermediate_collection = db.dailyThemeImpactIntermediate
    for idx,item in df.iterrows():
        findQuery = {"theme":theme,"date":item['date']}
        #print(findQuery)
        impact_exist = check_if_intermediate_impact_exist(db,findQuery)
        if impact_exist:
            print("Theme Impact for Day Exists")
        else:
            query = {"theme": theme, "date": item['date'], "impact": item['sentiment']}
            try:
                dailyThemeImpactIntermediate_collection.insert(query)
                print("Insert Done")
            except:
                print("Insert Error for Sentences")
    mongoCon.close()
    return True

def read_sentiment_from_mongo(theme):
    df_sentiments = pd.DataFrame()
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    themeSentimentArticlesMap_collection = db.themeSentimentArticlesMap
    query = {"theme":theme}
    response = themeSentimentArticlesMap_collection.find(query)
    for item in response:
        df_sentiments = df_sentiments.append({"date":item['date'],"sentiment":item['sentiment']},ignore_index=True)
    return df_sentiments

def execute_main():
    themeList = ["trade_war","wealth_management","loan_growth"]
    for theme in themeList:
        df = read_sentiment_from_mongo(theme)
        #print(df)
        df = aggregate_sentiment(df)
        df = get_mv_avg_df(df)
        df = df.fillna(0)
        write_intermediate_results_mongo(theme, df)
        df = change_scale(df)
        write_mongo(theme, df)

execute_main()


