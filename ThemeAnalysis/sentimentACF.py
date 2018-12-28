import DBConn.mysqlCon as mysqlCon
import DBConn.mongoCon as mc
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import pylab
from scipy import stats
import plotly.plotly as py
import numpy as np

def get_theme_sentiment_article_ids(theme,company):
    article_ids = []
    conn = mysqlCon.get_sql_con()
    query = "select pinalpha_news_id from pinalpha_mvp.pinalpha_news_keywords where (trade_tension > 0 or %s > 0) and %s > 0 "%(theme,company)
    print(query)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for item in results:
        article_ids.append(item[0])
    return article_ids


def get_sentiments(article_ids):
    df =pd.DataFrame({'date':[],'sentiment':[]})
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    article_collection = db.newsAPIArticles
    themeArticleMap_Collection = db.themeArticleMap
    article_sentiment_collection = db.newsArticleSentimentValues
    for item in article_ids:
        cursor = article_collection.find({"id":item})
        for document in cursor:
            theme_cursor = themeArticleMap_Collection.find({"url":document['url']})
            for doc in theme_cursor:
                sentiment_cursor = article_sentiment_collection.find({"pinalpha_news_id":doc['pinalpha_news_id']})
                for sents in sentiment_cursor:
                    df = df.append({"date": sents['date'],"sentiment": sents['google_score']}, ignore_index=True)
    return df

def aggregate_sentiment(df):
    df = df.groupby('date', as_index=False, sort=True)['sentiment'].mean()
    return df

def plot_sentiment(df):
    xi = np.arange(0,140,1)
    print(xi)
    y = list(df['sentiment'])
    print(y)
    slope, intercept, r_value, p_value, std_err = stats.linregress(xi, y)
    line = slope * xi + intercept

    plt.plot(xi, y, 'o', xi, line)
    plt.show()
    #pylab.title('Linear Fit with Matplotlib')
    #ax = plt.gca()
    #ax.set_axis_bgcolor((0.898, 0.898, 0.898))
    #fig = plt.gcf()
    #py.plot_mpl(fig, filename='linear-Fit-with-matplotlib')

def plot_linegraph(df):
    plt.plot(df['sentiment'])
    plt.show()

# articleids = get_theme_sentiment_article_ids("trade_war","dbs")
# print(len(articleids))
# df = get_sentiments(articleids)
# print(len(df))
# df = aggregate_sentiment(df)
# df = pd.DataFrame(df)
# df.to_csv("sentiment_dbs_tradewar.csv")
# plot_sentiment(df)

def readSentimentFile():
    df = pd.read_csv("sentiment_dbs_tradewar.csv")
    return df

df = readSentimentFile()
plot_linegraph(df)