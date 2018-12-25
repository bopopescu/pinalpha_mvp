import pandas as pd
import MongoDBConn.mongoCon as mc
import matplotlib.pyplot as plt
import seaborn as sns


def get_sentiments():
    df =pd.DataFrame({'date':[],'sentiment':[]})
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    articleSentiment_collection = db.newsArticleSentiment
    cursor = articleSentiment_collection.find({})
    for document in cursor:
        df = df.append({"date": document['date'],"sentiment": document['sentiment']}, ignore_index=True)
    return df

def aggregate_sentiment(df):
    df = df.groupby('date', as_index=False, sort=False)['sentiment'].mean()
    return df

def plot_sentiment(df):
    plt.plot(df.date,df.sentiment)
    plt.show()
    #df.plot(x='date',y='sentiment',kind='line')

def plot_sentiment_category(df):
    sns.catplot(x="date", y="sentiment", kind="box", data=df)


df_sentiments = get_sentiments()
#df_aggregate_sentiment = aggregate_sentiment(df_sentiments).sort_values(by=['date'])
print(df_sentiments)
plot_sentiment_category(df_sentiments)