import DBConn.mysqlCon as mysqlCon
import DBConn.mongoCon as mc
import pandas as pd
import datetime


def get_theme_sentiment_article_ids(theme,date):
    article_ids = []
    conn = mysqlCon.get_sql_con()
    query = "select pinalpha_news_id from pinalpha_mvp.pinalpha_news_keywords where date = '%s' and %s > 0"%(date,theme)
    print(query)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for item in results:
        article_ids.append(item[0])
    conn.close()
    return article_ids


def get_sentiments(article_ids):
    df =pd.DataFrame({'date':[],'sentiment':[]})
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
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
                        df = df.append({"date": sents['date'],"news_id":item,"sentiment": sents['google_score']}, ignore_index=True)
    mongoCon.close()
    return df

def aggregate_sentiment(df):
    df = df.groupby('date', as_index=False, sort=True)['sentiment'].mean()
    return df

def check_if_impact_exist(db,findQuery):
    try:
        len_news = db.themeSentimentArticlesMap.find(findQuery).count()
        print(len_news)
        if len_news != 0:
            return True
        else:
            return False
    except:
        print("Error with MongoDB Search")
        return False


def insert_to_mongo(theme,df_sentiment,):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    themeSentArticleMap_collection = db.themeSentimentArticlesMap
    for idx,item in df_sentiment.iterrows():
        query = {"theme":theme,"date":item['date'],"news_id":item['news_id'],"sentiment":item['sentiment']}
        value_exist = check_if_impact_exist(db,query)
        if value_exist:
            print("Sentiment Exists")
        else:
            try:
                result = themeSentArticleMap_collection.insert(query)
                print(result)
            except:
                print("insertError")
    mongoCon.close()

def execute_main():
    themeList = ["trade_war","credit_card_fees","wealth_management","loan_growth","private_banking","trade_tension","trade_finance","loans"]
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    print(today)
    for theme in themeList:
        articleids = get_theme_sentiment_article_ids(theme,today)
        print(articleids)
        df = get_sentiments(articleids)
        print(df)
        df = pd.DataFrame(df)
        insert_to_mongo(theme, df)

execute_main()