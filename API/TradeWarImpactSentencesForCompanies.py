import DBConn.mongoCon as mc
import pandas as pd
import requests
import NLPAnalysis.summary as summary
from datetime import date, datetime, timedelta
import NewsAPIWorker.OneTimeCollector as oneTimer

def get_content_from_articles(phrase,theme,YMdate):
    query = {"$and":[{"content":{"$regex":phrase}},{"content":{"$regex":theme}},
                     {"publishedAt":{"$regex":YMdate}}]}
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    newsAPIArticles_collection = db.newsAPIArticles  # collection
    news_article = newsAPIArticles_collection.find(query)
    for item in news_article:
        extracted = summary.ExtractSummary(item['content'])
        if not phrase == "trade war":
            phrase = "SG Banks"
        query = {"date":YMdate,"type":phrase,"news_id":item['id'],"sentence":extracted}
        print(query)
        insert_mongo(query)
        break
    mongoCon.close()

def insert_mongo(query):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    tradewarImpactSentences_collection = db.tradewarImpactSentences  # collection
    tradewarImpactSentences_collection.insert(query)
    mongoCon.close()

def execute_main():
    today = datetime.now().strftime("%Y-%m-%d")
    phrase = "economy"
    theme = "trade war"
    get_content_from_articles(phrase,theme,today)

execute_main()