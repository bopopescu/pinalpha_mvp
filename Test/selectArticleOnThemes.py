import DBConn.mongoCon as mc

def get_article_ids(theme,date):
    query = {"$and":[{"theme":theme},{"date":date}]}
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    themeSentArticleMap_collection = db.themeSentimentArticlesMap
    article_ids = []
    articles = themeSentArticleMap_collection.find(query)
    for item in articles:
        article_ids.append(item["news_id"])
    article_ids = list(set(article_ids))
    mongoCon.close()
    return article_ids




print(len(get_article_ids("trade_war","2018-12-23")))