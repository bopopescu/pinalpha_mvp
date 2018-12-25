from newsapi import NewsApiClient
import DataAccess.data_access as da
import datetime
import MongoDBConn.mongoCon as mongo
import uuid

def push_data_to_mongoDB(articles,search_query_id):
    db_connection = mongo.getDBCon()
    db = db_connection.production
    news_articles_collection = db.newsAPIArticles
    for article in articles:
        #check if valid article
        #implement a way to see if there exist a valid article

        #check if news article exists
        if not check_if_news_exists(article["url"],db):
            #push the article
            try:
                article["id"]= uuid.uuid4().hex
                result = news_articles_collection.insert(article)
                print(result)
            except:
                print("Error in Inserting to Mongo")
        #push the reference
        #create a pinAlpha News ID
        newsID = uuid.uuid4().hex
        input_dict = {"pinalpha_news_id":newsID, "search_theme_id":search_query_id, "url":article["url"], "date":article["publishedAt"][0:10]}
        print(input_dict)
        theme_article_collection = db["themeArticleMap"]
        theme_article_collection.insert(input_dict)

def check_if_news_exists(news_id,db):
    try:
        len_news = db.newsAPIArticles.find({"url": news_id}).limit(1).count()
        if (len_news != 0):
            return True
        else:
            return False
    except:
        print("Error with MongoDB Search")
        return False

def is_valid_article(article,theme):
    content = article['content']
    #occurances = content