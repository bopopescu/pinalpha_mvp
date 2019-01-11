import DBConn.mongoCon as mongo
import uuid

def push_data_to_mongoDB(articles,search_query_id):
    db_connection = mongo.getDBCon()
    db = db_connection.production
    news_articles_collection = db.newsAPIArticles
    for article in articles:

        #check if news article exists
        if not check_if_news_exists(article["url"],db):
            #push the article
            try:
                article["id"]= uuid.uuid4().hex
                result = news_articles_collection.insert(article)
                print(result)
            except:
                print("Error in Inserting to Mongo")
        else:
            print("Article Exists in DB")
        findQuery = {"search_theme_id":search_query_id, "url":article["url"], "date":article["publishedAt"][0:10]}
        if not check_if_news_map_exists(findQuery,db):
            try:
                newsID = uuid.uuid4().hex
                input_dict = {"pinalpha_news_id":newsID, "search_theme_id":search_query_id, "url":article["url"], "date":article["publishedAt"][0:10]}
                print(input_dict)
                theme_article_collection = db["themeArticleMap"]
                theme_article_collection.insert(input_dict)
            except:
                print("Artile Map Exists")
        else:
            print("Article Map Exists in DB")
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

def check_if_news_map_exists(findQuery,db):
    try:
        len_news = db.themeArticleMap.find(findQuery).limit(1).count()
        if (len_news != 0):
            return True
        else:
            return False
    except:
        print("Error with MongoDB Search")
        return False
