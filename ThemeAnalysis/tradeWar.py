import MongoDBConn.mongoCon as mc
import SentimentAnalyser.sentimentAnalsis as sa

def read_tradewar_articles_bulk():
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    themeArticle_collection = db.themeArticleMap  # collection
    query = {"search_theme_id": "trade war"}
    all_articles_links = themeArticle_collection.find(query)
    article_list = []
    newsArticle_collection = db.newsAPIArticles
    for x in all_articles_links:
        #process one article at a time
        query = {"url": x["url"]}
        #print(query)
        news_article = newsArticle_collection.find(query)
        for item in news_article:
            #print(item)
            item["pinalpha_news_id"] = x['pinalpha_news_id']
            article_list.append(item)
    mongoCon.close()
    return article_list

def get_sentiment(article_list):
    sentiment_list = []
    for article in article_list:
        sentiment = sa.get_sentiment_of_article(article['content'])
        sentimentObject = {"pinalpha_news_id":article['pinalpha_news_id'],"date":article['publishedAt'][0:10],"sentiment":sentiment}
        sentiment_list.append(sentimentObject)
    return sentiment_list


def update_mongo_sentiment(sentiment_list):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    articleSentiment_collection = db.articleSentiment  # collection
    #articleSentiment_collection.insert(sentiment_list)
    try:
        result = articleSentiment_collection.insert(sentiment_list)
        print(result)
    except:
        print("Insert Error - sentiment")
    mongoCon.close()
    return

articles = read_tradewar_articles_bulk()
sentiments = get_sentiment(articles)
update_mongo_sentiment(sentiments)