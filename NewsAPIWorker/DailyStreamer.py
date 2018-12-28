from newsapi import NewsApiClient
import DataAccess.data_access as da
import datetime
import DBConn.mongoCon as mongo
import uuid

newsapi = NewsApiClient(api_key='00e887a151f345c68dc57a1c19526283')


def collect_news():
    df_keywords = da.get_news_keyword()
    for idx,row in df_keywords.iterrows():
        #print(keyword)
        yesterday = get_date_today()
        print(row['keyphrase'].lower())
        all_articles = newsapi.get_everything(q='\"'+row['keyphrase'].lower()+'\"',
                                  from_param=yesterday,
                                  language='en',
                                  page_size=100)
        #print(all_articles)
        pageNumbers = all_articles['totalResults']%100
        pageNumbers = min(pageNumbers,20)
        push_data_to_mongoDB(all_articles['articles'],row['keyphrase'])
        i=2
        while(pageNumbers>0):
            all_articles = newsapi.get_everything(q='\"' + row['keyphrase'].lower() + '\"',
                                                  from_param=yesterday,
                                                  language='en',
                                                  page_size=100,
                                                  page=i)
            i = i+1
            pageNumbers = pageNumbers-1
            push_data_to_mongoDB(all_articles['articles'], row['keyphrase'])
    return 0

def get_date_today():
    yesterday = (datetime.datetime.today()-datetime.timedelta(1)).strftime('%Y-%m-%d')
    #print(yesterday)
    return yesterday

def push_data_to_mongoDB(articles,search_query_id):
    db_connection = mongo.getDBCon()
    db = db_connection.production
    news_articles_collection = db.newsAPIArticles
    for article in articles:
        #check if news article exists
        if not check_if_news_exists(article["url"],db):
            #push the article
            try:
                #print(article)
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
        # try:
        #
        # except:
        #     print('Error inserting Mapping')



def check_if_news_exists(news_id,db):
    #db.newsAPIArticles.find({"url": news_id}).limit(1).count()
    #return True
    try:
        print(news_id)
        len_news = db.newsAPIArticles.find({"url": news_id}).limit(1).count()
        print(len_news)
        if (len_news != 0):
            return True
        else:
            return False
    except:
        print("Error with MongoDB Search")
        return False