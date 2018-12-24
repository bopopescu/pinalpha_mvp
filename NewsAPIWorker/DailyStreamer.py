from newsapi import NewsApiClient
import DataAccess.data_access as da
import datetime
import MongoDBConn.mongoCon as mongo

newsapi = NewsApiClient(api_key='00e887a151f345c68dc57a1c19526283')


def collect_news():
    df_keywords = da.get_news_keyword()
    for idx,row in df_keywords.iterrows():
        #print(keyword)
        yesterday = get_date_today()
        print(row['pinalpha_company_name'].lower())
        all_articles = newsapi.get_everything(q=row['pinalpha_company_name'].lower(),
                                  from_param=yesterday,
                                  language='en',
                                  page_size=100)
        #print(all_articles)
        push_data_to_mongoDB(all_articles['articles'],row['pinalpha_company_code'])

def get_date_today():
    yesterday = (datetime.datetime.today()-datetime.timedelta(1)).strftime('%Y-%m-%d')
    #print(yesterday)
    return yesterday

def push_data_to_mongoDB(articles,company_id):
    db_connection = mongo.getDBCon()
    db = db_connection.production
    news_articles_collection = db.newsAPIArticles
    for article in articles:
        article['companyID']=company_id
        article['keyThemeID']=None
        #print(article)
        try:
            #result = news_articles_collection.insertOne(article)
            print(article)
        except:
            print("Error in Inserting to Mongo")
