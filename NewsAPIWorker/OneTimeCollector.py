from newsapi import NewsApiClient
import DataAccess.data_access as da

newsapi = NewsApiClient(api_key='00e887a151f345c68dc57a1c19526283')


def collect_news():
    df_keywords = da.get_news_keyword()
    for keyword in df_keywords['Keyword']:
        print(keyword)
    return
