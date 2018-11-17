from newsapi import NewsApiClient

# Init
newsapi = NewsApiClient(api_key='00e887a151f345c68dc57a1c19526283')

# /v2/top-headlines
top_headlines = newsapi.get_top_headlines(q='microsoft',
                                          sources='bbc-news,the-verge',
                                          language='en')
print(top_headlines)