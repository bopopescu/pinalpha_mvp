import NewsAPIWorker.OneTimeCollector as oneTimer
from datetime import date, datetime, timedelta
import DataAccess.data_access as da
import NewsAPIWorker.mongoHandler as mongohandler

start = date(2018, 12, 18)
end = date(2018, 12, 27)
delta = timedelta(days=7)
dates = oneTimer.populate_dates(start,end,delta)
keyPhrases = da.get_news_keyword()['keyphrase']

for keyPhrase in keyPhrases:
    for i in range(0,len(dates)-1):
        start = dates[i].strftime('%Y-%m-%d')
        end = (dates[i+1]-timedelta(days=1)).strftime('%Y-%m-%d')
        searchPhrase = '\"'+keyPhrase+'\"'
        url = oneTimer.get_urls(start,end,searchPhrase)
        print(url)
        jsonArticles = oneTimer.collect_news(url)
        if not jsonArticles['totalResults']==0:
            mongohandler.push_data_to_mongoDB(jsonArticles['articles'], keyPhrase)

