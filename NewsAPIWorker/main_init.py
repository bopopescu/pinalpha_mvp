import NewsAPIWorker.OneTimeCollector as oneTimer
import DataAccess.data_access as da
import NewsAPIWorker.mongoHandler as mongohandler
import datetime

startTime = (datetime.datetime.now() - datetime.timedelta(hours=12)) .strftime('%Y-%m-%d %H:%M:%S').replace(" ","T")
print(startTime)
keyPhrases = da.get_news_keyword()['keyphrase']
for keyPhrase in keyPhrases:
    print(keyPhrase)
    searchPhrase = '\"' + keyPhrase + '\"'
    url = oneTimer.get_urls(startTime, searchPhrase)
    print(url)
    jsonArticles = oneTimer.collect_news(url)
    if not jsonArticles['totalResults'] == 0:
        print(jsonArticles)
        mongohandler.push_data_to_mongoDB(jsonArticles['articles'], keyPhrase)


