import MongoDBConn.mongoCon as mc
import SentimentAnalyser.sentimentAnalsis as sa
import ThemeAnalysis.BankingThemes as themes
import DataAccess.data_access as da
import json
from pandas.io.json import json_normalize

pwd = "/home/kasun/PycharmProjects/MVP/"

def test_connection():
    print(mc.getDBCon())

def test_database():
    conn = mc.getDBCon()
    print(conn.database_names())

def test_sentiment():
    Text = "Following Jiayu’s method, I was able to scrape " \
           "roughly 2.2k comments from the target page into a " \
           "CSV file. However, instead of using the access token " \
           "found in the screenshot below, I had to use the Access " \
           "Token Debugger to fill in the token value in the script. "
    scores = sa.get_sentiment_of_article(Text)
    print(scores)

test_database()

#global Variables
companyList = ["UOB","OCBC","DBS","UBS","credit%20suisse","julius%20baer"]
StartDate = "2018-11-16"
EndDate = "2018-12-19"

#process data
for companyName in companyList:
    response = da.get_news_from_api(companyName, StartDate, EndDate)
    print(len(response))
    cleaned_response = da.remove_noisy_articles(response, companyName)
    print(len(cleaned_response))
    da.map_article_sentences(cleaned_response,companyName)

#da.get_sentences_with_theme("Test")
#    print(json.dumps(item))