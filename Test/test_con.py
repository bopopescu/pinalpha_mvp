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
#test_sentiment()
#df = da.get_themes_from_file(pwd+"/Data/CompanyThemes.csv")
#print(df['Theme'])
response = da.get_news_from_api("OCBC","2018-11-17","2018-12-19")
cleaned_response = da.remove_noisy_articles(response,"ocbc")
