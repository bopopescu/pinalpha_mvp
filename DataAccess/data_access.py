import pandas as pd
import MongoDBConn.mongoCon as mc
import requests
import re
import json
from pandas.io.json import json_normalize

def get_today_news_data_mongo(company):
    #get mongo connection
    mongo_con = mc.getDBCon()
    df = pd.DataFrame()
    #call API and get today's data
    return df

def get_any_news_data(company,mongo_con,start_date,end_date):
    df = pd.DataFrame()
    #call api and get historical data
    return df

def get_themes_from_file(filename):
    df_themes = pd.read_csv(filename)
    return df_themes

def put_themes_data_to_db():
    mongo_con = mc.getDBCon() #get mongoDB client
    prod_db = mongo_con['production']
    themes_collection = prod_db['themes']

    #read themes from file
    themes_filename = "./Data/CompanyThemes.csv"
    df_themes = get_themes_from_file()
    return df_themes

def get_news_from_api(company,start_date,end_date):
    df = pd.DataFrame()
    baseAPI = "https://bc5gy7key1.execute-api.ap-southeast-1.amazonaws.com/api/fetch-company-articles?" \
              "company=%s&from=%s&to=%s&page=%s"
    call_url = baseAPI % (company,start_date,end_date,1)
    response = requests.get(call_url)
    return response.json()

def getSentences(article):
    sentences = [x for x in article.sents]
    return(sentences)

def remove_noisy_articles(news_json,word):
    match_string = r"\b" + word + r"\b"
    for idx,item in enumerate(news_json):
        try:
            count = len(re.findall(match_string, item.get('article'), re.IGNORECASE))
            if (count <= 1):
                news_json.pop(idx)
        except:
            news_json.pop(idx)
    return news_json