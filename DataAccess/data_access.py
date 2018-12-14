import pandas as pd
import MongoDBConn.mongoCon as mc

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

