import pandas as pd
import DBConn.mongoCon as mc
from pandas import Series
import numpy as np
from sklearn import preprocessing


def get_tradewar_data():
    df_tradewar = pd.DataFrame()
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpact_collection = db.dailyThemeImpactIntermediate
    findQuery = {"theme": "trade_war"}
    respone = dailyThemeImpact_collection.find(findQuery)
    for item in respone:
        df_tradewar = df_tradewar.append({"date":item['date'],"impact":item['impact']},ignore_index=True)
    return df_tradewar

def scaling(df):
    df_scaled = pd.DataFrame()
    minval = min(df['impact'])
    maxVal = max(df['impact'])
    for idx,item in df.iterrows():
        print(item['impact'])
        normalized = 2*((item['impact']-minval)/(maxVal-minval))-1
        df_scaled = df_scaled.append({"date":item['date'],"impact":normalized},ignore_index=True)
    return df_scaled

def change_scale(df):
    #df['impact'] = df['sentiment'] * -1
    x = df['impact'].values  # returns a numpy array
    min_max_scaler = preprocessing.MinMaxScaler(feature_range=(-0.8,0.8))
    x_scaled = min_max_scaler.fit_transform(x.reshape(-1, 1))
    df['impact'] = pd.DataFrame(x_scaled)
    df['impact'] = df['impact'] * -1
    return df


def write_mongo(theme,df):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpact_collection = db.dailyThemeImpact
    for idx,item in df.iterrows():
        findQuery = {"theme":theme,"date":item['date']}
        #print(findQuery)
        impact_exist = check_if_impact_exist(db,findQuery)
        if impact_exist:
            print("Theme Impact for Day Exists")
        else:
            query = {"theme": theme, "date": item['date'], "impact": item['impact']}
            try:
                dailyThemeImpact_collection.insert(query)
                print("Insert Done")
            except:
                print("Insert Error for Sentences")
    mongoCon.close()
    return True

def check_if_impact_exist(db,findQuery):
    try:
        len_news = db.dailyThemeImpact.find(findQuery).count()
        print(len_news)
        if len_news != 0:
            return True
        else:
            return False
    except:
        print("Error with MongoDB Search")
        return False

def delete_tradwar():
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpact_collection = db.dailyThemeImpact
    query = {"theme":"DBS"}
    dailyThemeImpact_collection.delete_many(query)
    query = {"theme": "dbs"}
    dailyThemeImpact_collection.delete_many(query)
    mongoCon.close()

df = get_tradewar_data()
print(df)
#delete_tradwar()
write_mongo("dbs",df)