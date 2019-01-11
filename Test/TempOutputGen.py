import pandas as pd
import DBConn.mongoCon as mc
import NewsAPIWorker.OneTimeCollector as oneTimer
from datetime import date, datetime, timedelta
import numpy as np

def read_data(themeList):
    RawSentimentData = []
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    themeSentArticleMap_collection = db.themeSentimentArticlesMap
    for theme in themeList:
        theme_data_df =pd.DataFrame()
        query = {"theme":theme}
        cursor = themeSentArticleMap_collection.find(query)
        for item in cursor:
            theme_data_df = theme_data_df.append({"date": item['date'], "news_id": item, "sentiment": item['sentiment']},ignore_index=True)
        RawSentimentData.append(theme_data_df)
    return RawSentimentData


def aggregate_sentiment(df):
    df = df.replace(0,np.NaN)#added the nulls here
    df = df.groupby('date', as_index=False, sort=True)['sentiment'].sum()
    return df

def get_all_theme_agregates(dlist):
    agg_list = []
    for item in dlist:
        df = aggregate_sentiment(item)
        agg_list.append(df)
    return agg_list

def write_file(dlist,themes):
    i = 0
    for theme in themes:
        filename = "../Data/"+theme+".csv"
        df = pd.DataFrame(dlist[i])
        df = df.replace(np.NaN,0)
        df.to_csv(filename)
        i = 1+1

def data_fill(dates,dlist):
    newList = []
    for df in dlist:
        filled_df = pd.DataFrame()
        for item in dates:
            #print(df['date'])
            #print(item)
            #print(item.strftime("%Y-%m-%d") in df['date'])
            if item.strftime("%Y-%m-%d") in df.date.values:
                filled_df = filled_df.append({"date":item.strftime("%Y-%m-%d"),"sentiment":df.loc[df['date'] == item.strftime("%Y-%m-%d"), 'sentiment'].iloc[0]}, ignore_index=True)
            else:
                filled_df = filled_df.append({"date": item.strftime("%Y-%m-%d"), "sentiment":0}, ignore_index=True)
        newList.append(filled_df)
    return newList

themeList = ["trade_war","wealth_management","loan_growth","credit_card_fees"]
companyBreakDown = pd.read_csv("../Data/companybreakdown.csv")
start = date(2018, 1, 1)
end = date(2018, 12, 31)
delta = timedelta(days=1)


dates = oneTimer.populate_dates(start,end,delta)
dlist = read_data(themeList)
dlist = get_all_theme_agregates(dlist)
dlist = data_fill(dates,dlist)
#print(dlist)
write_file(dlist,themeList)

#def read_data_from_file():

# companies = pd.DataFrame([{"company":"UOB","trade_war":0.7,"wealth_management":(133/1842),"loan_growth":(1599/1842),"credit_card_fees":(110/1842)},
#              {"company": "DBS", "trade_war": 0.7, "wealth_management": (292 / 2750), "loan_growth": (2273 / 2750),
#               "credit_card_fees": (292 / 2750)},
#              {"company": "OCBC", "trade_war": 0.7, "wealth_management": (217 / 1768), "loan_growth": (1505 / 1768),
#               "credit_card_fees": (46 / 1768)}])
#
# companies = pd.DataFrame([{"company":"UOB","trade_war":0.7,"wealth_management":0,"loan_growth":0,"credit_card_fees":0},
#              {"company": "DBS", "trade_war": 0.7, "wealth_management": 0, "loan_growth": 0,
#               "credit_card_fees": 0},
#              {"company": "OCBC", "trade_war": 0.7, "wealth_management": 0, "loan_growth": 0,
#               "credit_card_fees": 0}])
#
#
# def prepare_data_for_company(themes,companies,dlist):
#
#     for idx,company in companies.iterrows():
#         i = 0
#         for theme in themes:
#             filename = "../Data/"+theme+"_"+company['company']+".csv"
#             df = dlist[i]
#             print(df)
#             print(company[theme])
#             print("\n")
#             df['sentiment'] *= company[theme]
#             df = pd.DataFrame(df)
#             df.to_csv(filename)
#             i = i + 1
#
# prepare_data_for_company(themeList,companies,dlist)