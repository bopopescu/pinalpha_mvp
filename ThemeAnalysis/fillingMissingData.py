import DBConn.mysqlCon as mysqlCon
import DBConn.mongoCon as mc
import pandas as pd
from datetime import date, datetime, timedelta
import NewsAPIWorker.OneTimeCollector as oneTimer


def write_mongo(theme,date):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpact_collection = db.dailyThemeImpact
    findQuery = {"theme": theme, "date": date}
    # print(findQuery)
    impact_exist = check_if_impact_exist(db, findQuery)
    if impact_exist:
        print("Theme Impact for Day Exists")
    else:
        if theme == "loan_growth":
            query = {"theme": theme, "date": date, "impact": 37}
        else:
            query = {"theme": theme, "date": date, "impact": 55}
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

def execute_main():
    today = datetime.now()
    themeList = ["loan_growth","wealth_management"]
    for theme in themeList:
        write_mongo(theme, today.strftime("%Y-%m-%d"))

execute_main()
