import DBConn.mysqlCon as mysqlCon
import DBConn.mongoCon as mc
import pandas as pd
from datetime import date, datetime, timedelta
import NewsAPIWorker.OneTimeCollector as oneTimer
import decimal



def get_company_ratios():
    df = pd.DataFrame()
    conn = mysqlCon.get_sql_con()
    query = "SELECT company_name,net_interest_income,income_from_wealthmanagement FROM pinalpha_mvp.company_stats where year = '2018' and quater = '3';"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for item in results:
        #print(item)
        df = df.append({"company":item[0],"loan_income":item[1],
                        "wm_income":item[2]},ignore_index=True)
    conn.close()
    df['loan_ratio'] = df['loan_income']/(df['loan_income']+df['wm_income'])
    df['wm_ratio'] = df['wm_income']/(df['loan_income']+df['wm_income'])
    return df

def get_theme_impact(theme,date):
    df = pd.DataFrame()
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpact_collection = db.dailyThemeImpact
    findQuery = {"theme": theme,"date":date}
    respone = dailyThemeImpact_collection.find(findQuery)
    for item in respone:
        print(item)
        df = df.append({"date":item['date'],"impact":item['impact']},ignore_index=True,sort=True)
    return df

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

def insert_mongo(impactQuery):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpact_collection = db.dailyThemeImpact
    findQuery = impactQuery
    print(findQuery)
    sents_exist = check_if_impact_exist(db, findQuery)
    if sents_exist:
        print("Sentence Exists")
    else:
        try:
            dailyThemeImpact_collection.insert(impactQuery)
            print("Insert Done")
        except:
            print("Insert Error for Sentences")
    return True

def calculate_company_impact(impactList,df_ratios,today):
    trade_war_ratio = float(0.5)
    #df_final_impact = pd.DataFrame()
    for idx,item in df_ratios.iterrows():
        companyName = item['company']
        loan_ratio = float(item['loan_ratio'])
        wm_ratio = float(item['wm_ratio'])
        df_tradeWar = impactList[0]
        df_wm = impactList[1]
        df_loans = impactList[2]
        #tmp_tw = trade_war_ratio * df_tradeWar.loc[df_tradeWar['date'] == today, 'impact'].iloc[0]
        #tmp_wm = (1 - trade_war_ratio) * wm_ratio * df_wm.loc[df_wm['date'] == today, 'impact'].iloc[0]
        #tmp_lg = (1 - trade_war_ratio) * loan_ratio * df_loans.loc[df_loans['date'] == today, 'impact'].iloc[0]
        #print(tmp_tw + tmp_wm + tmp_lg)
        final_impact = trade_war_ratio * df_tradeWar.loc[df_tradeWar['date'] == today, 'impact'].iloc[0] + \
                       (1 - trade_war_ratio) * wm_ratio * df_wm.loc[df_wm['date'] == today, 'impact'].iloc[0] + \
                       (1 - trade_war_ratio) * loan_ratio * df_loans.loc[df_loans['date'] == today, 'impact'].iloc[0]
        query = {"theme": companyName, "date": today, "impact": final_impact}
        print(query)
        #df_final_impact = df_final_impact.append(query, ignore_index=True)
        insert_mongo(query)
    return True

# def correct_impact():
#     mongoCon = mc.getDBCon()  # connection
#     db = mongoCon.production  # database
#     dailyThemeImpact_collection = db.dailyThemeImpactIntermediate
#     findQuery = {"theme":"trade_war"}
#     print(findQuery)
#     sents_exist = check_if_impact_exist(db, findQuery)
#     if sents_exist:
#         print("Sentence Exists")
#     else:
#         try:
#             dailyThemeImpact_collection.insert(impactQuery)
#             print("Insert Done")
#         except:
#             print("Insert Error for Sentences")
#     return True

def execute_main():
    themeList = ["trade_war", "wealth_management", "loan_growth"]
    impact_list = []
    today = datetime.now().strftime("%Y-%m-%d")
    for theme in themeList:
        impact_list.append(get_theme_impact(theme,today))
    #print(impact_list[0])
    df_ratios = get_company_ratios()
    #print(df_ratios['loan_ratio'],df_ratios['wm_ratio'])
    result = calculate_company_impact(impact_list,df_ratios,today)

    print(result)


execute_main()