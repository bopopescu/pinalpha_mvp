import pandas as pd
import DBConn.mongoCon as mc
import datetime

def get_impact(company,date,companyList):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpact_collection = db.dailyThemeImpact
    findQuery = {"theme": company,"date":date}
    respone = dailyThemeImpact_collection.find(findQuery)
    company_impact = 0
    for item in respone:
        company_impact = item['impact']

    findQuery = {"theme": {"$in":companyList}, "date": date}
    respone = dailyThemeImpact_collection.find(findQuery)
    industry_impact = 0
    for item in respone:
        industry_impact = industry_impact + item['impact']
    industry_impact = industry_impact/len(companyList)
    mongoCon.close()
    return [company_impact,industry_impact]

def check_if_impact_exist(db,findQuery):
    try:
        len_news = db.dailyThemeImpactIndustry.find(findQuery).count()
        print(len_news)
        if len_news != 0:
            return True
        else:
            return False
    except:
        print("Error with MongoDB Search")
        return False

def insert_mongo(company,date,impactList):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpactIndustry_collection = db.dailyThemeImpactIndustry
    findQuery = {"company":company,"date":date}
    print(findQuery)
    sents_exist = check_if_impact_exist(db, findQuery)
    if sents_exist:
        print("Sentence Exists")
    else:
        try:
            query = {"date":date,"company":company,"impact":impact_list[0],"industry_average":impact_list[1]}
            dailyThemeImpactIndustry_collection.insert(query)
            print("Insert Done")
        except:
            print("Insert Error for Sentences")
    mongoCon.close()
    return True


def execute_main():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    companyList = ["UOB", "DBS", "OCBC"]
    for item in companyList:
        companyName = item
        impact_list = get_impact(companyName,today,companyList)
        insert_mongo(companyName,today,impact_list)

execute_main()