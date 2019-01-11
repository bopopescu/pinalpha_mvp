import DBConn.mongoCon as mc
import json
from bson import json_util
from bson.json_util import dumps

def get_sentnces_daily(theme,date):
    query = {"theme":theme,"date":date}
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeSentence_collection = db.dailyThemeSentence
    result = dailyThemeSentence_collection.find(query)
    json_result = dumps(result)
    return json_result


def getnews_daily(company):
    query = {"company_name": company}
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    companyTenMostRecent_collection = db.companyTenMostRecent
    result = companyTenMostRecent_collection.find(query)
    json_result = dumps(result)
    return json_result

jsonResult = getnews_daily("OCBC")
with open('../Data/dailyNews.json', 'w') as outfile:
    json.dump(jsonResult, outfile)
