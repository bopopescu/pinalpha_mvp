import pandas as pd
import DBConn.mongoCon as mc
from matplotlib import pyplot,pylab

def get_company_data(company):
    query = {"theme": company}
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpact_collection = db.dailyThemeImpact
    all_company_impact = dailyThemeImpact_collection.find(query)
    df = pd.DataFrame()
    for item in all_company_impact:
        df = df.append({"date":item['date'],"impact":item['impact']},ignore_index=True)
    return df

def raw_plot(data,company):
    pyplot.plot(data['date'],data['impact'])
    pyplot.title("Trade War Impact_"+company)
    pyplot.show()

df = get_company_data("UOB")
raw_plot(df,"UOB")