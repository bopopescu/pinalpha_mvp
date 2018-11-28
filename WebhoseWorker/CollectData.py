import webhoseio
import config.webhose_api_access as web
import pandas as pd
import MongoDBConn.mongoCon as mongo_db_con
import json

def getToken():
    api_token = web.getWebhoseAPI()
    return api_token

def setQueryParams(org,lang,ppl,loc):
    query = ''
    if(org):
        query = query + 'organization:'+org+' '
    if(lang):
        query = query + 'language:' + lang + ' '
    if(ppl):
        query = query + 'person:' + ppl + ' '
    if(loc):
        query = query + 'location:' + ppl + ' '

    query_params = {
        "q": query.strip(),
        "sort": "crawled",
        "ts": "1539847402216"
    }
    return query_params

def insertToDB(data_to_insert):
    db = mongo_db_con.getDBCon()
    webhose_collection = db["webhose"]
    x = webhose_collection.insert_many(data_to_insert)
    #print(x.inserted_ids)

def getContent(query_params):
    output = webhoseio.query("filterWebContent", query_params)
    print(output)
    with open("./webhose_results.json", 'w') as outfile:
        json.dump(output, outfile, sort_keys=True)

    insertToDB(output["posts"])
    ReqNumber = 1
    while(output["moreResultsAvailable"]):
        output = webhoseio.get_next()
        # do something for subsequent query results
        with open("./webhose_results_"+str(ReqNumber)+".json", 'w') as outfile:
            json.dump(output, outfile, sort_keys=True)
        insertToDB(output["posts"])
        ReqNumber = ReqNumber +1
        if(ReqNumber>=5):
            break


def getStockList(filename):
    df_stock_list = pd.read_csv(filename)
    return df_stock_list

webhoseio.config(token=getToken())

df_companylist = pd.read_csv("/home/kasun/Documents/CompanyList.csv")
for name in df_companylist['CompanyName']:
    name = name.strip()
    name = name.replace(" ","%20")
    Qparams = setQueryParams(name, 'english', '', '')
    print(Qparams)
    getContent(Qparams)