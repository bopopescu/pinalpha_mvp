import DBConn.mysqlCon as mysqlcon
import DBConn.mongoCon as mc
import NLPAnalysis.simpleParsing as sp
import datetime

def get_keyWords():
    wordList = ["trade war","trade tension","china","singapore","malaysia","indonesia","thailand","taiwan","india",
                "philippines","vietnam","dubai","uae","south east asia","south asia","middle east","credit card fees",
                "wealth management","private banking","inflation","consumer spending","loan growth","loans",
                "trade finance","treasury markets","uob","ocbc","dbs","julius baer","credit suisse","ubs"]
    return wordList

def create_table_news_keywords():
    dbcon = mysqlcon.get_sql_con()
    cursor = dbcon.cursor()
    cursor.execute("CREATE TABLE pinalpha_news_keywords"
                   " (pinalpha_news_id VARCHAR(70), date DATETIME,"
                   "trade_war INT, trade_tension INT,china INT, singapore INT,"
                   "malaysia INT, indonesia INT, thailand INT, taiwan INT, india INT, philippines INT,"
                   "vietnam INT, dubai INT, uae INT, south_east_asia INT, south_asia INT, middle_east INT,"
                   "credit_card_fees INT, wealth_management INT, private_banking INT, inflation INT,"
                   "consumer_spending INT, loan_growth INT, loans INT, trade_finance INT,treasury_markets INT)")
    dbcon.close()

def get_all_news_for_day(date):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    newsAPIArticles_collection = db.newsAPIArticles  # collection
    query = {"publishedAt": {"$regex": date}}
    print(query)
    news_article = newsAPIArticles_collection.find(query)
    mongoCon.close()
    return news_article #this is a mongodb cursor

def process_news_list(newsArticles):
    for item in newsArticles:
        word_counts = get_news_analysis(item)
        insert_news_to_mysql(item,word_counts)

def get_news_analysis(newsItem):
    keyWord_list = get_keyWords()
    key_word_counts = {}
    for keyWord in keyWord_list:
        count = sp.get_occurances(newsItem['content'],keyWord)
        key_word_counts[keyWord] = count
    return key_word_counts

def insert_news_to_mysql(newsItem,counts):
    insertQuery = "INSERT INTO `pinalpha_mvp`.`pinalpha_news_keywords` (`pinalpha_news_id`, `date`, " \
                  "`trade_war`, `trade_tension`, `china`, `singapore`, `malaysia`, `indonesia`, `thailand`, " \
                  "`taiwan`, `india`, `philippines`, `vietnam`, `dubai`, `uae`, `south_east_asia`, `south_asia`, " \
                  "`middle_east`, `credit_card_fees`, `wealth_management`, `private_banking`, `inflation`, " \
                  "`consumer_spending`, `loan_growth`, `loans`, `trade_finance`, `treasury_markets`, `uob`, `ocbc`, " \
                  "`dbs`, `julius_baer`, `credit_suisse`, `ubs`) VALUES (%s);"
    insertvalues = '\"'+newsItem['id']+'\"'+","+'\"'+newsItem['publishedAt'][0:10]+'\"'+","
    insertvalues = insertvalues + ','.join(str(x) for x in list(counts.values()))
    insertQuery =  insertQuery % insertvalues
    print(insertQuery)
    try:
        dbcon = mysqlcon.get_sql_con()
        cursor = dbcon.cursor()
        cursor.execute(insertQuery)
        dbcon.commit()
        dbcon.close()
    except:
        print("DB connection, insert error")
    return None

def execute_main():
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    newsArticles = get_all_news_for_day(today)
    process_news_list(newsArticles)

execute_main()