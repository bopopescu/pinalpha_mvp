import DBConn.mongoCon as mc

def testQuery():
    findQuery = {"$or":[{"news_id":"c125c6c1c65249e0959c463d4b4d40f8"},{"news_id":"fa374d6c476346eb84f92438d4f873a1"}]}
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production
    SentCollection = db.newsSentenceSentiments
    result = SentCollection.find(findQuery)
    for item in result:
        print(item['news_id']+" : "+item['sentence'])
    mongoCon.close()

testQuery()