import DBConn.mongoCon as mc

def delete_duplicates():
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production
    db.themeSentimentArticlesMap.ensure_index({ "news_id":1, "theme":1 }, { "unique":"true", "dropDups":"true" } )
    mongoCon.close()

delete_duplicates()