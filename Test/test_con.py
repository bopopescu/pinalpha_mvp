import MongoDBConn.mongoCon as mc

def test_connection():
    print(mc.getDBCon())

def test_database():
    conn = mc.getDBCon()
    print(conn.database_names())

test_database()