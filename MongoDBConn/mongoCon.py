import pymongo

def getDBCon():
  myclient = pymongo.MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
  #print(myclient.database_names())
  mydb = myclient.mydatabase
  return mydb


#print(getDBCon())