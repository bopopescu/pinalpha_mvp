import pymongo

def getDBCon():
  #get username password
  [username,pwd] = get_username_password()
  #call for mongoDB and get the client
  url = "mongodb+srv://%s:%s@cluster0-zuzix.mongodb.net/test"%(username,pwd)
  myclient = pymongo.MongoClient(url)
  return myclient


def get_username_password():
  up_list = list()
  #read username, password from config file
  up_list = ["pinalpha","PinAlpha123"]
  return up_list

