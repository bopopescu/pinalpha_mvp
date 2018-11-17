import pymongo

def getDBCon():
  myclient = pymongo.MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
  mydb = myclient.mydatabase
  return mydb



# #print(mydb.cool_collection.count())
# mycol = mydb["customers"]
#
# mylist = [
#   { "name": "Amy", "address": "Apple st 652"},
#   { "name": "Hannah", "address": "Mountain 21"},
#   { "name": "Michael", "address": "Valley 345"},
#   { "name": "Sandy", "address": "Ocean blvd 2"},
#   { "name": "Betty", "address": "Green Grass 1"},
#   { "name": "Richard", "address": "Sky st 331"},
#   { "name": "Susan", "address": "One way 98"},
#   { "name": "Vicky", "address": "Yellow Garden 2"},
#   { "name": "Ben", "address": "Park Lane 38"},
#   { "name": "William", "address": "Central st 954"},
#   { "name": "Chuck", "address": "Main Road 989"},
#   { "name": "Viola", "address": "Sideway 1633"}
# ]
#
# x = mycol.insert_many(mylist)
#
# #print list of the _id values of the inserted documents:
# print(x.inserted_ids)