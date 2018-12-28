import mysql.connector

def get_sql_con():
    try:
        mydb = mysql.connector.connect(
          host="pinalphafactset.coo4ogef0dwe.ap-southeast-1.rds.amazonaws.com",
          user="pinalpha",
          passwd="PinAlpha123",
          database = "pinalpha_mvp"
        )
        return mydb
    except:
        return None
