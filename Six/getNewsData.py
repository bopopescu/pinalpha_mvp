import config.six_connect as six
import requests
import xmltodict

#public variables
client_id = 'SG33367'
username = 'pinalphatrial'
pwd = 'pinalpha'
six_access_token = six.get_session_id(client_id,username,pwd)
base_url = 'http://apidintegra.tkfweb.com/apid/request?id=%s&ci=%s&ui=%s-%s'%(six_access_token,client_id,client_id,username)


def get_news_articles(q,from_date,to_date):
    url = base_url+"method=getNewsData&search=%s&date_from=%s&date_to=%s&num=500&id=%s&ci=%s&ui=%s-%s&lang=4"%(q,from_date,to_date)
    html = requests.get(url)
    news_data = xmltodict.parse(html.content)['XRF']['NL']
    return news_data
