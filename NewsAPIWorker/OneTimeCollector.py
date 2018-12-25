from newsapi import NewsApiClient
import DataAccess.data_access as da
from datetime import date, datetime, timedelta
import requests
import json

def populate_dates(start,end,delta):
    date_list = []
    curr = start
    while curr < end:
        date_list.append(curr)
        curr += delta
    return date_list


def get_urls(start,end,keyphrase):
    baseUrl = "https://newsapi.org/v2/everything?q=%s&apiKey=00e887a151f345c68dc57a1c19526283&language=en&pageSize=100&from=%s&to=%s"
    Url = baseUrl % (keyphrase,start,end)
    return Url


def collect_news(url):
    response = requests.get(url)
    jsonResult = response.json()
    return jsonResult



