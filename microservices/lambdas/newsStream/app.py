from chalice import Chalice
import requests
from lxml import html
from newspaper import Config, Article
from random import choice
from json_tricks.np import dump, dumps
import datetime
from pymongo import MongoClient
import ast
import json
from datetime import datetime, timedelta
import sys
import html as krekml
from pymongo.errors import DuplicateKeyError
import traceback
import time



app = Chalice(app_name='newsStream')
app.debug = True






config = Config()


comps = ["DBS", "OCBC", "Credit%20Suisse", "UOB", "Julius%20Baer", "UBS"]
attrs = ['url', 'description', 'publishedAt', 'source', 'title']
preurl="http://api.scraperapi.com?key=ba5ace7791ef9891c05e8947f45f6f11&url="
texturl = "https://6b3y2hqyn8.execute-api.us-west-2.amazonaws.com/api/scrape?masking=1&url="



desktop_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
				 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
				 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
				 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
				 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
				 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
				 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
				 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
				 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
				 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0']



@app.route('/')
def index():
	return {'hello': 'world'}





def findNumPages(query,  dateString):
	count = requests.get("https://newsapi.org/v2/everything?q=" + query +
						"&apiKey=00e887a151f345c68dc57a1c19526283&language=en&from=" + dateString + "&to=" + dateString).json()['totalResults']
	return int(count)
	


def getNews(query, pageNum, dateString):
	return requests.get("https://newsapi.org/v2/everything?q=" + query + "&page=" + str(pageNum)  +
						"&apiKey=00e887a151f345c68dc57a1c19526283&language=en&from=" + dateString + "&to=" + dateString).json()



def getText(url):
	result = requests.get(texturl + url.split('?')[0]).content.decode("utf-8")
	if result.startswith("type error:"):
		result = requests.get(texturl + url.split('?')[0]).content.decode("utf-8")
		if result.startswith("type error:"):
			result = requests.get(texturl + url.split('?')[0]).content.decode("utf-8")
			return result
		else:
			return result
	else: 
		return result

	

def getTextLocal(url):
	try:
		url = preurl + url
		config.browser_user_agent = choice(desktop_agents)
		article = Article(url)
		article.download()
		article.parse()
		text = krekml.unescape(article.text)

		return text



	except Exception as e:
		return "type error: " + str(e)


@app.route('/get-article')
def getArticle():
	start = time.time()
	try:
		url = app.current_request.query_params['url']
		url = preurl + url
		config.browser_user_agent = choice(desktop_agents)
		article = Article(url)
		article.download()
		article.parse()
		text = krekml.unescape(article.text)

		return time.time()-start, text



	except Exception as e:
		return "type error: " + str(e)

def getUrls(response):
	return [el['url'].split('?')[0] for el in response['articles']]



@app.route('/company-news')
def getCompanyNews():
	request = app.current_request.query_params
	company = request['company']
	fromDateString = request['fromDateString']
	toDateString = request['toDateString']
	pageNum = request['pageNum']

	return requests.get("https://newsapi.org/v2/everything?q=" + company + "&page=" + str(pageNum) + "&pageSize=100" +
						"&apiKey=00e887a151f345c68dc57a1c19526283&language=en&from=" + fromDateString + "&to=" + toDateString).json()





@app.route('/news-stream-db-insert-one-entry')
def insertEntry():
	request = app.current_request.query_params
	company = request['company'].lower()
	dateString = request['dateString'].lower()
	url = request['url'].lower()

	text = getTextLocal(url)
	try:
		myclient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = myclient['production']
		streamNewsCollection = pdb['allNews']
		index_name = 'url'
		if 'url_1' in streamNewsCollection.index_information():
			pass
		else:
			streamNewsCollection.create_index(index_name, unique=True)


		try:
			x = streamNewsCollection.insert_one({'url' : url, 
												 'companies' : [company],
												 'article' : text,
												 'date' : dateString})

			return "New Article added to DB"


		except DuplicateKeyError:
			dups = streamNewsCollection.find_one({"url" : url})
			if company not in dups['companies']:
				dups['companies'].append(company)
				streamNewsCollection.update_one({'url' : url}, {"$set": dups}, upsert=False)


				return "New Company added to an Existing Article"

			else:
				return "Same URL and Company requested again"

		except:
			ermsg = traceback.format_exc()
			logClient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
			pdb = logClient['production']
			logsCollection = pdb['logger']

			logsCollection.insert_one({'info' : ermsg,
									   'origin' : 'NewsStream.InsertEntry.Inner',
									   'what' : 'Insertion Error'  })
			logClient.close()
			return "InnerInsertFailure. Logged Successfully"


	except:
		ermsg = traceback.format_exc()
		logClient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = logClient['production']
		logsCollection = pdb['logger']

		logsCollection.insert_one({'info' : ermsg,
								   'origin' : 'NewsStream.InsertEntry.Outer',
								   'what' : 'Insertion Error'  })
		logClient.close()
		return "OuterInsertFailure. Logged Successfully"





@app.route('/news-stream-db-insert')
def justdoit():
	request = app.current_request.query_params
	company = request['company']
	dateString = request['dateString']
	try:
		myclient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = myclient['production']
		streamNewsCollection = pdb['newsStreamer']
		index_name = 'url'
		if 'url_1' in streamNewsCollection.index_information():
			pass
		else:
			streamNewsCollection.create_index(index_name, unique=True)
	except:
		ermsg = traceback.format_exc()
		myclient1 = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = myclient['production']
		logsCollection = pdb['logger']
		
		logsCollection.insert_one({'state' : 0,
								   'info' : ermsg,
								   'origin' : 'NewsScraper.JustDoIt-0',
								   'what' : 'DB Connection Error'  })
		myclient1.close()
		return "Failure1"
		
		
	try:
		numberOfArticles = findNumPages(company,  dateString)
		numPages = numberOfArticles//100
		if numberOfArticles%100 != 0:
			numPages += 1

		for pageNum in range(1,numPages+1):
			compUrls = getUrls(getNews(company, pageNum, dateString))
			for index,url in enumerate(compUrls):
				data = getTextLocal(url)
				try:
					x = streamNewsCollection.insert_one({'url' : url, 
														 'companies' : [company],
														 'article' : data,
														 'date' : dateString})

				except DuplicateKeyError:
					dups = streamNewsCollection.find_one({"url" : url})
					if company not in dups['companies']:
						dups['companies'].append(company)
						streamNewsCollection.update_one({'url' : url}, {"$set": dups}, upsert=False)
						


				except:
					ermsg = traceback.format_exc()
					myclient2 = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
					pdb = myclient['production']
					logsCollection = pdb['logger']

					logsCollection.insert_one({'state' : 2,
											   'info' : ermsg,
											   'origin' : 'NewsScraper.JustDoIt-2',
											   'what' : 'DB Insertion Error'  })
					myclient2.close()
					return "Failure2"




		myclient.close()
		return "Success!"
			
			
	except:
		ermsg = traceback.format_exc()
		myclient3 = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = myclient['production']
		logsCollection = pdb['logger']
		
		logsCollection.insert_one({'state' : 1,
								   'info' : ermsg,
								   'origin' : 'NewsScraper.JustDoIt-1',
								   'what' : 'News API Error'  })
		myclient3.close()
		try:
			myclient.close()
		except:
			pass

		return "Failure3"












@app.route('/news-insert-la')
def serverjustdoit():
	request = app.current_request.query_params
	company = request['company']
	dateString = request['dateString']


	try:
		myclient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = myclient['production']
		streamNewsCollection = pdb['allNews']
		index_name = 'url'
		if 'url_1' in streamNewsCollection.index_information():
			pass
		else:
			streamNewsCollection.create_index(index_name, unique=True)
	except:
		ermsg = traceback.format_exc()
		myclient1 = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = myclient['production']
		logsCollection = pdb['logger']
		
		logsCollection.insert_one({'info' : ermsg,
								   'origin' : 'NewsScraper.JustDoIt-0',
								   'what' : 'DB Connection Error'  })
		myclient1.close()
		return "Failure1"
		
		
	try:
		numberOfArticles = findNumPages(company,  dateString)
		numPages = numberOfArticles//100
		if numberOfArticles%100 != 0:
			numPages += 1

		for pageNum in range(1,numPages+1):
			compUrls = getUrls(getNews(company, pageNum, dateString))
			for index,url in enumerate(compUrls):
				data = getTextLocal(url)
				try:
					x = streamNewsCollection.insert_one({'url' : url, 
														 'companies' : [company],
														 'article' : data,
														 'date' : dateString})

				except DuplicateKeyError:
					dups = streamNewsCollection.find_one({"url" : url})
					if company not in dups['companies']:
						dups['companies'].append(company)
						streamNewsCollection.update_one({'url' : url}, {"$set": dups}, upsert=False)
						


				except:
					ermsg = traceback.format_exc()
					myclient2 = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
					pdb = myclient['production']
					logsCollection = pdb['logger']

					logsCollection.insert_one({'info' : ermsg,
											   'origin' : 'NewsScraper.JustDoIt-2',
											   'what' : 'DB Insertion Error'  })
					myclient2.close()
					return "Failure2"




		myclient.close()
		return "Success!"
			
			
	except:
		ermsg = traceback.format_exc()
		myclient3 = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = myclient['production']
		logsCollection = pdb['logger']
		
		logsCollection.insert_one({'info' : ermsg,
								   'origin' : 'NewsScraper.JustDoIt-1',
								   'what' : 'News API Error'  })
		myclient3.close()
		try:
			myclient.close()
		except:
			pass

		return "Failure3"






