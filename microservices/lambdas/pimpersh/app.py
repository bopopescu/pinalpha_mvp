from chalice import Chalice
import requests
from lxml import html
from newspaper import Config, Article
from random import choice
from json_tricks.np import dump, dumps
from multiprocessing import Process, Pool
import datetime
from pymongo import MongoClient
import ast
import json
import json
from datetime import datetime, timedelta
import sys
import html as krekml


app = Chalice(app_name='pimpersh')

app.debug = True
config = Config()



comps = ["DBS", "OCBC", "Credit%20Suisse", "UOB", "Julius%20Baer", "UBS"]
texturl = "https://6b3y2hqyn8.execute-api.us-west-2.amazonaws.com/api/scrape?masking=1&url="
preurl="http://api.scraperapi.com?key=ba5ace7791ef9891c05e8947f45f6f11&url="

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


sticompanies = [ 'E5H',
				 'D01',
				 'C07',
				 'D05',
				 'J37',
				 'O39',
				 'S68',
				 'U11',
				 'G13',
				 'T39',
				 'Y92',
				 'V03',
				 'F34',
				 'BS6',
				 'BN4',
				 'U96',
				 'S63',
				 'A17U',
				 'C61U',
				 'C31',
				 'C38U',
				 'C09',
				 'H78',
				 'U14',
				 'J36',
				 'S58',
				 'C52',
				 'NS8U',
				 'C6L',
				 'Z74']
 

sticurls = [preurl + "https://www.marketwatch.com/investing/stock/" + el + '/analystestimates' for el in sticompanies]




def stiget(url):
	response = html.fromstring(requests.get(url).content)
	index = sticurls.index(url)
	company = sticompanies[index]
	data = {el : extractor(el,response) for el in ['snapshot', 'estimates', 'ratings']}
	data['snapshot'] = {el : data['snapshot'][index+1] for index, el in enumerate(data['snapshot']) if index%2 == 0}
	data['estimates'] = {el : [data['estimates'][index+i] for i in range(1,5)] for index, el in enumerate(data['estimates']) if index%5 == 0}
	data['ratings'] = {el : [data['ratings'][index+i] for i in range(1,4)] for index, el in enumerate(data['ratings']) if index%4 == 0}

	eppudu = response.xpath('//div[@class="marketheader"]/p/text()')[1]
	entry = {'company': company, 'data': data, 'time': eppudu }

	return entry



def clean(lst):
	return [el.strip() for el in lst]


def extractor(element, response):
	return clean(response.xpath("//table[@class=" + '"' + element + '"' + "]/tbody/tr/td/text()"))



@app.route('/')
def index():
	return {'hello': 'world'}



@app.route('/scrape')
def scrape():
	request = app.current_request.query_params
	url = request['url']
	masking = request['masking']
	try:
		if int(masking):
			url = preurl + url
		config.browser_user_agent = choice(desktop_agents)
		article = Article(url)
		article.download()
		article.parse()
		text = article.text.replace('\n', ' ').replace(u"\u2018", "'").replace(u"\u2019", "'")

		return text



	except Exception as e:
		return "type error: " + str(e)





def articleText(url):
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






@app.route('/sti-scrape')
def stisscrape():
	urls = [preurl + "https://www.marketwatch.com/investing/stock/" + el + '/analystestimates' for el in sticompanies]
	pool = Pool(processes=10)
	pool_outputs = pool.map(stiget,
							urls)
	pool.close()
	pool.join()
	return pool_outputs

@app.route('/stii-scrape')
def stiisscrape():
	urls = [preurl + "https://www.marketwatch.com/investing/stock/" + el + '/analystestimates' for el in sticompanies]
	return [stiget(url) for url in urls]



@app.route('/mwatch-company')
def getCompany():
	try:
		request = app.current_request.query_params
		company = request['company']
		url = preurl + "https://www.marketwatch.com/investing/stock/" + company + '/analystestimates'
		response = html.fromstring(requests.get(url).content)
		data = {el : extractor(el,response) for el in ['snapshot', 'estimates', 'ratings']}
		data['snapshot'] = {el : data['snapshot'][index+1] for index, el in enumerate(data['snapshot']) if index%2 == 0}
		data['estimates'] = {el : [data['estimates'][index+i] for i in range(1,5)] for index, el in enumerate(data['estimates']) if index%5 == 0}
		data['ratings'] = {el : [data['ratings'][index+i] for i in range(1,4)] for index, el in enumerate(data['ratings']) if index%4 == 0}

		eppudu = response.xpath('//div[@class="marketheader"]/p/text()')[1]
		exchange = "sti" if company.upper() in sticompanies else "smi"
		entry = {'company': company, 'data': data, 'mtime': eppudu, "exchange": exchange, "stime": datetime.datetime.now().strftime("%d-%m-%Y") }

		return entry

	except Exception as e:
		return "Error what : " + str(e)



@app.route('/mwatch-company-insert')
def companyInsert():
	try:
		request = app.current_request.query_params
		company = request['company']
		myclient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		db = myclient['production']
		mwaCollection = db['mwatch']

		myquery = { "company": company, 'sdate': datetime.datetime.now().strftime("%d-%m-%Y")}
		mydoc = list(mwaCollection.find(myquery))

		if not mydoc:
			url = preurl + "https://www.marketwatch.com/investing/stock/" + company + '/analystestimates'
			response = html.fromstring(requests.get(url).content)
			data = {el : extractor(el,response) for el in ['snapshot', 'estimates', 'ratings']}
			data['snapshot'] = {el : data['snapshot'][index+1] for index, el in enumerate(data['snapshot']) if index%2 == 0}
			data['estimates'] = {el : [data['estimates'][index+i] for i in range(1,5)] for index, el in enumerate(data['estimates']) if index%5 == 0}
			data['ratings'] = {el : [data['ratings'][index+i] for i in range(1,4)] for index, el in enumerate(data['ratings']) if index%4 == 0}

			eppudu = response.xpath('//div[@class="marketheader"]/p/text()')[1]
			exchange = "sti" if company.upper() in sticompanies else "smi"
			entry = {'company': company, 'data': data, 'mtime': eppudu, "exchange": exchange, "sdate": datetime.datetime.now().strftime("%d-%m-%Y") }


			
			mwaCollection.insert_one(entry)
			myclient.close()

		else:
			myclient.close()
			return "Exists already"


	except Exception as e:
		return "Error what : " + str(e)




@app.route('/mwatch-company-fetch')
def companyFetch():
	try:
		request = app.current_request.query_params
		company = request['company']
		myclient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		db = myclient['production']
		mwaCollection = db['mwatch']

		myquery = { "company": company, 'sdate': datetime.datetime.now().strftime("%d-%m-%Y")}
		mydoc = list(mwaCollection.find(myquery))

		if mydoc:
			return mydoc[0]


	except Exception as e:
		return "Error what : " + str(e)



def findNumArticles(query, dateString):
	count = requests.get("https://newsapi.org/v2/everything?q=" + query +
				   "&apiKey=00e887a151f345c68dc57a1c19526283&language=en&from=" + dateString + "&to=" + dateString).json()['totalResults']
	return int(count)
	


def getNews(query, pageNum, dateString):
	return requests.get("https://newsapi.org/v2/everything?q=" + query + "&page=" + str(pageNum) + "&pageSize=100" +
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
	

	
	
@app.route('/news-api-update-daily-testing-squared')	
def dailyNewsApiUpdate():
	myclient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
	pdb = myclient['production']
	streamNewsCollection = pdb['NewsApiStream']
	dateString = (datetime.now()- timedelta(days=2)).strftime("%Y-%m-%d")
	for query in comps:
		numberOfArticles = findNumArticles(query, dateString)
		if numberOfArticles%100:
			numPages = numberOfArticles//100
		else:
			numPages = numberOfArticles//100 + 1
			
		for pageNum in range(1, numPages+1):
			pageData = getNews(query, pageNum, dateString)
			attrs = ['url', 'description', 'publishedAt', 'source', 'title']
			articles = pageData['articles']
			ddata = []
			for article in articles:
				adata = {}
				for attr in attrs:
					if attr in article.keys():
						adata[attr] = krekml.unescape(article[attr])
					else:
						adata[attr] = '#'
				ddata.append(adata)

			print(ddata)
			# x = streamNewsCollection.insert_many(ddata)
			
			
	myclient.close()
	# return ddata


@app.route('/news-api-update-daily-testing')
def dailyNewsApiUpdateTesting():
	attrs = ['url', 'description', 'publishedAt', 'source', 'title']
	try:
		myclient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = myclient['production']
		streamNewsCollectionTesting = pdb['newsApiStreamTesting']
		dateString = (datetime.now()- timedelta(days=2)).strftime("%Y-%m-%d")
		try:
			for query in comps:
				numberOfArticles = findNumArticles(query, dateString)
				print(query, numberOfArticles)
				if numberOfArticles%100==0:
					numPages = numberOfArticles//100
				else:
					numPages = numberOfArticles//100 + 1
					
				for pageNum in range(1, numPages+1):
					pageData = getNews(query, pageNum, dateString)
					
					articles = pageData['articles']
					ddata = []
					for article in articles:
						adata = {}
						for attr in attrs:
							if attr in article.keys():
								adata[attr] = krekml.unescape(article[attr])
							else:
								adata[attr] = '#'
						ddata.append(adata)

					# x = streamNewsCollectionTesting.insert_many(ddata)

					
					
			myclient.close()
			return 0
		except Exception as e:
			myclient.close()
			#e = sys.exc_info()
			print(e)
			return 1


	except:
		e = sys.exc_info()[0]
		myclient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
		pdb = myclient['production']
		logCollectionTesting = pdb['logsTesting']
		y = logCollectionTesting.insert_one({'error': e})




		

			
		
	
def structuredData(query, dateString, pageNum):
	pageData = getNews(query, pageNum, dateString)
	attrs = ['url', 'description', 'publishedAt', 'source', 'title']
	articles = pageData['articles']
	ddata = []
	for article in articles:
		adata = {}
		for attr in attrs:
			if attr in article.keys():
				adata[attr] = krekml.unescape(article[attr])
			else:
				adata[attr] = '#'
		ddata.append(adata)

	myclient = MongoClient("mongodb+srv://pinalpha:PinAlpha123@cluster0-zuzix.mongodb.net/test")
	pdb = myclient['production']
	newsCollection = pdb['newsApi']
	x = newsCollection.insert_many(ddata)
	myclient.close()
	return x
				
	

def getUrls(response):
	return [el['url'].split('?')[0] for el in response['articles']]



	



@app.route('/news-api-update-daily')
def newsApiUpdateDaily():
	pass



