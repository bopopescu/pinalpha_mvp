from chalice import Chalice
import requests
from lxml import html
from newspaper import Config, Article
from random import choice
from json_tricks.np import dump, dumps
from multiprocessing import Process, Pool
import datetime
from pymongo import MongoClient




app = Chalice(app_name='pimbda')

app.debug = True
config = Config()



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





@app.route('/news-api-update-daily')
def newsApiUpdateDaily():
	pass



