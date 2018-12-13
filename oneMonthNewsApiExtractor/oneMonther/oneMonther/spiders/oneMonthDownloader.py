# -*- coding: utf-8 -*-
import scrapy
from datetime import datetime, timedelta





oneMonthDates =  reversed([(datetime.now()- timedelta(days=el)).strftime("%Y-%m-%d") for el in range(31)])
comps = ["DBS", "OCBC", "Credit%20Suisse", "UOB", "Julius%20Baer", "UBS"]














class LambdaSpider(scrapy.Spider):
	name = 'lambda'
	allowed_domains = ['localhost:8000']
	start_urls = ['http://localhost:8000/']

	def start_requests(self):
		for company in comps:
			for day in oneMonthDates:
				yield scrapy.Request(self.start_urls[0] + 'news-insert-la?company=' + company + '&dateString=' + day)
				