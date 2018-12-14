# -*- coding: utf-8 -*-
import scrapy
from datetime import datetime, timedelta





oneMonthDates =  [datetime.now().strftime("%Y-%m-%d")]
comps = ["DBS", "OCBC", "Credit%20Suisse", "UOB", "Julius%20Baer", "UBS"]


class DailyscraperSpider(scrapy.Spider):
	name = 'dailyscraper'
	allowed_domains = ['localhost:8000']
	start_urls = ['http://localhost:8000/']

	def start_requests(self):
		for company in comps:
			for day in oneMonthDates:
				yield scrapy.Request(self.start_urls[0] + 'news-insert-la?company=' + company + '&dateString=' + day)
				
