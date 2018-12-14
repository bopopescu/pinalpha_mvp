import DataAccess.data_access as da

def get_company_sentiment(company):

    #call for today's news data
    da.get_today_news_data(company)
    #