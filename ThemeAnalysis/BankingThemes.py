import DataAccess as da
import pandas as pd
import SentimentAnalyser.sentimentAnalsis as SA

#read updates themes for the given companies
def get_themes_from_db(company):
    list_of_themes = list()
    #read all the themes for given company
    return list_of_themes

def get_articles_with_theme(df,theme):
    df_theme = pd.DataFrame()
    #filter the articles with themes
    return df_theme

def perform_analysis(df,company): #df is for all the articles for today
    #this function get the related articles for given company, given theme
    # and calculate the overall impact
    df_themes = get_themes_from_db(company)
    for theme in df_themes:
        df_theme_articles = get_articles_with_theme(df,theme)
        impact = SA.get_overall_sentiment_of_df(df)#for now only calculate the Sentiment
         #store the current analysis on MongoDB with (CompanyID,Date,Impact,Articles [ArticleID,Impact])
    return impact