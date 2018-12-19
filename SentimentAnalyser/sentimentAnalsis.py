import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
nltk.download('vader_lexicon')

def get_sentiment_of_article(article):
    sentiment_scores = list()
    #call for a sentiment analysis, article can be one or mroe sentence
    sia = SIA()
    sentiment_scores = sia.polarity_scores(article.lower())
    #store the sentiment score in MongoDB for this article if it is not there,
    # else return the existing sentiment score
    return sentiment_scores["compound"] #only get the compound score

def get_overall_sentiment_of_df(df):
    overall_sentiment = list()
    #get sentiment for each article and calculate overall sentiment
    return overall_sentiment

def get_emotion_of_article(article):
    emotion_scores = list()
    #call for emotion score
    return emotion_scores

def get_overall_emotion_of_df(df):
    overall_emotion = list()
    #get sentiment for each article and calculate overall sentiment
    return overall_emotion