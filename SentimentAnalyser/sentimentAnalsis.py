import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from nltk import sent_tokenize
import uuid
import SentimentAnalyser.keyPhraseParsing as kp

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

def get_Sentences(article,articleId,date,companyName):
    sent_list = []
    if(article!=None):
        list_sentences = sent_tokenize(article)
        for sent in list_sentences:
            sent_dict = {}
            id = uuid.uuid4().hex
            sent_dict["id"]=id
            sent_dict["articleID"]= articleId
            sent_dict["date"]=date
            sent_dict["company"]=companyName
            sent_dict["sentence"] = sent
            sent_dict["impact"] = get_sentiment_of_article(sent)
            sent_dict["shortened"] = kp.sentence_removed_stopped_word(sent)
            sent_list.append(sent_dict)
    else:
        print(article)
    return sent_list