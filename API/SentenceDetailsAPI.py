import DBConn.mongoCon as mc
import pandas as pd
import requests
import NLPAnalysis.summary as summary
from datetime import date, datetime, timedelta
import NewsAPIWorker.OneTimeCollector as oneTimer

def get_sentences_based_sentiment(list_news):
    query = {"news_id":{"$in":list_news}}
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    sentenceSentiment_collection = db.newsSentenceSentiments
    all_sentences = sentenceSentiment_collection.find(query)
    sentence_df = pd.DataFrame()
    for sentence in all_sentences:
        sentence_df = sentence_df.append({"sentence":sentence['sentence'],"sentiment":sentence['sentiment'],"score":sentence['google_score']}, ignore_index=True)
    mongoCon.close()
    return sentence_df

def get_sentence_summary(list_news):
    summary_list = {}
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    newsArticle_collection = db.newsAPIArticles
    for id in list_news:
        query = {"id":id}
        all_articles = newsArticle_collection.find(query)
        for artilce in all_articles:
            content = artilce['content']
            article_summary = summary.ExtractSummary(content)
            summary_list[id] = article_summary
    mongoCon.close()
    return summary_list


def get_news_on_themes_date(theme,date,impact):
    url = "https://pinalphaanalytics.com:8082/api/v1.0/theme/%s/sentiment/%s/%s"%(theme,impact,date)
    id_list = []
    response = requests.get(url).json()
    for item in response:
        id_list.append(item['id'])
    print(id_list)
    return id_list

def clean_sentence(sentence):
    wordList = pd.read_csv("../Data/bag_of_words_for_cleaning.csv")
    for idx,item in wordList.iterrows():
        word = item['WordsToRemove']
        sentence = sentence.replace(word,'')
    sentence = sentence.strip()
    wordCount = len(sentence.split())
    if not wordCount < 4:
        return sentence.strip()
    else:
        return ""

def clean_sentences(summary_list):
    cleaned_list = {}
    for item in summary_list.items():
        #print(item)
        cleaned_sent = clean_sentence(item[1])
        cleaned_list[item[0]] = cleaned_sent
    return cleaned_list


def get_sentences(theme,date,impact):
    news_ids = get_news_on_themes_date(theme,date,impact)
    summary_list = get_sentence_summary(news_ids)
    cleaned_summary = clean_sentences(summary_list)
    return cleaned_summary

def check_if_sentence_exist(db,findQuery):
    try:
        len_news = db.dailyThemeSentence.find(findQuery).count()
        print(len_news)
        if len_news != 0:
            return True
        else:
            return False
    except:
        print("Error with MongoDB Search")
        return False

def insert_mongo(sents,theme,date):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeSentence_collection = db.dailyThemeSentence
    for sent in sents.items():
        findQuery = {"sentences":sent[1]}
        print(findQuery)
        sents_exist = check_if_sentence_exist(db,findQuery)
        if sents_exist:
            print("Sentence Exists")
        else:
            query = {"news_id": sent[0], "sentences": sent[1], "theme": theme, "date": date}
            try:
                dailyThemeSentence_collection.insert(query)
                print("Insert Done")
            except:
                print("Insert Error for Sentences")
    mongoCon.close()
    return True


def get_impact_of_theme_date(theme,date):
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # database
    dailyThemeImpactIntermediate_collection = db.dailyThemeImpactIntermediate
    query = {"theme":theme,"date":date}
    response = dailyThemeImpactIntermediate_collection.find(query)
    sentimentVal = "Negative"
    for item in response:
        if item['impact']<0:
            sentimentVal = "Negative"
        else:
            sentimentVal = "Positive"
    return sentimentVal

def initiate(theme,date):
    #sentimentVal = get_impact_of_theme_date(theme,date)
    sents = get_sentences(theme, date,"positive")
    insert_mongo(sents,theme,date)
    sents = get_sentences(theme, date, "negative")
    insert_mongo(sents, theme, date)


def execute_main():
    today = datetime.now()
    themeList = ["trade_war","wealth_management","loan_growth"]
    for theme in themeList:
        initiate(theme, today.strftime("%Y-%m-%d"))

execute_main()

