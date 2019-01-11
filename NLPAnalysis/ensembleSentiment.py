import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from textblob import TextBlob
from collections import Counter

nltk.download('vader_lexicon')


def vader_analyzer(text):
    vader = SentimentIntensityAnalyzer()
    sentiment1 = vader.polarity_scores(text)
    compound_score = sentiment1['compound']
    analysis = 'positive' if compound_score > 0.05 else 'negative' if compound_score < -0.05 else 'neutral'
    return analysis, sentiment1


def textblob_analyzer(text):
    document = TextBlob(text)
    compound_score = document.sentiment.polarity
    analysis = 'positive' if compound_score > 0.05 else 'negative' if compound_score < -0.05 else 'neutral'
    return analysis, compound_score


def google_analyzer(text):
    client = language.LanguageServiceClient.from_service_account_json(r'PinAlpha-e51efab74e05.json')

    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT,
        language='en')
    annotations = client.analyze_sentiment(document=document)
    score = annotations.document_sentiment.score
    # magnitude = annotations.document_sentiment.magnitude
    analysis = 'positive' if score > 0 else 'negative' if score < 0 else 'neutral'
    return analysis, score


def overall_sentiment(sentence):
    list = []
    vader_result, vader_score = vader_analyzer(sentence)
    list.append(vader_result)
    textblob_result, textblob_score = textblob_analyzer(sentence)
    list.append(textblob_result)
    google_result, google_score = google_analyzer(sentence)
    list.append(google_result)
    count = Counter(list)
    print(list)
    if count.most_common(1)[0][1] == 3:
        return count.most_common(1)[0][0], False, vader_score, textblob_score, google_score
    elif count.most_common(1)[0][1] == 2:
        return count.most_common(1)[0][0], True, vader_score, textblob_score, google_score
    else:
        return 'Split', True, vader_score, textblob_score, google_score