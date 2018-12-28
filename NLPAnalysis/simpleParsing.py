from collections import Counter
import nltk

def split_to_words(article):
    if not article == None:
        word_tokens = nltk.word_tokenize(article)
        return word_tokens
    else:
        return []

def getWordCount(wordlist):
    return Counter(wordlist)

def get_occurances(article,phrase):
    if not article == None:
        phrase = " "+phrase
        return article.lower().count(phrase)
    else:
        return 0

def get_sentences(article):
    sentence_list = []
    sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')
    if not article == None:
        sentence_list = sent_detector.tokenize(article.strip())
    return sentence_list

