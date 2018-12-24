import nltk

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

def ExtractKeyPhrases(Text):
    TextFile = "./temp.txt"
    f = open(TextFile,'w')
    f.write(Text)
    f.close()
    # initialize keyphrase extraction model, here TopicRank
    extractor = pke.unsupervised.TopicRank()
    extractor.load_document(input=TextFile, language='en')
    extractor.candidate_selection()
    # candidate weighting, in the case of TopicRank: using a random walk algorithm
    extractor.candidate_weighting()
    keyphrases = extractor.get_n_best(n=4, stemming=False)
    return ([i[0] for i in keyphrases])

def sentence_removed_stopped_word(sentence):
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(sentence)
    filtered_sentence = [w for w in word_tokens if not w in stop_words]
    final_sentence = " ".join(filtered_sentence)
    return final_sentence