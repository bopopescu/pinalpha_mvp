import NLPAnalysis.simpleParsing as sp
import DBConn.mongoCon as mc

def write_content_file(contents,company,date):
    with open("./" + company + "_news_" + date + ".csv", 'w') as f:
        for item in contents:
            sentences = sp.get_sentences(item)
            for sent in sentences:
                f.write('"{}",{},{}'.format(sent, -1, 0))
                f.write('\n')
    f.close()

def get_content(articles):
    contents = []
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    newsAPIArticles_collection = db.newsAPIArticles  # collection
    for article in articles:
        searchQuery = {"url": article['url']}
        article_content = newsAPIArticles_collection.find(searchQuery)
        for item in article_content:
            content = item['content']
            contents.append(content)
    mongoCon.close()
    return contents

def get_data(searchTheme, startDate):
    searchQuery = {"$and":[{"date": startDate},{"search_theme_id":searchTheme}]}
    mongoCon = mc.getDBCon()  # connection
    db = mongoCon.production  # databse
    newsAPIArticles_collection = db.themeArticleMap  # collection
    news_article = newsAPIArticles_collection.find(searchQuery)
    mongoCon.close()
    return news_article  # this is a mongodb cursor


company = "OCBC"
date = "2018-12-20"
articles = get_data(company,date)
contents = get_content(articles)
write_content_file(contents,company,date)