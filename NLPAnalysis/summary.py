from gensim.summarization.summarizer import summarize
import NLPAnalysis.simpleParsing as sp

def ExtractSummary(content):
    summary = ""
    try:
        summary =  summarize(content, word_count=50)
    except:
        summary = content
        print("Too short text to recognise key points.")
    return summary


def extract_sentences(content,company):
    list_sents = sp.get_sentences(content)




def clean_content(content):
    #cleaning the content
    return content
# print(ExtractSummary("Chegg The education and technology company offers both full-time and part-time employees up "
#                      "to $1,000 annually to help repay their student loans. There is no cap on the amount of student "
#                      "loan repayment that employees can receive. Like Fidelity, Chegg partnered with Tuition.io to "
#                      "administer the student loan repayment benefit. 9. Live Nation The live entertainment events "
#                      "promoter and venue operator offers its employees $100 per month to repay their student loans, "
#                      "with a total benefit of $6,000 in student loan repayment.. 10. Staples The retailer helps full-time "
#                      "sales associates repay their student loans with $1,200 per year (up to $3,600). What if your "
#                      "company doesn't offer student loan repayment assistance? There are other ways to tackle your "
#                      "student loans. Your best bet is to refinance student loans through student loan refinancing, "
#                      "which can lower your interest rate and help you pay off your student loans faster. You can also "
#                      "make extra payments - in addition to your minimum payment - to reduce your principal balance. "
#                      "Every dollar counts, even small amounts. This student loan prepayment calculator can show you "
#                      "how much you can save. While this employer list is by no means comprehensive (there are many "
#                      "more companies offering student loan repayment assistance), expect this benefit to spread to "
#                      "other companies that want to recruit and retain a loyal employee base. Its employee-centric. "
#                      "Its forward-thinking. Its solution-oriented.",50))