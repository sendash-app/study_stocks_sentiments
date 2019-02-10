# 1. Data Cleaning
import re

# 2. Summarization
from summa import summarizer

from sumy.summarizers.lsa import LsaSummarizer
from sumy.nlp.stemmers import Stemmer
from sumy.nlp.tokenizers import Tokenizer
from sumy.utils import get_stop_words
from sumy.parsers.plaintext import PlaintextParser

# 3. Relavency Score
import spacy

Stock_KW_Dict = {
    'FB' : [
        'FB', 'Facebook', 'Mark Zuckerberg', 'Zuckerberg',
        'Sheryl Sandberg', 'Sandberg',
        'Facebook Messenger', 'Messenger',
        'Instagram','WhatsApp','Oculus VR', 'Oculus'
    ],
    'AMZN' : [
            'AMZN', 'Amazon', 'Jeff Bezos', 'Bezos',
            'AWS', 'Amazon Web Services',
            'Amazon Kindle', 'Kindle',
            'Amazon Echo','Echo',
            'Amazon Prime', 'Prime',
            'Alexa',
            'Whole Foods Market', 'Whole Foods',
            'Audible', 'Amazon Studios', 'Goodreads', 'Woot', 'Zappos'
    ],
    'NFLX': [
        'NFLX', 'Netflix', 'Reed Hastings', 'Hastings',
        'Ted Sarandos', 'Sarandos',
        'Netflix Studio', 'ABQ Studio', 'Millarworld', 'DVD.com', 'LT-LA',
        'Netflix Streaming Services'
    ],
    'GOOGL': [
        'GOOGL', 'GOOG', 'Google', 'Google Inc', 'Alphabet Inc', 'Alphabet',
        'Larry Page', 'Sergey Brin', 'Brin',
        'John Hennessy', 'David Drummond', 'Drummond', 'Hennessy',
        'Sundar Pichai', 'Pichai', 'Ruth Porat', 'Porat',
        'Google AdSense', 'AdSense', 'Google Ads', 'DoubleClick', 'AdWords',
        'Google Chrome', 'Chrome', 'Google Play', 'YouTube', 'Nexus', 'Pixel',
        'Chromebook', 'Chromecast', 'Google Home', 'Google Cloud', 'Google Cloud Platform',
        'Google Pixel', 'Google VR', 'Android', 'Google Deep Mind', 'Deep Mind',
        'Google Fiber'
    ]
}

# 4. Sentiment
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from nltk.sentiment.vader import SentimentIntensityAnalyzer as nltk_SIA
from nltk import sent_tokenize



def GetCleanText(inText):
    outText = re.sub(r"(\n)|(\r)|('\')", '', inText)
    outText = outText.replace(u'\xa0',' ').encode('utf-8')
    outText = outText.decode('utf-8')

    return outText

def GetSummary(inText, method = 'summa'):
    if method == 'sumy':
        lang = 'english'
        sent_count = 5

        parser = PlaintextParser.from_string(inText, Tokenizer(lang))
        LsaSum = LsaSummarizer( Stemmer(lang))
        LsaSum.stop_words = get_stop_words(lang)

        summary_sumy = LsaSum(parser.document, sent_count)
        return summary_sumy

    elif method == 'summa':
        summary_summa = summarizer.summarize(inText, ratio = 0.3)
        summary_summa
    else:
        print(f'{method} is not defined')
        return None

def GetRelavency(inText, kwList):
    nlp = spacy.load('en_core_web_sm')
    doc = nlp(inText)

    rSource = 0
    for ent in doc.ents:
        if ent.text.lower() in [kw.lower() for kw in kwList]:
            rSource += 1

    return rSource

def GetSentimentScore(inText, method = 'bespoke'):
    if method == 'bespoke':
        SIA = SentimentIntensityAnalyzer()
    else:
        SIA = nltk_SIA()

    sent_text = sent_tokenize( summary_summa)

    sent_count = 0
    sent_sum = 0
    for sent in sent_text:
        sent_sum += SIA.polarity_scores(sent)['compound']
        sent_count += 1

    SIA_Score = sent_sum/ sent_count
    return SIA_Score
