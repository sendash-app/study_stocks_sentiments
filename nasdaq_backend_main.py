import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
from datetime import timedelta
from unidecode import unidecode
from datetime import datetime as dt
import schedule
import time
from textblob import TextBlob
import nltk
nltk.download('punkt')
from nltk.tokenize import sent_tokenize


#database
conn = sqlite3.connect('FANG_sentiment.db')
c = conn.cursor()

def create_table():
    
    try:
        c.execute("CREATE TABLE IF NOT EXISTS sendash (date TEXT , time TEXT, weekday REAL , headline TEXT, content TEXT, headline_vader REAL, extracted_content TEXT, ex_content_textblob REAL , stockcode TEXT)")
        conn.commit()

    except Exception as e:
        print(str(e))

create_table()
print('database is ready!')

# Vader Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()
print('sentiment analyzer is ready!')

# scrape sentiment data
def scrape_nasdaq(stockcode, last_page_num=10):
    """
    Stockcode: FB/NFLX/AMZN/GOOGL
    """
    page_num = 0
    results  = []
    while True:
        if page_num > last_page_num: # Last page
            break
        html = requests.get(f'https://www.nasdaq.com/symbol/{stockcode}/news-headlines?page={page_num}')
        soup = BeautifulSoup(html.text, "html.parser")
        
        for divs in soup.find_all(class_ = 'news-headlines'):
            headline = [s.text for s in divs.find_all('a', {'target':"_self"})]
            headline_vader = [analyzer.polarity_scores(unidecode(str(text)))['compound'] for text in headline]
            pub_date = [re.sub(r'\-.+','',d.text.strip()).strip() for d in divs.find_all('small')]
            pub_date = pd.Series(pub_date).apply(lambda x: str(dt.strptime(x, '%m/%d/%Y %I:%M:%S %p') - timedelta(hours=5))).values
            date     = [re.findall(r'\d.+\s', dat)[0].strip() for dat in pub_date]
            time     = [re.findall(r'\s.+', tim)[0].strip() for tim in pub_date]
            weekday  = [dt.strptime(day, '%Y-%m-%d %H:%M:%S').weekday() for day in pub_date]
            urls     = [s.get('href') for s in divs.find_all('a', {'target':"_self"})]
            content  = getting_news_content(urls ,page_num)
            extracted_content =  [extract_relevant_sentence(stockcode, text) for text in content]
            ex_content_textblob = ex_content_sentiment_analysis(extracted_content)
            content = [str(para) for para in content] # make the list-type paragraphs be string.
            print(len(headline))
            print(len(content))

            for i in range(len(headline)):
                c.execute("INSERT INTO sendash (date, time, weekday, headline, content, headline_vader, extracted_content, ex_content_textblob, stockcode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                    (date[i], time[i], weekday[i], headline[i], content[i], headline_vader[i], extracted_content[i], ex_content_textblob[i], stockcode))
                conn.commit() 
        print(f'finish scraping page {page_num}')
        page_num += 1
    return print(f'finish scraping all pages of {stockcode}')

def getting_last_page_num(stockcode):
  html = requests.get(f'https://www.nasdaq.com/symbol/{stockcode}/news-headlines')
  soup = BeautifulSoup(html.text, "html.parser")
  num  =int(re.sub(r'page=','',re.findall(r'page=.\d+', str(soup.find_all('a',{'class':'pagerlink'})[-1]))[0]))
  print(f'{stockcode} has {num} page in total')
  return num

def getting_news_content(urls ,page_num):
    content = []
    for url in urls:
        url = requests.get(url)
        soup = BeautifulSoup(url.text, "html.parser")
        search_by_text = soup.find_all('div', {'id':'articleText'})
        search_by_body = soup.find_all('div', {'id':'articlebody'})
        if search_by_text == None: 
            articles = search_by_body
            print(f'page {page_num} searches by articlebody')
        elif search_by_text == []:
            articles = search_by_body
            print(f'page {page_num} searches by articlebody')
        else:
            articles = search_by_text
            print(f'page {page_num} searches by articleText')           
        sent = []
        for items in articles:
            for item in items.find_all('p'):
                item = item.text
                item = re.sub('\s\s', '', item)
                item = re.sub('\\\\', '', item)
                sent.append(item)
            content.append(sent)
    return content


def extract_relevant_sentence(stockcode, content):
    if stockcode == 'NFLX':
        company_name = 'Netflix'
    elif stockcode == 'FB':
        company_name = 'Facebook'
    elif stockcode == 'AMZN':
        company_name = 'Amazon'
    else:
        company_name = 'Google'
    sentences = []
    for sentence in content:
        if (company_name in sentence) or (stockcode in sentence):
            sentences.append(sentence)
    print('num of all sentences: ', len(content))
    print('num of extracted sentences: ', len(sentences))
    sentences = ''.join(sentences)
    return sentences

def ex_content_sentiment_analysis(extracted_content):
    ex_content_textblob = []
    for text in extracted_content:
        text = re.sub(r'[^a-zA-z0-9\s]', '', text.lower())
        sent = TextBlob(unidecode(text)).sentiment[0]
        ex_content_textblob.append(sent)
    return ex_content_textblob

#define the job that you want to automate
def job():
    start_time = time.time()
    target = ['NFLX', 'FB','AMZN','GOOGL']
    for stock in target:
        # specify the page you want to scrape
        scrape_nasdaq(stock, last_page_num=getting_last_page_num(stock))
        print(f'finish streaming {stock} to the database at Hong Kong time: ' + str(dt.today()))
    print("--- %s seconds ---" % (time.time() - start_time))

# specify the time and times to do the job 
schedule.every(1).day.at("16:46").do(job)


while 1:
    schedule.run_pending()
    time.sleep(1)









