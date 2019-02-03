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

#database
conn = sqlite3.connect('FANG_sentiment.db')
c = conn.cursor()

def create_table():
    
    try:
        c.execute("CREATE TABLE IF NOT EXISTS sendash (date TEXT , time TEXT, weekday REAL , headline TEXT, vader REAL, stockcode TEXT)")
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
    Last_page: The last page of the latest news page of a specific stock
    """
    page_num = 1
    results  = []
    while True:
        if page_num > last_page_num: # Last page
            break
        html = requests.get(f'https://www.nasdaq.com/symbol/{stockcode}/news-headlines?page={page_num}')
        soup = BeautifulSoup(html.text, "html.parser")
        for divs in soup.find_all(class_ = 'news-headlines'):
            headline = [s.text for s in divs.find_all('a', {'target':"_self"})]
            pub_date = [re.sub(r'\-.+','',d.text.strip()).strip() for d in divs.find_all('small')]
            pub_date = pd.Series(pub_date).apply(lambda x: str(dt.strptime(x, '%m/%d/%Y %I:%M:%S %p') - timedelta(hours=5))).values
            date     = [re.findall(r'\d.+\s', dat)[0].strip() for dat in pub_date]
            time     = [re.findall(r'\s.+', tim)[0].strip() for tim in pub_date]
            weekday  = [dt.strptime(day, '%Y-%m-%d %H:%M:%S').weekday() for day in pub_date]
            vader    = [analyzer.polarity_scores(unidecode(text))['compound'] for text in headline]
            for i in range(len(headline)):
                c.execute("INSERT INTO sendash (date, time, weekday, headline, vader, stockcode) VALUES (?, ?, ?, ?, ?, ?)", (date[i], time[i], weekday[i], headline[i], vader[i], stockcode))
                conn.commit() 
        print(f'finish{page_num}')
        page_num += 1
    return print('finish')

# define the job that you want to automate
def job():
    target = ['FB','NFLX','AMZN','GOOGL']
    for stock in target:
        # specify the page you want to scrape
        scrape_nasdaq(stock, last_page_num=2)
        print(f'finish streaming {stock} to the database at Hong Kong time: ' + str(datetime.today()))

# specify the time and times to do the job 
schedule.every(1).day.at("20:21").do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)









