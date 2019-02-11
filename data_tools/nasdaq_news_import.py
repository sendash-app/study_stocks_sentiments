from torrequest import TorRequest
from stem import Signal
from stem.control import Controller
import requests
from bs4 import BeautifulSoup
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
from datetime import timedelta
from unidecode import unidecode
from datetime import datetime as dt
import schedule
import time
from textblob import TextBlob
import csv

with Controller.from_port(port = 9051) as controller:
    controller.authenticate(password='16:872860B76453A77D60CA2BB8C1A7042072093276A3D701AD684053EC4C')
    print("Success!")
    controller.signal(Signal.NEWNYM)
    print("New Tor connection processed")
    torR=TorRequest(password='')
    torR.reset_identity() #Reset Tor
    response= torR.get('http://ipecho.net/plain')
    print("New Ip Address",response.text)


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
        html = get_connection(f'https://www.nasdaq.com/symbol/{stockcode}/news-headlines?page={page_num}', torR)
        soup = BeautifulSoup(html.text, "html.parser")
        
        for divs in soup.find_all(class_ = 'news-headlines'):
            headline = [s.text for s in divs.find_all('a', {'target':"_self"})]
            urls     = [s.get('href') for s in divs.find_all('a', {'target':"_self"})] 
            content_page = getting_news_content(urls, page_num)
            article = content_page[0]
            datetime = content_page[1]
            source   = content_page[2]
            print(len(headline))
            print(len(urls))
            print(len(article))
            print(len(datetime))
            print(len(source))

            rows = []
            for i in range(len(headline)):
              row = [datetime[i], stockcode, source[i], headline[i], article[i], urls[i]]
              rows.append(row)
              print(f'inserted row {i}!')


            with open ('sendash.csv','a') as file:
              writer = csv.writer(file)
              for r in rows:
                writer.writerow(r)
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
    datetime = []
    sources = []
    for url in urls:
        url = get_connection(url, torR)
        soup = BeautifulSoup(url.text, "html.parser")
        try:
            source = soup.find('a', {'rel':'author'}).text
        except:
            source = 'NA'
        try:
            date = soup.find('span', {'itemprop':'datePublished'}).text
        except:
            date = 'NA'
        try:
            articles = soup.find('div', {'id':'articleText'}).text
            print(f'page {page_num} searches by articleText') 
        except:
            articles = soup.find('div', {'id':'articlebody'}).text
            print(f'page {page_num} searches by articlebody') 
        
        content.append(articles)
        datetime.append(date)
        sources.append(source)
    return (content, datetime,sources)   


def get_connection(links_site, torR):

   headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

   for i in range(15):
       try:
           resp = torR.get(links_site, headers=headers, timeout=5)
           return resp
       except requests.exceptions.Timeout:
           print("Reconnect" + str(i+1))
           with Controller.from_port(port = 9051) as controller:
               controller.authenticate(password='16:872860B76453A77D60CA2BB8C1A7042072093276A3D701AD684053EC4C')
               print("Success!")
               controller.signal(Signal.NEWNYM)
               print("New Tor connection processed")
               torR=TorRequest(password='')
               torR.reset_identity() #Reset Tor
               response= torR.get('http://ipecho.net/plain')
               print("New Ip Address",response.text)
           pass

   return resp

#define the job that you want to automate
def job():
  start_time = time.time()
  target = ['AMZN','GOOGL','FB','NFLX']
  for stock in target:
        # specify the page you want to scrape
    scrape_nasdaq(stock, last_page_num=5)
    print(f'finish streaming {stock} to the database at Hong Kong time: ' + str(dt.today()))
  print("--- %s seconds ---" % (time.time() - start_time))

# specify the time and times to do the job 
schedule.every(1).day.at("12:45").do(job)


while 1:
    schedule.run_pending()
    time.sleep(1)