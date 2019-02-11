from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from unidecode import unidecode
import time
import re
from textblob import TextBlob


#sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

#streaming
ckey=""
csecret=""
atoken=""
asecret=""

#database
conn = sqlite3.connect('tweets.db')
c = conn.cursor()

def create_table():
    
    try:
        c.execute("CREATE TABLE IF NOT EXISTS sentiment(unix REAL, tweet TEXT, vadar_sentiment REAL, textblob_sentiment REAL, keyword TEXT)")
        c.execute("CREATE INDEX fast_unix ON sentiment(unix)")
        c.execute("CREATE INDEX fast_tweet ON sentiment(tweet)")
        c.execute("CREATE INDEX fast_vadar_sentiment ON sentiment(vadar_sentiment)")
        c.execute("CREATE INDEX fast_textblob_sentiment ON sentiment(textblob_sentiment)")
        c.execute("CREATE INDEX fast_keyword ON sentiment(keyword)")
        conn.commit()
    
    except Exception as e:
        print(str(e))

create_table()

class listener(StreamListener):

    def on_data(self, data):
      
      try:
        data = json.loads(data)
        with open('data.json', 'w') as outfile:
          json.dump(data, outfile)
        
        # retrieve the tweet
        if 'retweeted_status' in data:
          print('This is from retweet')
          
          try:
            tweet = data['retweeted_status']['extended_tweet']["full_text"]
            print('retweet -> full_text')

          except:
            tweet = data['retweeted_status']['extended_tweet']['text']
            print('retweet -> text')
        
        else:
          print('This is from tweet')
          
          try:
            tweet = data['extended_tweet']['full_text']
            print('data -> full_text')


          except KeyError:
            tweet = data['text']
            print('data -> text')

        
        # retrieve the timestamp
        time_ms = data['timestamp_ms']
        
        # calculate sentiment based on the retrieved tweet
        vadar_sentiment = unidecode(re.sub(r'[^a-zA-z0-9\s]','',tweet).lower())
        vadar_sentiment = analyzer.polarity_scores(vadar_sentiment)
        vadar_sentiment = vadar_sentiment['compound']

        textblob_sentiment = unidecode(re.sub(r'[^a-zA-z0-9\s]','',tweet).lower())
        textblob_sentiment = TextBlob(textblob_sentiment).sentiment[0]

        # extract the keyword
        
        keyword = []
        
        if '$FB' in tweet:
          keyword.append('Facebook')
        if '$AMZN' in tweet:
          keyword.append('Amazon')
        if '$NFLX'in tweet:
          keyword.append('Netflix')
        if '$GOOGL'in tweet:
          keyword.append('Google')

        if len(keyword) == 1:
          keyword = keyword[0]
          c.execute("INSERT INTO sentiment (unix, tweet, vadar_sentiment, textblob_sentiment, keyword) VALUES (?, ?, ?, ?, ?)", (time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword))
          conn.commit()
          print(time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword)

        else: 
          print('!!!!!!!!!!!!!!!!!!!!More than 1 Keyword!!!!!!!!!!!!!!!!!!!!')
          
          for key in keyword:

            if key == 'Facebook':
              print("please insert 'Facebook' to the database")
              keyword = key
              c.execute("INSERT INTO sentiment (unix, tweet, vadar_sentiment, textblob_sentiment, keyword) VALUES (?, ?, ?, ?, ?)", (time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword))
              conn.commit()
              print(time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword)

            if key == 'Google':
              print("please insert 'Google' to the database")
              keyword = key
              c.execute("INSERT INTO sentiment (unix, tweet, vadar_sentiment, textblob_sentiment, keyword) VALUES (?, ?, ?, ?, ?)", (time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword))
              conn.commit()
              print(time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword)

            if key == 'Amazon':
              print("please insert 'Amazon' to the database")
              keyword = key
              c.execute("INSERT INTO sentiment (unix, tweet, vadar_sentiment, textblob_sentiment, keyword) VALUES (?, ?, ?, ?, ?)", (time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword))
              conn.commit()
              print(time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword)

            if key == 'Netflix':
              print("please insert 'Netflix' to the database")
              keyword = key
              c.execute("INSERT INTO sentiment (unix, tweet, vadar_sentiment, textblob_sentiment, keyword) VALUES (?, ?, ?, ?, ?)", (time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword))
              conn.commit()
              print(time_ms, tweet, vadar_sentiment, textblob_sentiment, keyword)
      
      except KeyError as e:
        print(str(e))
        return(True) 
    
    def on_error(self, status):
        print(status)
        

while True:
  try:
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    twitterStream = Stream(auth, listener(), )
    twitterStream.filter(track=['$NFLX','$AMZN','$GOOGL', '$FB'],languages=["en"]) 
    print('one tweet again')
  except Exception as e:
    print(str(e))
    time.sleep(3)