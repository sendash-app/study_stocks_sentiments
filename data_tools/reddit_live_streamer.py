import json
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from unidecode import unidecode
import time
import re
from textblob import TextBlob


#sentiment analyzer
import praw
import datetime

reddit = praw.Reddit(client_id='', 
                client_secret="", 
                username='', 
                password='', 
                user_agent='')


def sentiment_analytics(comment):
    sentiment = unidecode(re.sub(r'[^a-zA-z0-9\s]','',str(comment)).lower())
    analyzer = SentimentIntensityAnalyzer()
    vader_sentiment = analyzer.polarity_scores(sentiment)['compound']
    textblob_sentiment = TextBlob(sentiment).sentiment[0]
    return {'textblob':textblob_sentiment, 'vader':vader_sentiment}

subreddit = reddit.subreddit('wallstreetbets+stocks+investing') # tracking subreddits that have 100k+ followers
Google = []
Neflix = []
Facebook = []
Amazon = []
# 'wallstreetbets+stocks+investing'
for comment in subreddit.stream.comments(skip_existing=True):
    try:
        parent_id = str(comment.parent())
        submission = reddit.comment(parent_id)
        bod = [comment.body]
        sub = [submission.body]

        if re.search("Google", sub[0], re.IGNORECASE):
            data = {'parent': sub[0], 'comment':bod[0], 
                    'textblob': sentiment_analytics(bod[0])['textblob'],
                    'vader': sentiment_analytics(bod[0])['vader'],
                    'submission_timestamp': submission.created_utc, 
                    'comment_timestamp': comment.created_utc, 
                    'subreddit': comment.subreddit,
                    'stock':'Google'}
            print(data)
            Google.append(data)
            
        if re.search("Facebook", sub[0], re.IGNORECASE):
            data = {'parent': sub[0],'comment':bod[0], 
                    'textblob': sentiment_analytics(bod[0])['textblob'],
                    'vader': sentiment_analytics(bod[0])['vader'],
                    'submission_timestamp': submission.created_utc, 
                    'comment_timestamp': comment.created_utc, 
                    'subreddit': comment.subreddit, 'stock':'Facebook'}
            print(data)
            Facebook.append(data)

        if re.search("Amazon", sub[0], re.IGNORECASE):
            data = {'parent': sub[0],'comment':bod[0], 
                    'textblob': sentiment_analytics(bod[0])['textblob'],
                    'vader': sentiment_analytics(bod[0])['vader'],
                    'submission_timestamp': submission.created_utc, 
                    'comment_timestamp': comment.created_utc, 
                    'subreddit': comment.subreddit, 'stock':'Amazon'}
            print(data)
            Amazon.append(data)

        if re.search("Netflix", sub[0], re.IGNORECASE):
            data = {'parent': sub[0],'comment':bod[0], 
                    'textblob': sentiment_analytics(bod[0])['textblob'],
                    'vader': sentiment_analytics(bod[0])['vader'],
                    'submission_timestamp': submission.created_utc, 
                    'comment_timestamp': comment.created_utc, 
                    'subreddit': comment.subreddit, 'stock':'Netflix'}
            print(data)
            Neflix.append(data)
        
    
    except praw.exceptions.PRAWException as e:
        pass  