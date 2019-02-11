import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
from unidecode import unidecode
from datetime import datetime as dt
from mkt_dt_utils import TimeConvert, IsMarketOpen, GetNextMktDate, GetTimeToMktOpen 
import pytz
import time
import csv


def relevance_threshold(relevance, threshold=2):
    if relevance > threshold:
        relevance = relevance
    else:
        relevance = np.nan
    return relevance


def GenerateDailySentimentTable(df):
    close_mkt  = df[df['IsMarketOpen'] != True]
    close_mkt['_sentiment'] = close_mkt['_sentiment'].dropna()
    close_mkt['_relevance'] =  close_mkt['_relevance'].apply(lambda x: relevance_threshold(x, 2))
    close_mkt['_relevance'].dropna(inplace=True)
    classification = [column for column in close_mkt.columns]
    DailySentimentTable = close_mkt[['_sentiment','stockcode','TradeDate']].groupby(['stockcode','TradeDate']).mean()
    DailySentimentTable = DailySentimentTable.reset_index()
    return DailySentimentTable
       

def write_to_csv(sent_table):
    rows = []
    with open ('daily_sentiment.csv','a') as file:
        writer = csv.writer(file)
        rows = []
        for i in range(len(sent_table)):
            row = []
            for col in sent_table.columns:
                row.append(sent_table[col][i])
            rows.append(row)
            print(f'inserted row {i}!')

        for r in rows:
            writer.writerow(r)
    
    return print('finished!')