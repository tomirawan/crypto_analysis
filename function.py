import pandas as pd
import psycopg2 as pg

import requests
from datetime import *

from transformers import pipeline
import snscrape.modules.twitter as sntwitter

conn = pg.connect(database=<your database>, 
                        user=<username>,
                        password=<password>, 
                        host=<your host>, 
                        port=<port>)
conn.autocommit = True
cursor = conn.cursor()

model = f"cardiffnlp/twitter-roberta-base-sentiment-latest"
sentiment_task = pipeline('sentiment-analysis', model=model)

def getCrypto(choice):
    
    coin = str.lower(choice).replace(" ","")
    
    # Get the price data #
    url = 'https://api.coingecko.com/api/v3/coins/'+coin+'/market_chart?vs_currency=USD&days=7'
    response = requests.get(url).json()
    
    df = pd.DataFrame(response['prices'])
    df.columns = ['date', 'prices']
    df['date'] = pd.to_datetime(df['date'], unit='ms')
    df['date'] = pd.to_datetime(df['date'].dt.strftime('%Y/%m/%d %H:%M:%S'))
    df = df.set_index('date')

    df2 = pd.DataFrame(response['market_caps'])
    df2.columns = ['date', 'market_caps']
    df2['date'] = pd.to_datetime(df2['date'], unit='ms')
    df2['date'] = pd.to_datetime(df2['date'].dt.strftime('%Y/%m/%d %H:%M:%S'))
    df2['market_caps'] = round(df2['market_caps']/1000000, 2)
    df2 = df2.set_index('date')

    df3 = pd.DataFrame(response['total_volumes'])
    df3.columns = ['date', 'total_volumes']
    df3['date'] = pd.to_datetime(df3['date'], unit='ms')
    df3['date'] = pd.to_datetime(df3['date'].dt.strftime('%Y/%m/%d %H:%M:%S'))
    df3['total_volumes'] = round(df3['total_volumes']/1000000, 2)
    df3 = df3.set_index('date')

    merged_df = pd.concat([df, df2, df3], join='outer', axis=1)
    
    return merged_df
    
def getSentiment(choice):

    coin = str.lower(choice).replace(" ","")

    if coin == "dogecoin":
        coin = 'doge'
    else: pass
    
    cursor.execute("SELECT * FROM %s" %coin)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['date', 'id', 'Text'])
    
    sentiment = {}

    for i, row in df.iterrows():
        s = sentiment_task(row["Text"])
        sentiment[row['id']] = s

    sent_df = pd.DataFrame(sentiment).T
    sent_df['label'] = sent_df[0].apply(lambda x: x['label'])
    sent_df['score'] = sent_df[0].apply(lambda x: x['score'])
    sent_df = sent_df[['label', 'score']]
    sent_df.loc[sent_df['label'] == 'Negative', 'score'] = sent_df.loc[sent_df['label'] == 'Negative'] *-1

    final_tweet = sent_df.merge(df,
                            left_index=True, 
                            right_index=True)
    final_tweet = final_tweet.set_index('id')

    return final_tweet

