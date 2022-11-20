import pandas as pd
from datetime import *
import psycopg2 as pg
import psycopg2.extras as extras
import re
import snscrape.modules.twitter as sntwitter

conn = pg.connect(database="tweet_db", 
                        user='postgres',
                        password=12345, 
                        host='localhost', 
                        port= '5432')
conn.autocommit = True
cursor = conn.cursor()

coin_list = ["Bitcoin", "Ethereum",\
                "Ripple", "Solana",\
                "Cardano", "Binance Coin",\
                "Tether", "DogeCoin"]

def url_filter(text: str) -> str:
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    url = re.search(regex, text)
    
    if url and len(url[0]) / len(text) > 0.50:
        return "SPAM"
    else:
        return text
    
def remove_emojis(text: str) -> str:
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"
        u"\U0001F300-\U0001F5FF"  
        u"\U0001F680-\U0001F6FF"  
        u"\U0001F1E0-\U0001F1FF"  
        u"\U00002500-\U00002BEF"  
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  
        u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, '', text)

def filter_scam_tweets(text: str) -> str:
    word_black_list = ["giving away", "Giving away", "GIVING AWAY", "PRE-GIVEAWAY", "Giveaway", "GIVEAWAY", "giveaway", "follow me", "Follow me", "FOLLOW ME", "retweet", "Retweet", "RETWEET", "LIKE", "airdrop","AIRDROP", "Airdrop", "free", "FREE", "Free", "-follow", "-Follow", "-rt", "-Rt", "Requesting faucet funds"]
    if any(ext in text for ext in word_black_list):
        return "SPAM"
    else:
        return text
    
def clean_text(text: str) -> str:
    text = str(text)

    text = text.replace("\n", " ")
    text = url_filter(text)
    text = filter_scam_tweets(text)
    text = remove_emojis(text)
    
    text = re.sub("@[A-Za-z0-9_]+","", text)
    text = text.replace("#", "") # remove hashtags from tweet
    
    url_regex = r"(?i)\b((?:https://?|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    text = re.sub(url_regex, "", text) # remove URL's from tweet
        
    return text.strip()

def execute_values(conn, df, table, n):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT DO NOTHING;" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print(f"Dataframe {n+1} is inserted")
    cursor.close()

for n, crypto in enumerate(coin_list):
    
    coin = str.lower(crypto).replace(" ","")
    
    if coin == "dogecoin":
        coin = 'doge'
    else: pass
    
    query = '''
        CREATE TABLE IF NOT EXISTS %s(
        date TIMESTAMP NOT NULL PRIMARY KEY,
        id NUMERIC NOT NULL,
        Text TEXT
        )''' % coin
    
    cursor.execute(query)
    
    # Creating list to append tweet data
    tweets_list = []

    # Using TwitterSearchScraper to scrape data and append tweets to list
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(f"${coin} lang:en \
                                            since:{(datetime.now()-timedelta(1)).strftime('%Y-%m-%d')} \
                                            until:{(datetime.now().strftime('%Y-%m-%d'))}").get_items()):  # declare a username
        if i > 100:  # number of tweets you want to scrape
            break
        tweets_list.append([tweet.date,
                             tweet.id,
                             tweet.content])  # declare the attributes to be returned

    # Creating a dataframe from the tweets list above
    tweet_df = pd.DataFrame(tweets_list, columns=["date","id","text"])

    results = []

    for i, row in tweet_df.iterrows():
        result = clean_text(row["text"])
        results.append(result)

    tweet_df['Text'] = results
    tweet_df = tweet_df[tweet_df['Text'] != 'SPAM']
    tweet_df['date'] = pd.to_datetime(tweet_df['date'].dt.strftime('%Y/%m/%d %H:%M:%S'))
    tweet = tweet_df[['date','id','Text']]
    
    execute_values(conn, tweet, coin, n)