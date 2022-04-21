import pymongo
import time
from sqlalchemy import create_engine
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

time.sleep(10)  # seconds

# Establish connection to the MongoDB server
client = pymongo.MongoClient(host="mongodb", port=27017)

# Select database to use within the MongoDB server
db = client.twitter

# Clean tweet text
mentions_regex= '@[A-Za-z0-9]+'
url_regex='https?:\/\/\S+' #this will not catch all possible URLs
hashtag_regex= '#'
rt_regex= 'RT\s'

def clean_tweets(tweet):
    tweet = re.sub(mentions_regex, '', tweet)  #removes @mentions
    tweet = re.sub(hashtag_regex, '', tweet) #removes hashtag symbol
    tweet = re.sub(rt_regex, '', tweet) #removes RT to announce retweet
    tweet = re.sub(url_regex, '', tweet) #removes most URLs
    return tweet

# Create sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

pg = create_engine('postgresql://postgres:1234@postgresdb:5432/tweets', echo=True)

pg.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
    text VARCHAR(500),
    sentiment NUMERIC
);
''')

docs = db.tweets.find(limit=5)

for doc in docs:
    text = doc['text']
    text = clean_tweets(text)
    sentiment = analyzer.polarity_scores(text)
    score = sentiment['compound']  # placeholder value
    query = "INSERT INTO tweets VALUES (%s, %s);"
    pg.execute(query, (text, score))