##########################################################################
##########################################################################
                      # EXTRACT TRANSFORM LOAD #
##########################################################################
##########################################################################

# extract stack
from pymongo import MongoClient

# transform stack
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# load stack
from sqlalchemy import create_engine

# miscellaneous
import time


##########################################################################
                               # EXTRACT #
##########################################################################


def mongodb_connection():
    """
    conencts to mongodb twitter database
    """
    # establish connection to mongodb container
    client = MongoClient(
        host="mongodb",   # host: name of the container
        port=27017        # port: port inside the container
        )

    # connect to twitter database
    db = client.twitter

    return db


def extract(db, number_of_tweets=5):
    """
    extracts tweets from the mongodb twitter database
    """
    # connect to the tweets collection
    tweets = db.tweets
    
    # pull out tweets with filter
    extracted_tweets = tweets.find(limit=number_of_tweets)

    return extracted_tweets


##########################################################################
                             # TRANSFORM #
##########################################################################


# useful regular expressions
regex_list = [
    '@[A-Za-z0-9]+',  # to find @mentions
    '#',              # to find hashtag symbol
    'RT\s',           # to find retweet announcement
    'https?:\/\/\S+'  # to find most URLs
    ]


def clean_tweet(tweet):
    """
    gets the text of a tweet and cleans it up 
    """
    # get tweet text
    text = tweet['text'] 

    # remove all regex patterns
    for regex in regex_list:
        text = re.sub(regex, '', text)  
    
    return text


# instantiate sentiment analyzer
analyzer = SentimentIntensityAnalyzer()


def sentiment_score(text):
    """
    spits out sentiment score of a text
    """
    # calculate polarity scores 
    sentiment = analyzer.polarity_scores(text)
    
    # choose compund polarity score
    score = sentiment['compound'] 

    return score


def transform(tweet):
    """
    transforms extracted tweet: text cleaning and sentiment analysis
    """
    text = clean_tweet(tweet)
    score = sentiment_score(text)

    return text, score


##########################################################################
                              # LOAD #
##########################################################################


def postgres_connection():
    """
    establishes connection to postgres database
    """
    # create sql query engine
    pg_engine = create_engine(
        'postgresql://postgres:1234@postgresdb:5432/tweets',
        echo=True
        )

    return pg_engine


def load(pg_engine, transformed_data):
    """
    loads tweet text and score into postgresql database
    """
    # create table of tweet text and sentiment if necessary 
    pg_engine.execute("""
    CREATE TABLE IF NOT EXISTS tweets (
        text VARCHAR(500),
        sentiment NUMERIC
        );""")

    # sql query for inserting text and score data
    query = "INSERT INTO tweets VALUES (%s, %s);"
    
    # insert new recods in table
    for data in transformed_data:
        text,score = data
        pg_engine.execute(query, (text,score))


##########################################################################
                           # ETL STEPS #
##########################################################################

# mongodb connection; give mongodb some seconds to start
mongo_db = mongodb_connection()
time.sleep(10)

# extract tweets from mongodb
extracted_tweets = extract(mongo_db)

# transform data
transformed_data = [transform(tweet) for tweet in extracted_tweets]

# postgres connection
pg_engine = postgres_connection()

# load data into postgres
load(pg_engine, transformed_data)