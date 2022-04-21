import time
from datetime import datetime
import logging
import random
import pymongo
import tweepy

from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.

# Create connection to the MongoDB database server
client = pymongo.MongoClient(host='mongodb') # hostname = servicename for docker-compose pipeline

# Create/use a database
db = client.twitter
# equivalent of CREATE DATABASE twitter;

# Define the collection
db.tweets.drop() #clean existing data
collection = db.tweets
# equivalent of CREATE TABLE tweets;


##################
# Authentication #
##################
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

client = tweepy.Client(
    bearer_token=BEARER_TOKEN,
    wait_on_rate_limit=True,
)

########################
# Get User Information #
########################

# https://docs.tweepy.org/en/stable/client.html#tweepy.Client.get_user
# https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user

response = client.get_user(
    username='nytimes',
    user_fields=['created_at', 'description', 'location',
                 'public_metrics', 'profile_image_url']
)

user = response.data


#########################
# Get a user's timeline #
#########################

# https://docs.tweepy.org/en/stable/pagination.html#tweepy.Paginator
# https://docs.tweepy.org/en/stable/client.html#tweepy.Client.get_users_tweets
# https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet

cursor = tweepy.Paginator(
    method=client.get_users_tweets,
    id=user.id,
    exclude=['replies', 'retweets'],
    tweet_fields=['author_id', 'created_at', 'public_metrics']
).flatten(limit=5)

for tweet in cursor:
    logging.warning('-----Tweet being written into MongoDB-----')
    # logging.warning(tweet)
    collection.insert_one(dict(tweet))
    # logging.warning(str(datetime.now()))
    logging.warning('-----Tweet already inserted into MongoDB-----')
    # time.sleep(3)