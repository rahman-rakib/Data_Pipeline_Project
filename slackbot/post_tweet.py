import requests
from sqlalchemy import create_engine
import pandas as pd
import time

from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.

time.sleep(20)

webhook_url = os.getenv("webhook_url")

# 1) connecting to postgres
pg = create_engine('postgresql://postgres:1234@postgresdb:5432/tweets', echo=True)
# this requires us to install sqlalchemy to connect

# 2) querying data from postgres
query = '''
    SELECT * FROM tweets
    LIMIT 5;
'''
df_tweets = pd.read_sql(query,pg)

# 3) posting the data on slack

for i in range(5):
    text = str(df_tweets.iloc[i]['text'])
    sentiment = str(df_tweets.iloc[i]['sentiment'])
    data={
        "blocks": [
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": text
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": sentiment
                }
            }
        ]
    }
    requests.post(url=webhook_url, json=data)
    time.sleep(60)
