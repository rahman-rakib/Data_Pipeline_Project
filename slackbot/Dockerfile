FROM python:3.8

WORKDIR /post_tweet

COPY requirements.txt /post_tweet

RUN pip install -r requirements.txt

CMD ["python", "post_tweet.py"]