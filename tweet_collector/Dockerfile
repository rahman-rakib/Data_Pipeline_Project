FROM python:3.8

WORKDIR /tweet_collector

COPY requirements.txt /tweet_collector

RUN pip install -r requirements.txt

CMD ["python", "tweet_collector.py"]