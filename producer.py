import praw
from kafka import KafkaProducer
import logging
import json
from datetime import datetime

"""API ACCESS KEYS"""
redditClientId = '8h8g1t7yJYOVP_B8bMih2Q'
redditClientSecret = 'YFshn5ausxLow-Qjv3rNJ-822fBgvg'
producer = KafkaProducer(bootstrap_servers='localhost:9092')
subreddit = 'Pepsi'
topic_name = 'reddit_posts'

logging.basicConfig(level=logging.INFO)

def fetch_reddit_posts(subreddit):
    reddit = praw.Reddit(client_id=redditClientId,
                         client_secret=redditClientSecret,
                         user_agent='Data')

    # Fetch posts from a specific subreddit
    posts = reddit.subreddit(subreddit).new(limit=20)
    # Fetches 10 latest posts
    for post in posts:
        title = post.title
        content = post.selftext
        if post.author is not None:
            author = post.author.name
        else:
            author = "anonymous"
        upload_time = str(post.created_utc)
        
        data = {
            'title': title,
            'content': content,
            'author': author,
            'upload_time': upload_time
        }
        producer.send(topic_name, value=json.dumps(data).encode('utf-8'))

if __name__ == '__main__':
    try:
        fetch_reddit_posts(subreddit)
    finally:
        producer.flush()
        producer.close(timeout=100)  # Increase the timeout value, e.g., 10 seconds