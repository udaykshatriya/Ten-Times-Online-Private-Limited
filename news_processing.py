# Python Libraries
import feedparser
from datetime import datetime
from typing import List
import logging

# Database Libraries
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Celery Libraries
from celery import Celery

# NLP Libraries
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Configuration
DATABASE_URL = 'sqlite:///news.db'
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Define Article Model
class Article(Base):
    __tablename__ = 'articles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String)
    content = Column(String)
    published = Column(DateTime)
    source_url = Column(String)
    category = Column(String)

# Celery Configuration
app = Celery('tasks', broker='redis://localhost:6379/0')

# NLP Configuration
stop_words = set(stopwords.words('english'))
stemmer = PorterStemmer()

def process_text(text):
    tokens = word_tokenize(text)
    filtered_tokens = [stemmer.stem(word.lower()) for word in tokens if word.isalpha() and word.lower() not in stop_words]
    return filtered_tokens

def classify_category(text):
    # returning a fixed category
    return 'politics'

# Initialize Database
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Feed Parser and Data Extraction
def parse_feed(url):
    feed = feedparser.parse(url)
    articles = []
    for entry in feed.entries:
        article = {
            'title': entry.get('title'),
            'content': entry.get('summary'),
            'published': datetime.strptime(entry.get('published'), '%a, %d %b %Y %H:%M:%S %z'),
            'source_url': entry.get('link')
        }
        articles.append(article)
    return articles

# Database Storage
def store_articles(articles: List[dict]):
    session = Session()
    try:
        for article in articles:
            existing_article = session.query(Article).filter_by(source_url=article['source_url']).first()
            if not existing_article:
                new_article = Article(**article)
                session.add(new_article)
        session.commit()
        logger.info("Articles stored successfully")
    except Exception as e:
        logger.error(f"Error storing articles: {e}")
        session.rollback()
    finally:
        session.close()

# Task Queue and News Processing
@app.task
def process_articles():
    session = Session()
    try:
        articles = session.query(Article).filter_by(category=None).all()
        for article in articles:
            tokens = process_text(article.content)
            category = classify_category(tokens)
            article.category = category
        session.commit()
        logger.info("Articles processed successfully")
    except Exception as e:
        logger.error(f"Error processing articles: {e}")
        session.rollback()
    finally:
        session.close()

# Main Script
if __name__ == '__main__':
    feed_urls = [
        "http://rss.cnn.com/rss/cnn_topstories.rss",
        "http://qz.com/feed",
        "http://feeds.foxnews.com/foxnews/politics",
        "http://feeds.reuters.com/reuters/businessNews",
        "http://feeds.feedburner.com/NewshourWorld",
        "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"
    ]

    for url in feed_urls:
        articles = parse_feed(url)
        store_articles(articles)
        process_articles.delay()
