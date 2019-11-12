import json
from collections import Counter
import re

def create_database(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE tweets (name TINYTEXT, 
                            tweet_text TEXT, 
                            country_code TINYTEXT, 
                            display_url TINYTEXT,
                            lang TINYTEXT,
                            created_at TIMESTAMP,
                            location TEXT,
                            tweet_sentiment REAL
                            );
    """)
    conn.commit()

def insert_rows(conn, tweets_file):
    def extract_fields(tweet):
        name = tweet["user"]["screen_name"]
        tweet_text = tweet["text"]
        if tweet["place"] is not None:
            country_code = tweet["place"]["country_code"]
            location = tweet["place"]["full_name"]
        else:
            country_code = None
            location = None
        display_url = "https://twitter.com/{}/status/{}".format(name,
                                                            tweet["id"])
        lang = tweet["lang"]
        created_at = tweet["timestamp_ms"]
        if created_at is not None:
            created_at = int(created_at) // 1000
        return (name, tweet_text, country_code, display_url,
                lang, created_at, location, None)

    cur = conn.cursor()

    tweets = map(json.loads, tweets_file)
    tweets = filter(lambda t: "created_at" in t, tweets)
    rows = map(extract_fields, tweets)
    cur.executemany("INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

    conn.commit()

def normalize(conn):
    cur = conn.cursor()

    # country_code is defined by location
    cur.execute("""
    CREATE TABLE locations AS
    SELECT location as name, MAX(country_code) as country_code
    FROM tweets WHERE location IS NOT NULL GROUP BY location;
    """)

    # display_url dublicates name in it
    cur.execute("""
    CREATE TABLE tweets_normalized AS
    SELECT REPLACE(display_url, RTRIM(display_url, REPLACE(display_url, '/', '')), '') as id,
        name, tweet_text, country_code, lang, created_at, location, tweet_sentiment
    FROM tweets;
    """)

def fill_sentiment(conn, afinn_file):
    word_ratings = [line.split('\t') for line in afinn_file]
    word_ratings = {word[0]: int(word[1]) for word in word_ratings} 

    # With this approach hashtags are lost. Also, there are 16 entries in AFINN, containing more than 1 word.
    # So it might be improved, but seems good enough.
    def prepare_text(text):
        return Counter(re.findall(r'\w{3,}', text.lower()))

    def rate(words):
        score = 0
        for word, n_occurencies in words.items():
            score += word_ratings.get(word, 0) * n_occurencies
        return score

    cur = conn.cursor()
    cur.execute("SELECT id, tweet_text FROM tweets_normalized WHERE lang = 'en'")
    for row in cur:
        conn.execute('UPDATE tweets_normalized SET tweet_sentiment = ? WHERE id = ?',
                    (str(rate(prepare_text(row[1]))), row[0]))
        
    conn.commit()