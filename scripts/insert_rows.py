import sqlite3
import json

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

conn = sqlite3.connect('db')
cur = conn.cursor()

with open("raw_data/three_minutes_tweets.json.txt", "r") as f:
    tweets = map(json.loads, f)
    tweets = filter(lambda t: "created_at" in t, tweets)
    rows = map(extract_fields, tweets)
    cur.executemany("INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

conn.commit()