import sqlite3
import os

import scripts

conn = sqlite3.connect("twitter.sqlite3")

scripts.create_database(conn)
with open(os.path.join("raw_data", "three_minutes_tweets.json.txt"), "r") as f:
    scripts.insert_rows(conn, f)
scripts.normalize(conn)
with open(os.path.join("raw_data", "AFINN-111.txt")) as f:
    scripts.fill_sentiment(conn, f)
