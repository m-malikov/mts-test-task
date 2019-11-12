import sqlite3

conn = sqlite3.connect('db')
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