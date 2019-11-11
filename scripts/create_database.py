import sqlite3

conn = sqlite3.connect('db')
cur = conn.cursor()

cur.execute("""
    CREATE TABLE tweets (name text, 
                         tweet_text text, 
                         country_code text, 
                         display_url text,
                         lang text,
                         created_at timestamp,
                         location text
                         );
""")

conn.commit()