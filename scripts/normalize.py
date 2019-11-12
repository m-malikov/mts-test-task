import sqlite3

conn = sqlite3.connect('db')
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