import sqlite3
import re
from collections import Counter

with open("raw_data/AFINN-111.txt") as f:
    word_ratings = [line.split('\t') for line in f]
    word_ratings = {word[0]: int(word[1]) for word in word_ratings} 

def prepare_text(text):
    return Counter(re.findall(r'\w{3,}', text.lower()))

def rate(words):
    score = 0
    for word, n_occurencies in words.items():
        score += word_ratings.get(word, 0) * n_occurencies
    return score

conn = sqlite3.connect("db")
cur = conn.cursor()

cur.execute("SELECT id, tweet_text FROM tweets_normalized WHERE lang = 'en'")
for row in cur:
    conn.execute('UPDATE tweets_normalized SET tweet_sentiment = ? WHERE id = ?',
                 (str(rate(prepare_text(row[1]))), row[0]))
    
conn.commit()