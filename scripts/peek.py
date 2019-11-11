import sqlite3

conn = sqlite3.connect('db')
cur = conn.cursor()

for row in cur.execute("SELECT * FROM tweets LIMIT 3"):
    print(row)