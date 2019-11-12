#!/bin/sh
python create_database.py
python insert_rows.py
#python peek.py tweets
python normalize.py