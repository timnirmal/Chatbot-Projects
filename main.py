from datetime import datetime

import keras as keras
import tensorflow as tf

print("is gpu available ",tf.config.list_physical_devices('GPU'))
print("List : ",tf.config.experimental.list_physical_devices())
print("is with Cuda " ,tf.test.is_built_with_cuda())
print("test gpu device name ", tf.test.gpu_device_name())


import sqlite3
import json
from datetime import datetime

# In here we prepare a database to hold Comments and replys because RAM along cant hold the data we have.
# We will use sqlite3 to store the data in a database. Json to load data from datadump.

timeframe = '2015-05'
sql_transaction = []

connection = sqlite3.connect('{}2.db'.format(timeframe))
c = connection.cursor()

def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

def format_data(data):
    data = data.replace('\n',' newlinechar ').replace('\r',' newlinechar ').replace('"',"'")
    return data

def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        #print(str(e))
        return False

if __name__ == '__main__':
    create_table()
    row_counter = 0 # time to time in which row we are in
    paired_rows = 0 # comment and reply pair how much

    dataset_path = "C:/Users/timni/Downloads/RC_2015-01"

    with open('C:/Users/timni/Downloads/RC_2015-01/RC_{}-01'.format(timeframe.split('-')[0],timeframe), buffering=1000) as f:
        for row in f:
            #print(row)
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['name']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)
            print(row)









"""
DBMS: SQLite (ver. 3.34.0)
Case sensitivity: plain=mixed, delimited=mixed
Driver: SQLite JDBC (ver. 3.34.0, JDBC2.1)
Ping: 37 ms
"""