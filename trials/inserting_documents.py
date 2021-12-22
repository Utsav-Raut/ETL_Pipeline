# This is part 2 of the mongo learning

from pymongo import MongoClient

try:
    # client = MongoClient('localhost', 27017)
    # Alternative way to establish a connection to mongoclient
    client = MongoClient("mongodb://localhost:27017/")
    print('Connected successfully.')
except:
    print('Could not connect to the mongodb instance.')

mydb = client['practice_db']
mycol = mydb['myColl']

record = {
    "title": "MongoDB and Python",
    "description": "MongoDB is a no-SQL database",
    "tags": ['mongodb', 'database', 'NoSQL'],
    "viewers": 104
}

rec = mycol.insert_one(record)

# Checking if the data got inserted or not
for doc in mycol.find():
    print(doc)