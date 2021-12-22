# This is part 3 of the mongo learning

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

print('Using find_one')
print(mycol.find_one())

print('Fetching the exact record')
print(mycol.find_one({'title': 'MongoDB and Python'}))

print('Finding all')
for doc in mycol.find():
    print(doc)


print('Returning only some fields')
for doc in mycol.find({}, {'_id': 0}):
    print(doc)

print('Getting the count of records')
cnt = mycol.count_documents({})
print(cnt)