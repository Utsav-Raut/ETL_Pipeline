# This is part 1 of the mongo learning

from pymongo import MongoClient

client = MongoClient('localhost', 27017)
# Alternative way to establish a connection to mongoclient
# client = MongoClient("mongodb://localhost:27017/")

# For checking the databases
# for db in client.list_databases():
#     print(db)

# To access an existing db or to create a new one
myDb = client['practice_db']

# The above db has no existence unless we create a collection within it and insert data
myColl = myDb['tbl1']

record = {
    "title": "MongoDB and Python",
    "description": "MongoDB is a no-SQL database",
    "tags": ['mongodb', 'database', 'NoSQL'],
    "viewers": 104
}

rec = myDb.myColl.insert_one(record)

for db in client.list_databases():
    print(db)



print("THE DATABASES ARE:")
# Listing the databases
for i in client.list_databases():
    print(i)


print("THE COLLECTIONS ARE:")
# finding collections within a db
for coll in myDb.list_collections():
    print(coll['name'])

