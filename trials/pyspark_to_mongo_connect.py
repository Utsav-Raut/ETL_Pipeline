# This is part 4 of the mongo learning
import json
import pyspark
from pyspark.sql import SparkSession

import sys
sys.path.insert(0, '/home/boom/Documents/programming/ETL_Pipeline/UTILS')
from CONFIG import *

print(pyspark.__version__)

#  config("spark.mongodb.input.uri=mongodb://127.0.0.1/"). \
#     config("spark.mongodb.output.uri=mongodb://127.0.0.1/"). \
#     config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\

spark = SparkSession.builder.appName("pyspark-mongo-connect").getOrCreate()

mongo_uri = "mongodb://127.0.0.1:27017/"
database = "practice_db"
collection = "myColl"


df = spark.read.format("mongo"). \
        option("uri", mongo_uri). \
        option("database", mongo_config['db_name']). \
        option("collection", mongo_config['collection']). \
        load()

df.show()

offset_json = df.toJSON().collect()[0]
offset_data = json.loads(offset_json)

print(offset_data)

# command to run this script - spark-submit filename.py

# jars used - 
# mongodb-driver-core-4.3.3.jar
# mongodb-driver-sync-4.3.4.jar
# mongo-spark-connector_2.12-3.0.0.jar
# bson-4.2.1.jar