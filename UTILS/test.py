import MongoDBUtils
import KafkaConsumerOffsetMgmtUtils
import json
# from datetime import datetime


# kafka_brokers = "192.168.26.25:9092, 192.168.26.24:9092, 192.168.26.23:9092"
kafka_brokers = "localhost:9092"
kafka_offset_collection = "kafkaOffsetMgmt"

mongo_connect = {
    "database": "dbControl",
    "collection": kafka_offset_collection,
    "shards": [
        "sim-non-prod-shared-00-00.c7pz8.azure.mongodb.net:27017",
        "sim-non-prod-shared-00-01.c7pz8.azure.mongodb.net:27017",
        "sim-non-prod-shared-00-02.c7pz8.azure.mongodb.net:27017"
    ],
    "test_db": "test",
    "properties": [
        "ssl=true",
        "replicaSet=sim-non-prod-shard-0",
        "authSource=admin",
        "retryWrites=true",
        "w=majority"
    ],
    "user": "dbControlDev",
    "secretScopeName": "dev-keyValut",
    "secretScopeKey": None
}

collection_name = "kafkaOffsetMgmt"


# metadata_entry = {
#     "_id" : "ecomm_item_nrt_ETL",
#     "topic_name" : "ecomm_item_nrt",
#     "product_grp" : "ETL",
#     "from_offset" : {
#         "0" : 0
#     },
#     "till_offset" : {
#         "0" : -3
#     },
#     "brokers" : kafka_brokers,
#     "raw_table_name" : "",
#     "raw_table_path" : "",
#     "raw_table_retention" : 30,
#     "last_processed_offset" : {
#         "0" : 0
#     },
#     "kafka_update_timestamp" : datetime.now(),
#     "processed_update_timestamp" : datetime.now(),
# }

# print(metadata_entry)

# MongoDBUtils.fnInsertMetaEntryToMongoDB(mongo_connect, metadata_entry)

mongo_config = json.dumps(mongo_connect, default=str)
KafkaConsumerOffsetMgmtUtils.fnFetchDataFromKafkaTopic("ecomm_item_nrt", "ETL", collection_name, mongo_config)

# topic_name = "ecomm_item_nrt"
# prod_grp = "ETL"
# pipeline = (
#             """
#             [
#                 {
#                     "$match" : {
#                         "_id" : \""""+topic_name+"_"+prod_grp+"""\"
#                     }
#                 }
#             ]
#             """
#         )

# print(pipeline)