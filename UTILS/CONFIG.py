mongo_connect = {
    "database": "dbControl",
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

mongo_config = {
    "db_name": "practice_db",
    "collection": "myColl"
}
kafka_offset_collection_name = "kafkaOffsetMgmt"