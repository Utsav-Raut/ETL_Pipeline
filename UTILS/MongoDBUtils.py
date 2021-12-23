# -----------------------------------------------------------------------------------------------------------------
# function name                     :   fnGetMongoConnection
# function description              :   Getting mongo connection
# 
# function parameter                :   connection      -> Mongodb connection details
#                                       mongo_URI       -> mongo_URI needed for connecting to MongoDB
# 
# -----------------------------------------------------------------------------------------------------------------

def fnGetMongoConnection(connection: dict, mongo_URI: str):
    from pymongo import MongoClient

    client = MongoClient(mongo_URI)
    databaseName = connection['database']
    collectionName = connection['collection']

    return client[databaseName][collectionName]


# ------------------------------------------------------
# function name        : fnLogMessage
# function description : This function will log messages 
# function parameter   : log_level        -> Level of logging either ERROR/INFO
#                        log_message      -> Text message that needs to be logged
# ------------------------------------------------------

def fnLogMessage(log_level, log_message):
    import logging
    logger = logging.getLogger("MongoDBUtils")

    if(log_level == 'ERROR'):
        logging.basicConfig(filename = 'result.log', filemode = 'a', level = logging.ERROR)
        logger.error(log_message)
    else:   
        logging.basicConfig(filename = 'result.log', filemode = 'a', level = logging.INFO)
        logger.info(log_message)


# ------------------------------------------------------
# function name        : fnGetMongoUri
# function description : This function will build the mongodb uri for connecting DB code to MongoDB
# function parameter   : connection -> Dictionary with all the required key value pairs for connection.
# ------------------------------------------------------

def fnGetMongoUri(connection):
    try:
        if(not connection):
            fnLogMessage('ERROR', 'Configuration dictionary is empty!')
            raise Exception("Configuration dictionary is empty!")

        if not 'database' in connection:
            fnLogMessage('ERROR', 'database key is missing')
            raise KeyError('database key is missing')

        if not 'shards' in connection:
            fnLogMessage('ERROR', 'shards key is missing')
            raise KeyError('shards key is missing')

        if not 'test_db' in connection:
            fnLogMessage('ERROR', 'test_db key is missing')
            raise KeyError('test_db key is missing')
        
        if not 'properties' in connection:
            fnLogMessage('ERROR', 'properties key is missing')
            raise KeyError('properties key is missing')

        if not 'user' in connection:
            fnLogMessage('ERROR', 'user key is missing')
            raise KeyError('user key is missing')

        if not 'secretScopeName' in connection:
            fnLogMessage('ERROR', 'secretScopeName key is missing')
            raise KeyError('secretScopeName key is missing')

        db_name = connection["database"]

        mongo_uri = "mongodb://127.0.0.1:27017/"
        mongo_config_map = {'mongo_uri': mongo_uri, 'db_name': db_name}
        
    except Exception as e:
        raise e
    
    return mongo_config_map


# -----------------------------------------------------------------------------------------------------------------
# function name                     :   fnInsertMetaEntryToMongoDB
# function description              :   This function will help to insert initial entry to mongoDB value in mongoDB
# 
# function parameter                :   connection      -> Mongodb connection details
#                                       metadata        -> Entry that needs to be inserted
# ------------------------------------------------------------------------------------------------------------------

def fnInsertMetaEntryToMongoDB(connection: dict, metadata: dict):
    collection = fnGetMongoConnection(
        connection = connection,
        mongo_URI = fnGetMongoUri(
            connection = connection
        )['mongo_uri']
    )

    if isinstance(metadata, dict):

        filterDict = (
            {
                "_id":metadata['_id']
            }
        )

        updateResult = collection.replace_one(filter = filterDict, replacement= metadata, upsert=True)

    return updateResult

