from UTILS.KafkaConsumerFramework.RawDataConsumerOffsetMgmtUtils import fnLogMessage
import pymongo
import logging
# ------------------------------------------------------
# function name        : fnLogMessage
# function description : This function will log messages 
# function parameter   : log_level        -> Level of logging either ERROR/INFO
#                        log_message      -> Text message that needs to be logged
# ------------------------------------------------------

def fnLogMessage(log_level, log_message):
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

        # secretScopeKey = (
        #     connection['secretScopeKey']
        #     if 'secretScopeKey' in connection and connection['secretScopeKey']
        #     else connection['user']
        # )
        # connection['password'] = ''

        mongo_uri = (
            'mongodb://' + 
            connection['user'] +
            ':' + 
            connection['password'] +
            '@' + 
            (','.join(connection['shards'])) + 
            '/' +
            (
                connection['test_db']
                if 'test_db' in connection and connection['test_db']
                else 'test'
            ) + 
            '?' + 
            ('&'.join(connection['properties']))
        )

        mongo_config_map = {'mongo_uri': mongo_uri, 'db_name': db_name}
        
    except Exception as e:
        raise e
    
    return 'Hi'
        




# ------------------------------------------------------
# function name        : fnGetMongoUri_Distributed
# function description : This function will build the mongodb uri for connecting DB code to MongoDB in distributed mode
# function parameter   : connection -> Dictionary with all the required key value pairs for connection.
# ------------------------------------------------------

def fnGetMongoUri_Distributed(connection):
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

        secretScopeKey = (
            connection['secretScopeKey']
            if 'secretScopeKey' in connection and connection['secretScopeKey']
            else connection['user']
        )
        connection['password'] = ''

        mongo_uri = (
            'mongodb://' + 
            connection['user'] +
            ':' + 
            connection['password'] +
            '@' + 
            (','.join(connection['shards'])) + 
            '/' +
            (
                connection['test_db']
                if 'test_db' in connection and connection['test_db']
                else 'test'
            ) + 
            '?' + 
            ('&'.join(connection['properties']))
        )

        mongo_config_map = {'mongo_uri': mongo_uri, 'db_name': db_name}
    except Exception as e:
        raise e
    
    return mongo_config_map
        