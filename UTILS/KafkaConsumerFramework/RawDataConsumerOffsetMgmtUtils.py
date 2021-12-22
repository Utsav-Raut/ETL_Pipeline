import logging
# ------------------------------------------------------
# function name        : fnLogMessage
# function description : This function will log messages 
# function parameter   : log_level        -> Level of logging either ERROR/INFO
#                        log_message      -> Text message that needs to be logged
# ------------------------------------------------------

def fnLogMessage(log_level, log_message):
    logger = logging.getLogger("RawDataConsumerOffsetMgmtUtils")

    if(log_level == 'ERROR'):
        logging.basicConfig(filename = 'result.log', filemode = 'a', level = logging.ERROR)
        logger.error(log_message)
    else:   
        logging.basicConfig(filename = 'result.log', filemode = 'a', level = logging.INFO)
        logger.info(log_message)


# ------------------------------------------------------
# function name        : fnFetchDataFromDelta
# function description : This function will fetch data from raw tables as per the latest_processed_offset value in mongoDB
# function parameter   : topic_name        -> Topic name to fetch data from mongo db
#                        prod_grp          -> product group name to fetch data from mongodb
#                        collection_name   -> mongodb collection name
#                        mongo_config      -> mongodb configuration dictionary
#                        raw_data_consumer -> raw data consumer group, default value is None
# ------------------------------------------------------

def fnFetchDataFromDelta(topic_name, prod_grp, collection_name, mongo_config, raw_data_consumer = None):
    try:

        if(topic_name.strip() == ""):
            fnLogMessage('ERROR', 'Topic name is empty!')
            raise Exception('ERROR: -> fnFetchDataFromDelta: Topic name is empty!')

        if(prod_grp.strip() == ""):
            fnLogMessage('ERROR', 'Product group is empty!')
            raise Exception('ERROR: -> fnFetchDataFromDelta: Product group is empty!')

        if(collection_name.strip() == ""):
            fnLogMessage('ERROR', 'Collection name is empty!')
            raise Exception('ERROR: -> fnFetchDataFromDelta: Collection name is empty!')

        mongo_config_map = fnGetMongoUri(mongo_config)

    except Exception as e:
        raise e