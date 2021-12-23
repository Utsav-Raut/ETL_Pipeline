import datetime as dt
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

from MongoDBUtils import fnGetMongoUri

spark = SparkSession.builder.appName("etl-spark-instance").getOrCreate()

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


# -----------------------------------------------------------------------------------------------------------------
# function name                     :   fnGetLastCommittedOffsets
# function description              :   This function is responsible for getting the last committed offset value of
#                                       of a topic
# 
# function parameter                :   topic_name      -> pass the name of the kafka topic
#                                       prod_grp        -> pass the product group name e.g: ETL
#                                       collection_name -> pass the name of the mongodb collection where the kafka offset entry is done
#                                       mongo_config    -> pass the mongodb configuration dictionary
# -----------------------------------------------------------------------------------------------------------------

def fnGetLastCommittedOffsets(topic_name, prod_grp, collection_name, mongo_config):
    try:

        # Sanity check for parameters passed
        if(topic_name.strip() == ""):
            fnLogMessage('ERROR', 'fnGetLastCommittedOffsets: Topic name value is empty !')
            raise Exception("ERROR: -> fnGetLastCommittedOffsets: Topic name value is empty !")
        
        if(prod_grp.strip() == ""):
            fnLogMessage('ERROR', 'fnGetLastCommittedOffsets: Product group value is empty !')
            raise Exception("ERROR: -> fnGetLastCommittedOffsets: Product group value is empty !")
        
        if(collection_name.strip() == ""):
            fnLogMessage('ERROR', 'fnGetLastCommittedOffsets: Collection name value is empty !')
            raise Exception("ERROR: -> fnGetLastCommittedOffsets: Collection name value is empty !")

        if(not mongo_config):
            fnLogMessage('ERROR', 'fnGetLastCommittedOffsets: MongoDB Configuration dictionary is empty !')
            raise Exception("ERROR: -> fnGetLastCommittedOffsets: MongoDB Configuration dictionary is empty !")

        sleep_time = 30
        num_of_retry = 3

        # Getting the mongo config details from MONGO_CONFIG
        mongo_config_map = fnGetMongoUri(mongo_config)

        # This pipeline will be used to filter data from the mongodb collection
        pipeline = (
            """
            [
                {
                    "$match" : {
                        "_id" : \""""+topic_name+"_"+prod_grp+"""\"
                    }
                }
            ]
            """
        )

        for i in range(num_of_retry):
            try:
                kafkaOffsetDF = spark.read.format("mongo") \
                                .option("pipeline", pipeline) \
                                .option("uri", mongo_config_map['mongo_uri']) \
                                .option("database", mongo_config_map['db_name']) \
                                .option("collection", collection_name) \
                                .load()
            except Exception as e:
                if "com.mongodb.MongoTimeoutException" in str(e):
                    print("MongoDB connection timeout, retry will be attempted in 30 seconds again !")
                    print("Retry attempt no. "+str(i+1))
                    time.sleep(sleep_time)
                    if i == (num_of_retry - 1):
                        raise e
                    continue
                else:
                    raise e
            break

        # Check if the topic metadata entry is made in mongodb
        if(kafkaOffsetDF.count() == 0):
            fnLogMessage('ERROR', 'fnGetLastCommittedOffsets: No metadata entry found for the topic \'' + topic_name +'\' in mongodb !')
            raise Exception('ERROR: -> fnGetLastCommittedOffsets: No metadata entry found for the topic \'' + topic_name +'\' in mongodb !')

        # Fetching the first value from the dataframe
        till_Offset = kafkaOffsetDF.toJSON().collect()[0]
    
    except Exception as e:
        raise e

    return till_Offset


# -----------------------------------------------------------------------------------------------------------------
# function name                     :   fnFetchDataFromKafkaTopic
# function description              :   This function will fetch data from the given topic
# 
# function parameter                :   topic_name      -> pass the name of the kafka topic
#                                       prod_grp        -> pass the product group name e.g: ETL
#                                       collection_name -> pass the name of the mongodb collection where the kafka offset entry is done
#                                       mongo_config    -> pass the mongodb configuration dictionary
# -----------------------------------------------------------------------------------------------------------------

def fnFetchDataFromKafkaTopic(topic_name, prod_grp, collection_name, mongo_config):
    try:
        job_start_time = dt.datetime.now()
        job_id = job_start_time.strftime("%Y%m%d%H%M%S")

        meta_args = {
            "job_id" : job_id,
            "job_start_time" : str(job_start_time),
            "topic_name" : topic_name,
            "product_grp" : prod_grp,
        }

        batch_ctrl_key = topic_name + "_" + prod_grp
        stats = json.dumps({})

        # Loading the json string
        mongo_config = json.loads(mongo_config)

        # Sanity check for parameters passed
        if(topic_name.strip() == ""):
            fnLogMessage('ERROR', 'fnFetchDataFromKafkaTopic: Topic name value is empty !')
            raise Exception("ERROR: -> fnFetchDataFromKafkaTopic: Topic name value is empty !")
        
        if(prod_grp.strip() == ""):
            fnLogMessage('ERROR', 'fnFetchDataFromKafkaTopic: Product group value is empty !')
            raise Exception("ERROR: -> fnFetchDataFromKafkaTopic: Product group value is empty !")
        
        if(collection_name.strip() == ""):
            fnLogMessage('ERROR', 'fnFetchDataFromKafkaTopic: Collection name value is empty !')
            raise Exception("ERROR: -> fnFetchDataFromKafkaTopic: Collection name value is empty !")

        if(not mongo_config):
            fnLogMessage('ERROR', 'fnFetchDataFromKafkaTopic: MongoDB Configuration dictionary is empty !')
            raise Exception("ERROR: -> fnFetchDataFromKafkaTopic: MongoDB Configuration dictionary is empty !")

        # Loaded all the columns from mongodb
        offsetDict = json.loads(fnGetLastCommittedOffsets(topic_name, prod_grp, collection_name, mongo_config))
        
        print("MongoDB metadata : " + str(offsetDict))
        raw_table_path = offsetDict['raw_table_path']
        print('Raw table path : ' + raw_table_path)

        if(not offsetDict):
            fnLogMessage('ERROR', 'fnFetchDataFromKafkaTopic: NextOffset value is empty !')
            raise Exception("ERROR: -> fnFetchDataFromKafkaTopic: NextOffset value is empty !")

        if(offsetDict['brokers'].strip() == ""):
            fnLogMessage('ERROR', 'fnFetchDataFromKafkaTopic: Broker list value is empty !')
            raise Exception("ERROR: -> fnFetchDataFromKafkaTopic: Broker list value is empty !")

        # if(offsetDict['raw_table_name'].strip() == ""):
        #     fnLogMessage('ERROR', 'fnFetchDataFromKafkaTopic: Raw table name value is empty !')
        #     raise Exception("ERROR: -> fnFetchDataFromKafkaTopic: Raw table name value is empty !")


        # if(offsetDict['raw_table_path'].strip() == ""):
        #     fnLogMessage('ERROR', 'fnFetchDataFromKafkaTopic: Raw table path value is empty !')
        #     raise Exception("ERROR: -> fnFetchDataFromKafkaTopic: Raw table path value is empty !")

        # Getting the offset value from the dictionary
        nextOffset = {offsetDict['topic_name']:offsetDict['till_offset']}

        # Logic to check and update offset in case if its not the first run.
        for k,v in nextOffset[topic_name].items():
            if(v != 0):
                nextOffset[topic_name][k] = v+1

        infoStr = "fnFetchDataFromKafkaTpic : Next offset for reading topic " + topic_name + " is " + str(nextOffset)
        fnLogMessage('INFO', infoStr)
        
        fnLogMessage('INFO', 'fnFetchDataFromKafkaTopic: Starting to fetch data from kafka topic.')
        print('nextOffset : ' + str(nextOffset))

    except Exception as e:
        print(e)