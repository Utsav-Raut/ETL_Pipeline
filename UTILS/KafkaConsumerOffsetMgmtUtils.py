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

        # Fetching data from kafka topic
        # -2 as an offset can be used to refer to earliest, -1 to latest.
        df_temp =   spark \
                    .read \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", offsetDict['brokers']) \
                    .option("failOnDataLoss", "false") \
                    .option("subscribe", topic_name) \
                    .option("startingOffsets", json.dumps(nextOffset)) \
                    .load()

        raw_table_path_temp = raw_table_path + "_" + offsetDict['_id'] + "_checkpoint"
        
        print('Checkpoint table path : ' + raw_table_path_temp)

        df_temp.repartition(1).write.mode('overwrite').format('parquet').save(raw_table_path_temp)

        print("Dataframe count from kafka :" + str(df_temp.count()))

        df = spark.read.format("parquet").load(raw_table_path_temp)

        print('Dataframe count from this checkpoint table :' + str(df.count()))

        if(len(df.head(1)) != 0):
            df_kafka_topic_data = df.selectExpr("CAST(key as STRING)",
                                                "CAST(value as STRING)",
                                                "CAST(topic as STRING)",
                                                "CAST(partition as INTEGER)",
                                                "CAST(offset as STRING)",
                                                "CAST(timestamp as STRING)")

            
            # Adding one more column "processed_timestamp" to the dataframe,
            # this will be used for retention policy
            df_topic_data = df_kafka_topic_data.withColumn("processed_timestamp", F.current_timestamp())

            fnLogMessage('INFO', "fnFetchDataFromKafkaTopic: Data fetched from topic.")

            # Delete the data from table as per the retention policy set
            fnDeleteTable(offsetDict['raw_table_name'], offsetDict.get('raw_table_retention', "15"))


            # Saving the kafka data to raw tables
            result = fnSaveToDeltaTable(offsetDict['raw_table_path'], offsetDict['raw_table_name'], df_topic_data)

            # Change the return true or false
            if(result):
                fnLogMessage('INFO', "fnFetchDataFromKafkaTopic: Data successfully written to delta table.")
                offsetValues = json.loads(fnSaveOffsets(df_topic_data, topic_name, prod_grp, collection_name, mongo_config))
                print("Offsets written to mongo out of kafka dataframe : " + str(offsetValues))
                fnLogMessage('INFO', "fnFetchDataFromKafkaTopic: offset saved successfully for topic " + topic_name)

                batch_ctrl_key = offsetDict['_id']
                stats = json.dumps(
                    {
                        "offsetValues": offsetValues,
                        "raw_table_path": offsetDict['raw_table_path'],
                        'raw_table_name': offsetDict['raw_table_name'],
                        "kafka_data_count": df_topic_data.count()
                    }
                )

                meta_args = {
                    "job_id" : job_id,
                    "job_start_time" : str(job_start_time),
                    "topic_name" : offsetDict['topic_name'],
                    "product_grp" : offsetDict['prod_grp'],
                }

                # audit_handler(

                # )

                fnLogMessage('INFO', 'fnFetchDataFromKafkaTopic: Audit entry done successfully for raw table '+offsetDict['raw_table_name'])

            else:
                fnLogMessage('INFO', 'fnFetchDataFromKafkaTopic: Issue while writing data to delta table.')
                raise Exception("ERROR: -> fnFetchDataFromKafkaTopic: Issue while writing data to delta table.")
        else:
            fnSaveToDeltaTable(offsetDict['raw_table_path'], offsetDict['raw_table_name'], None)
            fnLogMessage('ERROR', "fnFetchDataFromKafkaTopic: There is no new data present in the kafka topic '" + topic_name + "'")

            print('Notebook exit, exit job')


    except Exception as e:
        raise e
    
    fnLogMessage('INFO', 'fnFetchDataFromKafkaTopic: Fetched data from topic '+topic_name+" and saved to delta table "+offsetDict['raw_table_name']+ " successfully !")

    return df_topic_data


# -----------------------------------------------------------------------------------------------------------------
# function name                     :   fnSaveOffsets
# function description              :   This function will save the offsets of the topics in the offset control table in mongodb
# 
# function parameter                :   kafka_df        -> Dataframe that has the data from the kafka topic
#                                       topic_name      -> pass the name of the kafka topic
#                                       prod_grp        -> pass the product group name eg: ETL
#                                       collection_name -> pass the name of the mongodb collection where the kafka offset entry is done
#                                       mongo_config    -> pass the mongodb configuration directory
#                                       
# -----------------------------------------------------------------------------------------------------------------

def fnSaveOffsets(kafka_df, topic_name, prod_grp, collection_name, mongo_config):
    try:
        # Sanity check for parameters passed
        if(topic_name.strip() == ""):
            fnLogMessage('ERROR', 'fnSaveOffsets: Topic name value is empty !')
            raise Exception("ERROR: -> fnSaveOffsets: Topic name value is empty !")
        
        if(prod_grp.strip() == ""):
            fnLogMessage('ERROR', 'fnSaveOffsets: Product group value is empty !')
            raise Exception("ERROR: -> fnSaveOffsets: Product group value is empty !")
        
        if(collection_name.strip() == ""):
            fnLogMessage('ERROR', 'fnSaveOffsets: Collection name value is empty !')
            raise Exception("ERROR: -> fnSaveOffsets: Collection name value is empty !")

        if(len(kafka_df.head(1)) == 0):
            fnLogMessage('ERROR', 'fnSaveOffsets: Kafka dataframe is empty !')
            raise Exception("ERROR: -> fnSaveOffsets: Kafka dataframe is empty !")

        if(not mongo_config):
            fnLogMessage('ERROR', 'fnSaveOffsets: MongoDB Configuration dictionary is empty !')
            raise Exception("ERROR: -> fnSaveOffsets: MongoDB Configuration dictionary is empty !")

        # Getting the mongo config details from mongo_config
        mongo_config_map = fnGetMongoUri(mongo_config)

        # Sanity check for mongo config dictionary
        if(not mongo_config_map):
            fnLogMessage('ERROR', 'fnSaveOffsets: Mongo config dictionary is empty !')
            raise Exception("ERROR: -> fnSaveOffsets: Mongo config dictionary is empty !")

        # creating view for getting data from DF
        kafka_df.createOrReplaceTempView('kafka_data')

        # Getting the from_offset and till_offset data from kafka data
        offsetDataDF = spark.sql("""select map_from_arrays(collect_list(partition), collect_list(cast(till_offset as Int))) as till_offset,
                                            map_from_arrays(collect_list(partition), collect_list(cast(from_offset as Int))) as from_offset,
                                            '""" + topic_name + "_" + prod_grp + """' as _id,
                                            current_timestamp() as kafka_update_timestamp
                                            from(
                                            select max(cast(offset as Int)) as till_offset,
                                                    min(cast(offset as Int)) as from_offset,
                                                    cast(partition as String) as partition
                                                    from 
                                                    kafka_data
                                                    group by partition
                                                    order by partition) a""")

        offsetDataDF.write.format("mongo") \
                        .mode("append") \
                        .option("replaceDocument", "false") \
                        .option("uri", mongo_config_map.get('mongo_uri')) \
                        .option("database", mongo_config_map.get('db_name')) \
                        .option("collection", collection_name) \
                        .save()

        offsetValues = offsetDataDF.toJSON().collect()[0]

    except Exception as e:
        raise e

    fnLogMessage('INFO', "fnSaveOffsets: Kafka offset successfully updated")
    return offsetValues



    
# ******** THIS FUNCTION WILL WRITE TO MySQL, change params accordingly ********


# -----------------------------------------------------------------------------------------------------------------
# function name                     :   fnSaveToDeltaTable
# function description              :   This function will save the kafka data to delta table
# 
# function parameter                :   delta_table_path      -> Path of the raw delta table
#                                       delta_table_name      -> Raw delta table name
#                                       kafka_df              -> kafka dataframe that needs to be saved in delta table
# 
# function return                   :   False                  -> Returns False if there was an exception while writing data to delta
#                                       True                   -> Returns True if the data was written successfully to the delta
#                                       
# -----------------------------------------------------------------------------------------------------------------

def fnSaveToDeltaTable(delta_table_path, delta_table_name, kafka_df):
    try:
        if kafka_df:
            kafka_df_count = kafka_df.count()
            kafka_df.write.format("sql").mode("append").save(delta_table_path)
            print("fnSaveToDeltaTable: " + str(kafka_df_count) + " Records saved to delta table " + delta_table_name + " at path " + delta_table_path)

            spark.sql("""CREATE TABLE IF NOT EXISTS """ + delta_table_name + """ (`key` STRING, `value` STRING, `topic` STRING, `partition` INT, 
                        `offset` STRING, `timestamp` STRING, `processed_timestamp` TIMESTAMP) USING DELTA LOCATION '""" + delta_table_path+"""'""")
            
            fnLogMessage("INFO", "fnSaveToDeltaTable: Data saved to delta table "+delta_table_name+" at path "+delta_table_path)
    except Exception as e:
        fnLogMessage("ERROR ",e)
        print(e)
        return False
    return True


# -----------------------------------------------------------------------------------------------------------------
# function name                     :   fnDeleteTable
# function description              :   This function will delete data from delta table based on retention days passed
# 
# function parameter                :   raw_table_name      -> Raw table name for which data needs to be deleted
#                                       retention_days      -> Retention days needed for deleting the data from delta table

# -----------------------------------------------------------------------------------------------------------------

def fnDeleteTable(raw_table_name, retention_days):
    try:
        database = raw_table_name.split(".")[0]
        table_in_db = raw_table_name.split(".")[1].lower()
        tables = [table.tableName for table in spark.sql("show tables in "+database).collect()]
        if table_in_db in tables:
            spark.sql("""DELETE FROM """ + raw_table_name + """ where date(processed_timestamp) <= date_sub(current_date(), """ + str(retention_days) + """)""")
            fnLogMessage("INFO", "fnDeleteTable: " + raw_table_name + " data deleted as per retention policy of " + str(retention_days) + " days")
        else:
            fnLogMessage("INFO", "fnDeleteTable: No table found !")
    except Exception as e:
        raise e