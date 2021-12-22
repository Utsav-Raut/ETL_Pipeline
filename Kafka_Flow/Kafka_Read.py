import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('Reading from Kafka').getOrCreate()

brokers = "localhost:9092"
topic = "TestTopic"
key = "RecordCreate"

# Reading data------------------------------------------------
df_read = spark.read.format("kafka"). \
    option("kafka.bootstrap.servers", brokers). \
    option("subscribe", topic). \
    option("startingOffsets", "earliest"). \
    load()

df_topic_data = df_read.selectExpr("CAST(key as STRING)",
                                    "CAST(value as STRING)",
                                    "CAST(topic as STRING)",
                                    "CAST(partition as STRING)",
                                    "CAST(offset as STRING)",
                                    "CAST(timestamp as STRING)"
                                    )
df_topic_data.createOrReplaceTempView('vw_kafka')

spark.sql("select * from vw_kafka order by offset desc").show()
