import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('Starting Kafka stuff').getOrCreate()

brokers = "localhost:9092"
topic = "TestTopic"
key = "RecordCreate"

# Loading data-------------------------------------------
# new_data = [{"userId": 2474, "addressLine1": "1600 Amphitheatre Parkway"}]
# df = spark.createDataFrame(new_data)
# df.show()

# df_kafka = df.withColumn('value', F.to_json(F.struct([df[x] for x in df.columns]))).withColumn('topic', F.lit(topic)).withColumn('key', F.lit(key))

# df_kafka.selectExpr('topic', 'CAST(key as STRING)', 'CAST(value as STRING)') \
# .write \
# .format('kafka') \
# .option('kafka.bootstrap.servers', brokers) \
# .save()

# print('Loaded ' + str(df_kafka.count()) + ' messages into ' + str(topic) + ', with key as : ' + str(key))



