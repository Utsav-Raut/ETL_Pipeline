from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("connect_to_mysql").getOrCreate()

df = spark.read.format("jdbc"). \
        option("url","jdbc:mysql://localhost/ORG"). \
        option("driver","com.mysql.jdbc.Driver"). \
        option("dbtable","Worker"). \
        option("user","root"). \
        option("password","MySqlPassword2020!"). \
        load()


df.show()

# put the following jars in the spark jars folder --
# mysql-connector-java-8.0.26.jar
# protobuf-java-2.5.0.jar
