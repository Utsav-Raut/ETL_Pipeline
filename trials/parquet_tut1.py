from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("write_to_parquet").getOrCreate()


# def connect_to_sql(spark, jdbc_hostname, jdbc_port, database, data_table, username, password):
#         jdbc_url = "jdbc:mysql://{0}:{1}/{2}".format(jdbc_hostname, jdbc_port, database)

#         connection_details = {
#         "user": username,
#         "password": password,
#         "driver": "com.mysql.cj.jdbc.Driver",
#         }
#         df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
#         return df

# jdbc_hostname = 'root@localhost'
# jdbc_port = 3306
# database = 'ORG'
# data_table = 'Worker'
# username = 'root'
# password = 'MySqlPassword2020!'

# new_df = connect_to_sql(spark, jdbc_hostname, jdbc_port, database, data_table, username, password)
df = spark.read.format("jdbc"). \
        option("url","jdbc:mysql://root@localhost:3306/ORG"). \
        option("driver","com.mysql.cj.jdbc.Driver"). \
        option("dbtable","Worker"). \
        option("user","root"). \
        option("password","MySqlPassword2020!"). \
        load()

df.show()
df.write.parquet("file:///home/boom/Documents/programming/parquet_files/output/people.parquet")

# spark-submit --packages mysql:mysql-connector-java:8.0.26 parquet_tut1.py