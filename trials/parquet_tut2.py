from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("read_from_parquet").getOrCreate()

read_df = spark.read.parquet("file:///home/boom/Documents/programming/parquet_files/output/people.parquet")

read_df.show()