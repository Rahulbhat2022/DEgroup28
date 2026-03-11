from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Reddit JSON to Parquet") \
    .getOrCreate()

df = spark.read.json("hdfs:///reddit/raw/corpus-webis-tldr-17.json")

df = df.limit(100000)

df = df.repartition(4)

df.write.mode("overwrite").parquet("hdfs:///reddit/parquet")

spark.stop()
