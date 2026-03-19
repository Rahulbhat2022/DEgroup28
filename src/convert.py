from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Reddit JSON to Parquet") \
    .master("yarn") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.shuffle.partitions", "12") \
    .getOrCreate()

# Read entire JSON file
df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("lineSep", "\n") \
    .json("hdfs:///reddit/raw/corpus-webis-tldr-17.json")

# Print schema and row count
df.printSchema()
print(f"Total rows: {df.count()}")

# Write full dataset as Parquet
df.write.mode("overwrite").parquet("hdfs:///reddit/parquet/")

print("Done! Parquet written to HDFS.")
spark.stop()
