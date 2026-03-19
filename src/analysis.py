from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, explode
import time

spark = SparkSession.builder \
    .appName("Reddit-Markov-Chain") \
    .master("yarn") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

sc = spark.sparkContext
start = time.time()
start_split = time.time()
# Read slim dataset - use 20% sample
df = spark.read.parquet("hdfs:///reddit/slim/") \
           .sample(fraction=0.2, seed=42)

print(f"Sample row count: {df.count()}")

# Step 1: Clean and split into words using Spark SQL functions
words_df = df.select(
    split(
        regexp_replace(lower(col("content")), r'[^a-z\s]', ' '),
        '\s+'
    ).alias("words")
).filter(col("words").isNotNull())
elapsed_split = time.time() - start_split
Extract_start = time.time()
# Step 2: Extract bigrams using Python UDF on smaller data
def get_bigrams(words):
    words = [w for w in words if len(w) > 2]
    return [f"{words[i]}_{words[i+1]}" 
            for i in range(len(words)-1)]

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

bigram_udf = udf(get_bigrams, ArrayType(StringType()))
time_udf = time.time() - Extract_start
exp_start = time.time()
# Step 3: Explode bigrams and count
bigram_counts = words_df \
    .withColumn("bigram", explode(bigram_udf(col("words")))) \
    .groupBy("bigram") \
    .count() \
    .orderBy("count", ascending=False)

# Show top 20
bigram_counts.show(20, truncate=False)
time_exp = time.time() - exp_start
model_start = time.time()
# Save top 200k
print("Saving model...")
bigram_counts.limit(200000) \
    .write.mode("overwrite") \
    .parquet("hdfs:///reddit/model/")
time_model= time.time() - model_start
elapsed = time.time() - start
print(f"Total time: {elapsed:.1f}s")
print(f"Time for splitting: {elapsed_split:.1f}s")
print(f"Time for UDF: {time_udf:.1f}s")
print(f"Time for counting: {time_exp:.1f}s")
print(f"Time for saving: {time_model:.1f}s")
spark.stop()
