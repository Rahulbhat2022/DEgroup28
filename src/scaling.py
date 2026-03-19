from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, explode
import time
  

def process_reddit_data(nr_of_executors, ex_cores):
    spark = SparkSession.builder \
        .appName("Reddit-Markov-Chain") \
        .master("yarn") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", str(ex_cores)) \
	.config("spark.executor.instances", str(nr_of_executors)) \
        .config("spark.sql.shuffle.partitions", str(200)) \
        .getOrCreate()

    sc = spark.sparkContext
    start = time.time()

    # Read slim dataset - use 20% sample
    df = spark.read.parquet("hdfs:///reddit/slim/") \
            .sample(fraction=0.02, seed=42)

    print(f"Sample row count: {df.count()}")
    # Step 1: Clean and split into words using Spark SQL functions
    words_df = df.select(
        split(
            regexp_replace(lower(col("content")), r'[^a-z\s]', ' '),
            '\s+'
        ).alias("words")
    ).filter(col("words").isNotNull())

    # Step 2: Extract bigrams using Python UDF on smaller data
    def get_bigrams(words):
        words = [w for w in words if len(w) > 2]
        return [f"{words[i]}_{words[i+1]}"
                for i in range(len(words)-1)]

    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType, StringType

    bigram_udf = udf(get_bigrams, ArrayType(StringType()))

    # Step 3: Explode bigrams and count
    bigram_counts = words_df \
        .withColumn("bigram", explode(bigram_udf(col("words")))) \
        .groupBy("bigram") \
        .count() \
        .orderBy("count", ascending=False)

    # Show top 20
    bigram_counts.show(20, truncate=False)

    # Save top 200k
    print("Saving model...")
    bigram_counts.limit(200000) \
        .write.mode("overwrite") \
        .parquet("hdfs:///reddit/model/")

    elapsed = time.time() - start
    print(f"Total time: {elapsed:.1f}s")
    spark.stop()
    return elapsed

def main():
    rows = []
    for nr_of_executors in [1,2,3]:
        for ex_cores in [1, 2]:
            time = process_reddit_data(nr_of_executors, ex_cores)
            rows.append({
                "Executors": nr_of_executors,
                "Cores per Executor": ex_cores,
                "Time (s)": time
            })
    print("Results:")
    for row in rows:
        print(row)

if __name__ == "__main__":
    main()
