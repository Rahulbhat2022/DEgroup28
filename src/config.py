# Central configuration for the pipeline

# HDFS paths
HDFS_BASE = "hdfs:///reddit"
HDFS_SLIM = f"{HDFS_BASE}/slim"
HDFS_MODEL = f"{HDFS_BASE}/model"

# Spark settings
EXECUTOR_MEMORY = "1g"
DRIVER_MEMORY = "1g"
EXECUTOR_CORES = 1
SHUFFLE_PARTITIONS = 12

# Analysis settings
SAMPLE_FRACTION = 0.2
SAMPLE_SEED = 42
TOP_BIGRAMS = 200000
