from pandarallel import pandarallel
from pyspark.sql import SparkSession

import time
import pandas





spark = SparkSession.builder \
    .appName("Parquet to CSV") \
    .getOrCreate()


parquet_file_path = "files/edges/edges.parquet"
df = spark.read.parquet(parquet_file_path)

columns_to_drop = ["id", "weight"]
df = df.drop(*columns_to_drop)

print(f'looool {df.count()}')

csv_file_path = "CENM_dir"
print(df.head(20))
df.coalesce(1).write.option("header", "true").csv(csv_file_path)


spark.stop()