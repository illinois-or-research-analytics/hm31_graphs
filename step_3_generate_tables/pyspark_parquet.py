from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("parquet_unification").getOrCreate()
parquet_path = '/shared/hossein_hm31/pubmed_parquet/'
df = spark.read.parquet(parquet_path)

df.printSchema()

# Show the first few rows of the DataFrame
print(df.head())

init_count = df.count()
# print(df.count(),'asoon!')

count_condition = (col("doi") != '') & (col("doi").isNotNull())
count_result = df.filter(count_condition).count()

print(f"Count where len(doi) > 0 or doi != '': {count_result} init count {init_count}")