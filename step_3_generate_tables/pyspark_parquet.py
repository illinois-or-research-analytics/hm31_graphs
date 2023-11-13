from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as func
from pyspark.sql.types import StringType
import time
import os


def insert_table(result, jdbc_url, jdbc_properties):
    result.repartition(1).write.jdbc(url=jdbc_url, table='hm31.xml_baseline_full', mode="overwrite",properties=jdbc_properties)
    #result.repartition(1).write.jdbc(url=jdbc_url, table='hm31.xml_baseline_full', mode="overwrite",properties=jdbc_properties)




spark = SparkSession.builder \
        .appName("parquet_unification") \
        .config("spark.executor.memory", "10g") \
        .getOrCreate()
    # spark.conf.set("spark.executor.memory", "4g")


parquet_path = '/shared/hossein_hm31/pubmed_parquet/'
parquet_path = '/shared/hossein_hm31/test/'

df = spark.read.parquet(parquet_path)
spark.sparkContext.setLogLevel("WARN")

df.printSchema()
start = time.time()

df = df.repartition(1)
df = df.dropDuplicates(['doi'])
init_count = df.count()
mid = time.time()

print(df.count(),'asoon! drop duplicates', mid - start)
df.createOrReplaceTempView("temp_table")

sql_query = """
       WITH RankedRows AS (
           SELECT
               *,
               ROW_NUMBER() OVER (PARTITION BY PMID ORDER BY date_completed DESC) AS RowRank
           FROM
               temp_table
       )
       SELECT
           *
       FROM
           RankedRows
       WHERE
           RowRank = 1
"""

result_df = spark.sql(sql_query)
result_df = result_df.drop("RowRank")

    # result_df = result_df.drop("RowRank")
end = time.time()

print(f'finalllll {end - mid} count: {result_df.count()}')


spark.stop()

    # Add a delay (adjust the duration as needed)

    # Print a message to indicate the script has completed
print("Script completed successfully.")


