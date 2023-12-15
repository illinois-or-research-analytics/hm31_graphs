from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import time

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ngrams", help="some useful description.")
args = parser.parse_args()
ngrams = args.ngrams


print("kiiiiiiir", ngrams)


def insert_table(result, jdbc_url, jdbc_properties):
    result.repartition(10).write.jdbc(url=jdbc_url, table='hm31.george_pipeline_50', mode="overwrite",properties=jdbc_properties)




spark = SparkSession.builder \
    .appName("parquet_unification") \
    .config("spark.executor.memory", "10g") \
    .getOrCreate()


parquet_path = '/shared/hossein_hm31/pubmed_parquet/'



df = spark.read.parquet(parquet_path)
spark.sparkContext.setLogLevel("WARN")

# df.printSchema()
start = time.time()

df = df.repartition(10)
df = df.dropDuplicates(['doi'])
init_count = df.count()
mid = time.time()

# print(df.count(),'asoon! drop duplicates', mid - start)
df.createOrReplaceTempView("temp_table")

sql_query = """
       WITH RankedRows AS (
           SELECT
               *,
               ROW_NUMBER() OVER (PARTITION BY PMID ORDER BY date_revised DESC) AS RowRank
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
result_df = result_df.filter(col("doi") != '')


after_query = time.time()
# print(f'run query in {after_query - mid}')

george_df = result_df.withColumn("has_abstract", F.when(F.length("abstract") > 0, 1).otherwise(0))
george_df = george_df.withColumn("has_title", F.when(F.length("title") > 0, 1).otherwise(0))
george_df = george_df.withColumn("has_mesh", F.when(F.length("mesh") > 0, 1).otherwise(0))
george_df = george_df.withColumn("has_year", F.when(F.length("year") > 0, 1).otherwise(0))

george_df = george_df.select("PMID", "doi", "has_abstract", "has_title", "has_mesh", "has_year")
george_df = george_df.withColumnRenamed("PMID", "pmid")






jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
jdbc_properties = {
    "user": "hm31",
    "password": "graphs",
    "driver": "org.postgresql.Driver"
}

insert_table(george_df, jdbc_url, jdbc_properties)

end = time.time()
print(f'Inserted into table: Elapsed time: {end - start} number of rows: {result_df.count()}')


spark.stop()



