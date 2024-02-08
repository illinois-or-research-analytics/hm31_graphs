from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import time

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--tname", help="schema.table_name. Like hm31.george_pipeline")
parser.add_argument("--user", help="user of the db. Like hm31")
parser.add_argument("--pas", help="password of the db")
parser.add_argument("--path", help="path to the directory containing distributed parquet files")


args = parser.parse_args()
tname = args.tname
user = args.user
pas = args.pas
parquet_path = args.path



if parquet_path[-1] != '/':
    parquet_path += '/'




def insert_table(result, jdbc_url, jdbc_properties, tname):
    result.repartition(10).write.jdbc(url=jdbc_url, table=tname, mode="overwrite",properties=jdbc_properties)

def write_df(result, table_name, jdbc_properties):
    result = result.repartition(10)
    result.write.format('jdbc').options(
        url=jdbc_properties['jdbc_url'],
        driver="org.postgresql.Driver",
        user=jdbc_properties["user"],
        password=jdbc_properties["password"],
        dbtable=table_name,
        mode="overwrite"
    ).save()



# jdbc_properties = {
#     "user": user,
#     "password": pas,
#     "driver": "org.postgresql.Driver"
# }
# jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"


spark = SparkSession.builder \
        .appName("parquet_unification") \
        .config("spark.executor.memory", "10g") \
        .getOrCreate()





df = spark.read.parquet(parquet_path)
df.persist()

print("loaded parquet files")

spark.sparkContext.setLogLevel("WARN")

# df.printSchema()
start = time.time()

df = df.repartition(1)
df = df.dropDuplicates(['doi'])
init_count = df.count()
print(f"dropped duplicate doi total {init_count}")
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
init_count = df.count()
print(f"ran queries {init_count}")


after_query = time.time()
# print(f'run query in {after_query - mid}')s

george_df = result_df.withColumn("has_abstract", F.when(F.length("abstract") > 0, 1).otherwise(0))
george_df = george_df.withColumn("has_title", F.when(F.length("title") > 0, 1).otherwise(0))
george_df = george_df.withColumn("has_mesh", F.when(F.length("mesh") > 2, 1).otherwise(0))
george_df = george_df.withColumn("has_year", F.when(F.length("year") > 0, 1).otherwise(0))

george_df = george_df.select("PMID", "doi", "has_abstract", "has_title", "has_mesh", "has_year", 'mesh', 'year')
george_df = george_df.withColumnRenamed("PMID", "pmid")
george_df = george_df.drop("mesh")
george_df = george_df.drop("year")



init_count = df.count()
print(f" converted statistics {init_count}")




jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"

jdbc_properties = {
    "user": user,
    "password": pas,
    "driver": "org.postgresql.Driver"
}

#New insertion stuff
# jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
#
# jdbc_properties = {
#     "user": "hm31",
#     "password": "graphs",
#     "driver": "org.postgresql.Driver",
#     'jdbc_url' : "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
# }
# write_df(george_df, tname, jdbc_properties)


insert_table(george_df, jdbc_url, jdbc_properties, tname)

end = time.time()
# print(f'Inserted into table: Elapsed time: {end - start} number of rows: {result_df.count()}')


spark.stop()



