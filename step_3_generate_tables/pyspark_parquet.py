from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as func
from pyspark.sql.types import StringType
import time


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

# df = df.repartition(2)
df = df.repartition(1)

df = df.dropDuplicates(['doi'])


init_count = df.count()

print(df.count(),'asoon! drop duplicates', time.time() - start)
# df.show(10)

# jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
# jdbc_properties = {
#     "user": "hm31",
#     "password": "graphs",
#     "driver": "org.postgresql.Driver"
# }
# mid = time.time()
# insert_table(df,jdbc_url,jdbc_properties)
# df.write.parquet("/shared/hossein_hm31/whole.parquet")
end = time.time()
print(f'elapsed {end - start} partition {df.rdd.getNumPartitions()}')

spark.stop()

