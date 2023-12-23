from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import statistics
import pandas
import ast
import json

def read_df(spark, jdbc_url, table_name, jdbc_properties ):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
    return df


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", "postgresql-42.5.2.jar").config("spark.executor.extraClassPath","postgresql-42.5.2.jar") \
        .config("spark.local.dir", "/shared/hm31") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
    jdbc_properties = {
        "user": "hm31",
        "password": "graphs",
        "driver": "org.postgresql.Driver"
    }

    spark.sparkContext.setLogLevel("WARN")

    parquet_path = '/shared/hossein_hm31/embeddings_parquets'

    unique_nodes = read_df(spark, jdbc_url, 'hm31.unique_nodes_cert', jdbc_properties)
    df = spark.read.parquet(parquet_path)


    unique_nodes.createOrReplaceTempView("unique_nodes")
    df.createOrReplaceTempView("all_embeddings")

    sql_query = """
        select u.node_id, a.embedding from unique_nodes u inner join all_embeddings a on u.pmid = a.pmid
    """
    result =  spark.sql(sql_query)



    print("num", result.count())