# importing module
import pyspark
import json
from pyspark.sql import SparkSession
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def read_csv(file_path, spark):
    #schema = StructType([StructField("PMID", IntegerType(), False), StructField("cluster_id", IntegerType(), False)])

   # cr = spark.read.schema(schema).option("sep", " ").csv(file_path)
    cr = spark.read.option("header", "true").csv(file_path)
    return cr


def read_df(spark, jdbc_url, table_name, jdbc_properties ):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
    return df


def write_df(result, jdbc_url, table_name, jdbc_properties):
    result.repartition(50).write.jdbc(url=jdbc_url, table=table_name, mode="overwrite",
                                      properties=jdbc_properties)


def read_EDGES(file_path, spark):
    schema = StructType([StructField("first", IntegerType(), False), StructField("second", IntegerType(), False),StructField("weight", FloatType(), True)])

    edges = spark.read.schema(schema).option("sep", "\t").csv(file_path)
    edges.show()

    edges.repartition(10).write.jdbc(url=jdbc_url, table='hm31.cen_raw_edges', mode="overwrite",properties=jdbc_properties)

    return edges

def filter_edges(edges, xml):
    print('prior_count', edges.count())
    edges = edges.repartition(1)
    xml = xml.repartition(1)


    result = edges.alias('edges').join(xml.alias('xml'), (func.col('edges.first') == func.col('xml.node_id')), 'inner').alias('joined_edges').\
            join(xml.alias('xml'), (func.col('joined_edges.second') == func.col('xml.node_id')), 'inner').alias('final_edges')

    result.persist()
    selected_columns = [func.col("final_edges." + col_name) for col_name in edges.columns]
    result = result.select(*selected_columns)

    print('post_count', result.count())
    result.show()

    write_df(result, jdbc_url, 'hm31.CEN_intersection_edges_correct', jdbc_properties)


def create_CEN_intersection_table(spark, jdbc_url, jdbc_properties):
    cr = read_CEN(nodes_address, spark)
    cr.show()
    paxn = read_df(spark, jdbc_url, 'hm31.pubmed_all_xmls_new', jdbc_properties)

    paxn = paxn.repartition(1)  # Repartition 'paxn' into 100 partitions (adjust as needed)
    cr = cr.repartition(1)  # Repartition 'cr' into 100 partitions (adjust as needed)

    # print("PAXN", paxn.rdd.getNumPartitions())
    # print("CRIS", cr.rdd.getNumPartitions())

    result = cr.alias('cr').join(paxn.alias('paxn'), (func.col('cr.PMID') == func.col('paxn.pmid')), 'inner')

    result.persist()
    selected_columns = [func.col("paxn." + col_name) for col_name in paxn.columns]
    result = result.select(*selected_columns)
    result.repartition(50).write.jdbc(url=jdbc_url, table='hm31.xml_intersection', mode="overwrite",
                                      properties=jdbc_properties)
    result.show()
    print("Hey", result.count())

if __name__ == "__main__":
    george_file_address = '/shared/pubmed/pmid_doi.csv'

    jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
    jdbc_properties = {
        "user": "hm31",
        "password": "graphs",
        "driver": "org.postgresql.Driver"
    }


    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", "postgresql-42.5.2.jar").config("spark.executor.extraClassPath","postgresql-42.5.2.jar") \
        .config("spark.local.dir", "/shared/hm31") \
        .config("spark.master", "local[*]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    george_csv = spark.read.option("header", "true").csv(george_file_address)
    pre_drop = george_csv.count()

    george_csv = george_csv.na.drop(subset=["doi"]).repartition(4)
    post_drop = george_csv.count()
    hossein_df = read_df(spark, jdbc_url, 'hm31.george_pipeline', jdbc_properties).repartition(4)


    result_df = hossein_df.select("pmid").subtract(george_csv.select("pmid"))
    print(f'hossein - george {result_df.count()}')
    result_df.show(10)















