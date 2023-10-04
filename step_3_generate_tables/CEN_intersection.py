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





if __name__ == "__main__":
    nodes_address = '../data/reformatted.tsv'

    schema = StructType([StructField("PMID", IntegerType(), False), StructField("cluster_id", IntegerType(), False)])
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", "postgresql-42.5.2.jar").config("spark.executor.extraClassPath","postgresql-42.5.2.jar") \
        .config("spark.local.dir", "/shared/hm31") \
        .config("spark.master", "local[*]")\
        .getOrCreate()

    cr = spark.read.schema(schema).option("sep", " ").csv(nodes_address)
    cr.show(100)

    jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
    jdbc_properties = {
        "user": "",
        "password": "",
        "driver": "org.postgresql.Driver"
    }

    start = time.time()
    #CEN.repartition(50).write.jdbc(url=jdbc_url, table='hm31.CEN_raw', mode="overwrite", properties=jdbc_properties)


    table_name = "hm31.pubmed_all_xmls_new"  # Replace with your actual table name
    # # Write the DataFrame to the PostgreSQL table
    paxn = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)

    paxn = paxn.repartition(1)  # Repartition 'paxn' into 100 partitions (adjust as needed)
    cr = cr.repartition(1)  # Repartition 'cr' into 100 partitions (adjust as needed)

    print("PAXN", paxn.rdd.getNumPartitions())
    print("CRIS", cr.rdd.getNumPartitions())

    # result = cr.join(paxn, cr["PMID"] == paxn["pmid"], "left")
    # result.show()
    #result = paxn.alias('paxn').join(cr.alias('cr'),(func.col('cr.PMID')==func.col('paxn.pmid')))
    result = cr.alias('cr').join(paxn.alias('paxn'),(func.col('cr.PMID')==func.col('paxn.pmid')), 'left')
    #result = paxn.alias('paxn').join(cr.alias('cr'), (func.col('paxn.PMID') == func.col('cr.pmid')), 'left')

    result.persist()
    selected_columns = [func.col("paxn." + col_name) for col_name in paxn.columns]
    result = result.select(*selected_columns)
    # result = paxn.alias('paxn').join(cr.alias('cr'), (func.col('cr.PMID') == func.col('paxn.pmid')))
    result.repartition(50).write.jdbc(url=jdbc_url, table='hm31.xml_intersection', mode="overwrite",
                                      properties=jdbc_properties)

    result.show()
    print(f'elapsed {time.time() - start}')










