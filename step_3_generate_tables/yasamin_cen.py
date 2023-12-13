
import time
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col

import sys

#Read the CEN from the tsv file
def read_CEN(file_path, spark):
    schema = StructType([StructField("PMID", IntegerType(), False), StructField("cluster_id", IntegerType(), False)])
    cr = spark.read.schema(schema).option("sep", " ").csv(file_path)
    return cr


#Read a df from db
def read_df(spark, jdbc_url, table_name, jdbc_properties ):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
    return df


#Using George_pipeline that maps dois and PMIDS, we use the dimensions_complete_nodelist to get the subset
#intersection of dimensions and baseline that have full metadata
def calculate_node_id_to_pmid_mapping(cen_nodes, george_df, full = False):
    dimensions_df =  read_df(spark, jdbc_url, 'dimensions.exosome_dimensions_complete_nodelist', jdbc_properties)
    george_df.createOrReplaceTempView("george_pipeline")
    dimensions_df.createOrReplaceTempView("exosome_dimensions")

    print("george partition", george_df.rdd.getNumPartitions())
    print("dimensions_df partition", dimensions_df.rdd.getNumPartitions())

    if full == False: #Take with partial details
        # Register DataFrames as temporary tables to be used in SQL queries
        # Run the SQL query
        result_df = spark.sql("""
            SELECT d.integer_id
            FROM george_pipeline gp
            JOIN exosome_dimensions d
            ON gp.doi = lower(d.doi)
        """)
        write_df(result_df, jdbc_url, 'hm31.node_id_to_pmid_partial', jdbc_properties)

        # result_df.show(10)



    else: #Take only those with full details mesh, title, abstract and year
        result_df = spark.sql("""
            SELECT d.integer_id, gp.PMID
            FROM george_pipeline gp
            JOIN exosome_dimensions d
            ON gp.doi = lower(d.doi)
            WHERE gp.has_abstract = 1 AND gp.has_title = 1 AND gp.has_mesh = 1 AND gp.has_year = 1
        """)

        print("after db operation")
        write_df(result_df, jdbc_url, 'hm31.node_id_to_pmid_full', jdbc_properties)



    return result_df

def save_table_into_parquet(table_name, saving_directory):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
    df.write.parquet(f'{saving_directory}{table_name.split(".")[-1]}.parquet')



#Write a df into the db
def write_df(result, jdbc_url, table_name, jdbc_properties):
    result.repartition(50).write.jdbc(url=jdbc_url, table=table_name, mode="overwrite",
                                      properties=jdbc_properties)

#read edges from a tsv file or from the database
def read_EDGES(file_path, spark, read = True):
    if read == False:
        schema = StructType([StructField("first", IntegerType(), False), StructField("second", IntegerType(), False),StructField("weight", FloatType(), True)])
        edges = spark.read.schema(schema).option("sep", "\t").csv(file_path)
        edges.repartition(10).write.jdbc(url=jdbc_url, table='hm31.cen_raw_edges', mode="overwrite",properties=jdbc_properties)

    else:
        df =  read_df(spark, jdbc_url, 'hm31.cen_raw_edges', jdbc_properties )
        return df



    return edges

#Filter edges of CEN to a list of node_id1 node_id to but to those within dimensions and full metadata
def filter_edges(edge, map):
    print('prior_count', edge.count())

    edges = edge.repartition(1)
    mapping = map.repartition(1)
    # Register DataFrames as temporary tables to be used in SQL queries
    edges.createOrReplaceTempView("edges_table")
    mapping.createOrReplaceTempView("mapping_table")

    # Write a Spark SQL query to achieve the filtering
    filtered_edges = spark.sql("""
        SELECT e.first, e.second
        FROM edges_table e
        LEFT JOIN mapping_table m1
        ON e.first = m1.integer_id
        LEFT JOIN mapping_table m2
        ON e.second = m2.integer_id
        WHERE m1.integer_id IS NOT NULL AND m2.integer_id IS NOT NULL
    """)

    # Display the count of filtered edges
    print('filtered_count', filtered_edges.count())

    # Assuming you have a write_df function to write the result back to a table
    write_df(filtered_edges, jdbc_url, 'hm31.cen_intersection_edges', jdbc_properties)

    return filtered_edges

#Fitler nodes of cen to those with metadata
def filter_nodes():
    cen_raw = read_df(spark, jdbc_url, 'hm31.cen_raw', jdbc_properties)
    map = read_df(spark, jdbc_url, 'hm31.node_id_to_pmid_full', jdbc_properties)

    cen_raw = cen_raw.repartition(1)
    map = map.repartition(1)


    cen_raw.createOrReplaceTempView("cen_raw")
    map.createOrReplaceTempView("map")

    result_df = spark.sql("""
            SELECT c.node_id, m.PMID
            FROM cen_raw c
            INNER JOIN map m
            ON c.node_id = m.integer_id
        """)

    write_df(result_df, jdbc_url, 'hm31.cen_raw_nodes', jdbc_properties)



#Read cen nodes from table and dump into csv
def read_and_dump_cen_nodes(spark, jdbc_url, jdbc_properties):
    cen_raw = read_df(spark, jdbc_url, 'hm31.cen_raw', jdbc_properties)
    cen_raw.write.option("sep", "\t").csv('/home/hm31/step_1/data/reformatted.tsv', header=True, sep='\t', mode='overwrite')





if __name__ == "__main__":
    # nodes_address = '../data/reformatted.tsv'
    # edges_address = '../data/CEN.tsv'

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

    #read_and_dump_cen_nodes(spark, jdbc_url, jdbc_properties)
    # cen = read_CEN(nodes_address, spark)
    # george_df = read_df(spark, jdbc_url, 'hm31.george_pipeline', jdbc_properties )


    # filter_nodes()
    #calculate_node_id_to_pmid_mapping(cen, george_df, full = False) #10484769
    #calculate_node_id_to_pmid_mapping(cen, george_df, full = True) #9304726

    #Obtain mapping
    # node_id_to_pmid_mapping = read_df(spark,jdbc_url,'hm31.node_id_to_pmid_full',jdbc_properties)
    #
    # #Obtain edges
    # edges = read_EDGES(edges_address,spark)
    #
    # filter_edges(edges,node_id_to_pmid_mapping)

    save_table_into_parquet('hm31.cen_raw_nodes', '/shared/pubmed/')





















