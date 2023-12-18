from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col


#Read a df from db
def read_df(spark, jdbc_url, table_name, jdbc_properties ):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
    return df

#Write a df into the db
def write_df(result, jdbc_url, table_name, jdbc_properties):
    result.repartition(50).write.jdbc(url=jdbc_url, table=table_name, mode="overwrite",
                                      properties=jdbc_properties)


#Clean the distributed baseline parquets to have unique PMIDs
def clean_baseline(parquets):
    parquets = parquets.dropDuplicates(['doi'])
    parquets.createOrReplaceTempView("temp_table")

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

    parquets = spark.sql(sql_query)
    parquets = parquets.drop("RowRank")
    parquets = parquets.filter(col("doi") != '')

    return parquets



#Obtain year and Mesh of our subset of nodes into a df
#Turns out that parquet files need to get cleaned, since they contain multiple PMIDs
def obtain_year_mesh(parquet_path, node_id_to_pmid_table_name, unique_nodes_table_name,  spark):
    parquets = spark.read.parquet(parquet_path)
    parquets = parquets.repartition(10)
    parquets.persist()

    parquets = clean_baseline(parquets)


    node_id_to_pmid = read_df(spark, jdbc_url, node_id_to_pmid_table_name, jdbc_properties)
    node_id_to_pmid = node_id_to_pmid.repartition(10)
    node_id_to_pmid.persist()


    unique_nodes = read_df(spark, jdbc_url, unique_nodes_table_name, jdbc_properties)
    unique_nodes = unique_nodes.repartition(10)
    unique_nodes.persist()



    node_id_to_pmid.createOrReplaceTempView("n2p")
    parquets.createOrReplaceTempView("metadata")
    unique_nodes.createOrReplaceTempView("unique_nodes")


    Nid_year_mesh_df = spark.sql("""
        SELECT u.merged_column as node_id, m.year, m.mesh
        FROM unique_nodes u
        INNER JOIN n2p n
        ON u.merged_column = n.integer_id
        INNER JOIN metadata m
        ON n.pmid = m.pmid
    """)

    Nid_year_mesh_df.show()


    # write_df(Nid_year_mesh_df, jdbc_url, jdbc_properties)
    write_df(Nid_year_mesh_df, jdbc_url, 'hm31.year_mesh_cleaned', jdbc_properties)










#This file obtains a table
if __name__ == "__main__":
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

    parquet_path = '/shared/hossein_hm31/pubmed_parquet'

    obtain_year_mesh(parquet_path, 'hm31.node_id_to_pmid_full', 'hm31.unique_node_ids', spark)









