from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType



#Read a df from db
def read_df(spark, jdbc_url, table_name, jdbc_properties ):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
    return df

#Write a df into the db
def write_df(result, jdbc_url, table_name, jdbc_properties):
    result.repartition(50).write.jdbc(url=jdbc_url, table=table_name, mode="overwrite",
                                      properties=jdbc_properties)


def calculate_year_difference(row):

    # Assuming 'first' and 'second' are node IDs
    first_year = int(row['first_year'])
    second_year = int(row['second_year'])

    return abs(first_year - second_year)




def handle_year(edges, year_mesh, spark):


    # Register DataFrames as temporary tables to be used in SQL queries
    edges.createOrReplaceTempView("edges_table")
    year_mesh.createOrReplaceTempView("year_mesh")

    #Uncomment this to actually calculate year_difference

    # edge_years = spark.sql("""
    # SELECT
    #     e.first,
    #     e.second,
    #     ym_first.year AS first_year,
    #     ym_second.year AS second_year
    # FROM
    #     edges_table e
    # inner JOIN
    #     year_mesh ym_first ON e.first = ym_first.node_id
    # inner JOIN
    #     year_mesh ym_second ON e.second = ym_second.node_id;
    #     """)
    #

    #Or just read from db
    edge_years = read_df(spark, jdbc_url, 'hm31.year_difference', jdbc_properties)
    edge_years = edge_years.repartition(100)
    edge_years.persist()

    calculate_year_difference_udf = F.udf(lambda row: calculate_year_difference(row), IntegerType())
    edge_years = edge_years.withColumn('year_difference', calculate_year_difference_udf(F.struct(edge_years['first_year'], edge_years['second_year'])))
    edge_years = edge_years.select('first', 'second', 'year_difference')
    edge_years.persist()


    write_df(edge_years, jdbc_url, 'hm31.year_difference_feature', jdbc_properties)
    edge_years.show()



def calculate_mesh_similarity(row):
    pass
    #TODO




def handle_mesh(edges, year_mesh, spark):
    # Register DataFrames as temporary tables to be used in SQL queries
    edges.createOrReplaceTempView("edges_table")
    year_mesh.createOrReplaceTempView("year_mesh")

    #Uncomment this to actually calculate mesh_difference

    # edge_meshes = spark.sql("""
    # SELECT
    #     e.first,
    #     e.second,
    #     ym_first.mesh AS first_mesh,
    #     ym_second.mesh AS second_mesh
    # FROM
    #     edges_table e
    # inner JOIN
    #     year_mesh ym_first ON e.first = ym_first.node_id
    # inner JOIN
    #     year_mesh ym_second ON e.second = ym_second.node_id;
    #     """)
    # write_df(edge_meshes, jdbc_url, 'hm31.year_edges', jdbc_properties)


    #Or just read from db
    edge_meshes = read_df(spark, jdbc_url, 'hm31.mesh_edges', jdbc_properties)
    edge_meshes = edge_meshes.repartition(100)
    edge_meshes.persist()

    calculate_mesh_similarity_udf = F.udf(lambda row: calculate_mesh_similarity(row), IntegerType())
    edge_meshes = edge_meshes.withColumn('mesh_similarity', calculate_mesh_similarity_udf(F.struct(edge_meshes['first_mesh'], edge_meshes['second_mesh'])))
    edge_meshes = edge_meshes.select('first', 'second', 'mesh_similarity')
    edge_meshes.persist()


    write_df(edge_meshes, jdbc_url, 'hm31.mesh_similarity_feature', jdbc_properties)
    edge_meshes.show()







#Read edges, and for each features, calculate the similarities for that feature
def calculate_raw_similarities(spark):

    edges = read_df(spark, jdbc_url, 'hm31.cen_intersection_edges', jdbc_properties)

    # for feature in features:
    #     df = df.withColumn(feature, None)

    edges = edges.repartition(100)
    edges.persist()

    node_id_year_mesh = read_df(spark, jdbc_url, 'hm31.year_mesh', jdbc_properties)
    node_id_year_mesh = node_id_year_mesh.repartition(10)
    node_id_year_mesh.persist()


    handle_year(edges, node_id_year_mesh, spark)


    # calculate_year_difference_udf = F.udf(lambda row: calculate_year_difference(row, node_id_year_mesh), IntegerType())
    # calculate_year_difference_udf = F.udf(calculate_year_difference, IntegerType())













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
    calculate_raw_similarities(spark)
    # obtain_year_mesh(parquet_path, 'hm31.node_id_to_pmid_full', 'hm31.unique_node_ids', spark)









