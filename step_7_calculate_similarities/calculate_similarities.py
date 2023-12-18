from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import statistics
import pandas
import ast
import json




#Read a df from db
def read_df(spark, jdbc_url, table_name, jdbc_properties ):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
    return df

#Write a df into the db
def write_df(result, jdbc_url, table_name, jdbc_properties):
    result.repartition(50).write.jdbc(url=jdbc_url, table=table_name, mode="overwrite",
                                      properties=jdbc_properties)


def calculate_year_feature(row):

    # Assuming 'first' and 'second' are node IDs
    first_year = int(row['first_year'])
    second_year = int(row['second_year'])

    year_diff = abs(first_year - second_year)

    if year_diff <= 5:
        return 1.0

    if year_diff <= 10:
        return 0.5

    return 0.25








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
    edge_years = read_df(spark, jdbc_url, 'hm31.year_edges', jdbc_properties)
    edge_years = edge_years.repartition(100)
    edge_years.persist()

    # calculate_year_difference_udf = F.udf(lambda row: calculate_year_difference(row), IntegerType())
    calculate_year_difference_udf = F.udf(lambda row: calculate_year_feature(row), FloatType())
    #edge_years = edge_years.withColumn('year_feature', calculate_year_difference_udf(F.struct(edge_years['first_year'], edge_years['second_year'])))

    # edge_years = edge_years.withColumn(
    #     'year_feature',
    #     F.when(F.abs(edge_years['first_year'] - edge_years['second_year']) <= 5, 1.0)
    #     .when((5 < F.abs(edge_years['first_year'] - edge_years['second_year'])) & (F.abs(edge_years['first_year'] - edge_years['second_year']) <= 10), 0.5)
    #     .otherwise(0.25)
    # )


    edge_years.persist()

    write_df(edge_years, jdbc_url, 'hm31.year_feature', jdbc_properties)
    edge_years.show()

#Calculate mesh_pair_similarity by counting common terms between two mesh strings
#Here we assume meshes are of the form D23.300.820', 'D23.550.291.125
#We consider similarity for the letter as well (D vs D)
def calculate_mesh_pair_similarity(mesh1, mesh2):
    global max_mesh_overlap

    mesh1 = mesh1[0] + '.' + mesh1[1:]
    mesh2 = mesh2[0] + '.' + mesh2[1:]

    mesh_1_decomp = mesh1.split('.')
    mesh_2_decomp = mesh2.split('.')


    common_terms = 0
    limit = min(len(mesh_1_decomp), len(mesh_2_decomp))
    for i in range(limit):
        if mesh_1_decomp[i] == mesh_2_decomp[i]:
            common_terms += 1

        else:
            break

    return common_terms



with open('mesh.json', 'r') as json_file:
    mesh_dict = json.load(json_file)


def calculate_mesh_similarity_mean(row, mesh_lookup = mesh_dict):

    similarities = []
    mesh_str_1 = row['first_mesh']
    mesh_str_2 = row['second_mesh']

    mesh_terms_1 = ast.literal_eval(mesh_str_1)
    mesh_terms_2 = ast.literal_eval(mesh_str_2)

    for first in mesh_terms_1:
        if not first in mesh_lookup:
            continue

        first_mesh_tree = mesh_lookup[first][0]
        for second in mesh_terms_2:
            if not second in mesh_lookup:
                continue

            second_mesh_tree = mesh_lookup[second][0]
            similarities.append(calculate_mesh_pair_similarity(first_mesh_tree, second_mesh_tree))
            # counter += 1



    median = statistics.mean(similarities)
    return float(median)


def calculate_mesh_similarity_median(row, mesh_lookup = mesh_dict):

    similarities = []
    mesh_str_1 = row['first_mesh']
    mesh_str_2 = row['second_mesh']

    mesh_terms_1 = ast.literal_eval(mesh_str_1)
    mesh_terms_2 = ast.literal_eval(mesh_str_2)

    counter = 0
    # return len(mesh_terms_1) + len(mesh_terms_2)
    #
    for first in mesh_terms_1:
        if not first in mesh_lookup:
            continue

        first_mesh_tree = mesh_lookup[first][0]
        for second in mesh_terms_2:
            if not second in mesh_lookup:
                continue

            second_mesh_tree = mesh_lookup[second][0]
            similarities.append(calculate_mesh_pair_similarity(first_mesh_tree, second_mesh_tree))
            # counter += 1

    # if len(similarities) == 0:
    #     return -1


    median = statistics.median(similarities)
    return float(median)








def handle_mesh(edges, year_mesh, spark):
    # Register DataFrames as temporary tables to be used in SQL queries
    global mesh_dict
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

    # edge_meshes = edge_meshes.limit(10)

    edge_meshes = edge_meshes.repartition(150)
    edge_meshes.persist()

    # #Read mesh_dictionary
    # mesh_dict = read_df(spark, jdbc_url, 'hm31.mesh', jdbc_properties)
    # mesh_dict = mesh_dict.toPandas().set_index('mesh_term').T.to_dict('list')
    #
    # with open('mesh.json', 'w') as json_file:
    #     json.dump(mesh_dict, json_file)


    #print(type(mesh_dict), len(mesh_dict))


    calculate_mesh_similarity_udf = F.udf(lambda row: calculate_mesh_similarity_mean(row), FloatType())
    edge_meshes = edge_meshes.withColumn('mesh_similarity_mean', calculate_mesh_similarity_udf(F.struct(edge_meshes['first_mesh'], edge_meshes['second_mesh'])))


    calculate_mesh_similarity_udf = F.udf(lambda row: calculate_mesh_similarity_median(row), FloatType())
    edge_meshes = edge_meshes.withColumn('mesh_similarity_median', calculate_mesh_similarity_udf(F.struct(edge_meshes['first_mesh'], edge_meshes['second_mesh'])))

    edge_meshes.persist()

    write_df(edge_meshes, jdbc_url, 'hm31.mesh_feature', jdbc_properties)
    edge_meshes.show()







#Read edges, and for each features, calculate the similarities for that feature
def calculate_raw_similarities(spark):

    edges = read_df(spark, jdbc_url, 'hm31.cen_intersection_edges', jdbc_properties)

    # for feature in features:
    #     df = df.withColumn(feature, None)

    edges = edges.repartition(100)
    edges.persist()

    node_id_year_mesh = read_df(spark, jdbc_url, 'hm31.year_mesh', jdbc_properties)
    node_id_year_mesh = node_id_year_mesh.repartition(100)
    node_id_year_mesh.persist()


    #handle_year(edges, node_id_year_mesh, spark)
    handle_mesh(edges, node_id_year_mesh, spark)




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

    mesh_dict = {'123', '123'}

    spark.sparkContext.setLogLevel("WARN")

    parquet_path = '/shared/hossein_hm31/pubmed_parquet'
    calculate_raw_similarities(spark)
    # obtain_year_mesh(parquet_path, 'hm31.node_id_to_pmid_full', 'hm31.unique_node_ids', spark)









