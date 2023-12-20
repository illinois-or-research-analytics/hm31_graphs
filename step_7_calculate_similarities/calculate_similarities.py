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
def write_df2(result, jdbc_url, table_name, jdbc_properties):
    result.repartition(100).write.jdbc(url=jdbc_url, table=table_name, mode="overwrite",
                                      properties=jdbc_properties)


def write_df(result, jdbc_url, table_name):
    result.repartition(3).write.format('jdbc').mode('overwrite').option("truncate", False).option("url", jdbc_url).option('driver', "org.postgresql.Driver").option("user", 'hm31') .option("password", 'graphs').option("dbtable", table_name) .option("isolationLevel", "NONE").option("batchsize", 100000) .save();
# 
# def write_df(result, jdbc_url, table_name, jdbc_properties):
#     result.repartition(100).write.format('jdbc').options(url=jdbc_url,table = table_name, mode="overwrite", properties=jdbc_properties )
#


    




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








def handle_year(year_mesh_edge_duplicate_features):


    calculate_year_difference_udf = F.udf(lambda row: calculate_year_feature(row), FloatType())
    #edge_years = edge_years.withColumn('year_feature', calculate_year_difference_udf(F.struct(edge_years['first_year'], edge_years['second_year'])))

    year_mesh_edge_duplicate_features = year_mesh_edge_duplicate_features.withColumn(
        'year_similarity',
        F.when(F.abs(year_mesh_edge_duplicate_features['first_year'] - year_mesh_edge_duplicate_features['second_year']) <= 5, 1.0)
        .when((5 < F.abs(year_mesh_edge_duplicate_features['first_year'] - year_mesh_edge_duplicate_features['second_year'])) & (F.abs(year_mesh_edge_duplicate_features['first_year'] - year_mesh_edge_duplicate_features['second_year']) <= 10), 0.5)
        .otherwise(0.25)
    )

    columns_to_drop = ['first_year', 'second_year']
    year_mesh_edge_duplicate_features = year_mesh_edge_duplicate_features.drop(*columns_to_drop)

    # year_mesh_edge_duplicate_features.show()
    year_mesh_edge_duplicate_features = year_mesh_edge_duplicate_features.repartition(100)
    year_mesh_edge_duplicate_features.persist()
    print('year completed')

    return year_mesh_edge_duplicate_features

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


    if len(similarities) == 0:
        return 0.0

    avg = statistics.mean(similarities)
    return float(avg)


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

    if len(similarities) == 0:
            return 0.0

    median = statistics.median(similarities)
    return float(median)








def handle_mesh(edges_annotated_with_year_features):

    calculate_mesh_similarity_udf = F.udf(lambda row: calculate_mesh_similarity_mean(row), FloatType())
    edges_annotated_with_year_features = edges_annotated_with_year_features.withColumn('mesh_similarity_mean', calculate_mesh_similarity_udf(F.struct(edges_annotated_with_year_features['first_mesh'], edges_annotated_with_year_features['second_mesh'])))


    calculate_mesh_similarity_udf = F.udf(lambda row: calculate_mesh_similarity_median(row), FloatType())
    edges_annotated_with_year_features = edges_annotated_with_year_features.withColumn('mesh_similarity_median', calculate_mesh_similarity_udf(F.struct(edges_annotated_with_year_features['first_mesh'], edges_annotated_with_year_features['second_mesh'])))

    columns_to_drop = ['first_mesh', 'second_mesh']
    edges_annotated_with_year_features = edges_annotated_with_year_features.drop(*columns_to_drop)

    # edges_annotated_with_year_features.show()

    print('mesh completed')

    return edges_annotated_with_year_features







#Read edges, and for each features, calculate the similarities for that feature
def calculate_raw_node_similarities(spark):
    import time
    year_mesh_node_duplicated_edges = read_df(spark, jdbc_url, 'hm31.year_mesh_node_duplicated_edges_cert', jdbc_properties)
    # year_mesh_node_duplicated_edges = read_df(spark, jdbc_url, 'hm31.test_features', jdbc_properties)
    year_mesh_node_duplicated_edges = year_mesh_node_duplicated_edges.repartition(100)
    year_mesh_node_duplicated_edges.persist()


    edges_annotated_with_year_features = handle_year(year_mesh_node_duplicated_edges)
    edges_annotated_with_year_and_mesh_features = handle_mesh(edges_annotated_with_year_features)

    print('pre persist')
    edges_annotated_with_year_and_mesh_features.persist()

    print('post persist')
    start = time.time()
    write_df(edges_annotated_with_year_and_mesh_features, jdbc_url, 'hm31.year_mesh_edge_weights_cert')
    end = time.time()

    print(f'elapsed insertion {end-start}')
    # def write_df(result, jdbc_url, table_name):


def handle_bib_coupling():
    references_duplicated_edges = read_df(spark, jdbc_url, 'hm31.out_edges_features_cert', jdbc_properties)


#Read edges, and for each features, calculate the similarities for that feature
def calculate_raw_edge_similarities(spark):
    handle_bib_coupling()






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

    calculate_raw_node_similarities(spark)









