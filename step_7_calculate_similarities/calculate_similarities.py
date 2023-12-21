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


def write_df(result, jdbc_url, table_name): #182953
    result.repartition(5).write.format('jdbc').mode('overwrite').option("truncate", False).option("url", jdbc_url).option('driver', "org.postgresql.Driver").option("user", 'hm31') .option("password", 'graphs').option("dbtable", table_name) .option("isolationLevel", "NONE").option("batchsize", 10000) .save();
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

#If two arrays a1 and a2 are sorted, what is the size of their intersection?
def find_common_count(arr1, arr2):
    size1, size2 = len(arr1), len(arr2)
    i, j, count = 0, 0, 0

    while i < size1 and j < size2:
        if arr1[i] == arr2[j]:
            # Found an element in the intersection
            count += 1
            i += 1
            j += 1
        elif arr1[i] < arr2[j]:
            # Move the pointer in the first array
            i += 1
        else:
            # Move the pointer in the second array
            j += 1

    return count

#Calculate jaccard of bib_coupling of a row
def calculate_bib_coupling_jaccard_similarity(row):
    first_out = row['first_out']
    second_out = row['second_out']

    first_out_nodes = first_out.split(',')
    second_out_nodes = second_out.split(',')

    first_out_nodes.sort()
    second_out_nodes.sort()

    intersection_count = find_common_count(first_out_nodes, second_out_nodes)

    if len(first_out_nodes) + len(second_out_nodes) == 0:
        return 0.0

    return float(intersection_count/(len(first_out_nodes) + len(second_out_nodes) - intersection_count))

#Calculate jaccard of bib_coupling of a row
def calculate_bib_coupling_raw_frequency(row):
    first_out = row['first_out']
    second_out = row['second_out']

    first_out_nodes = first_out.split(',')
    second_out_nodes = second_out.split(',')

    first_out_nodes.sort()
    second_out_nodes.sort()

    intersection_count = find_common_count(first_out_nodes, second_out_nodes)

    return intersection_count

#####################################################################################3

#Calculate jaccard of bib_coupling of a row
def calculate_cocitation_jaccard_similarity(row):
    first_in = row['first_in']
    second_in = row['second_in']

    first_in_nodes = first_in.split(',')
    second_in_nodes = second_in.split(',')

    first_in_nodes.sort()
    second_in_nodes.sort()

    intersection_count = find_common_count(first_in_nodes, second_in_nodes)

    if len(first_in_nodes) + len(second_in_nodes) == 0:
        return 0.0

    return float(intersection_count/(len(first_in_nodes) + len(second_in_nodes) - intersection_count))

#Calculate jaccard of bib_coupling of a row
def calculate_cocitation_raw_similarity(row):
    first_in = row['first_in']
    second_in = row['second_in']

    first_in_nodes = first_in.split(',')
    second_in_nodes = second_in.split(',')

    first_in_nodes.sort()
    second_in_nodes.sort()

    intersection_count = find_common_count(first_in_nodes, second_in_nodes)

    return intersection_count


def handle_cocitation(spark):
    citations_duplicated_edges = read_df(spark, jdbc_url, 'hm31.in_edges_features_cert', jdbc_properties)
    print("read from db")
    # citations_duplicated_edges = read_df(spark, jdbc_url, 'hm31.limited', jdbc_properties)
    # # references_duplicated_edges = references_duplicated_edges.limit(40)


    calculate_cocitation_jaccard_similarity_udf = F.udf(lambda row: calculate_cocitation_jaccard_similarity(row), FloatType())
    citations_duplicated_edges_plus_jaccard = citations_duplicated_edges.withColumn('cocitation_jaccard_similarity', calculate_cocitation_jaccard_similarity_udf(F.struct(citations_duplicated_edges['first_in'], citations_duplicated_edges['second_in'])))
    print('co_citation_jaccard_similarity completed')


    calculate_cocitation_raw_frequency_udf = F.udf(lambda row: calculate_cocitation_raw_similarity(row), IntegerType())
    citations_duplicated_edges_plus_jaccard_and_frequency = citations_duplicated_edges_plus_jaccard.withColumn('cocitation_frequency_similarity', calculate_cocitation_raw_frequency_udf(F.struct(citations_duplicated_edges_plus_jaccard['first_in'], citations_duplicated_edges_plus_jaccard['second_in'])))
    print('co_citation_jaccard_similarity completed')


    # columns_to_drop = ['first_in', 'second_in']
    # citations_duplicated_edges_plus_jaccard_and_frequency = citations_duplicated_edges_plus_jaccard_and_frequency.drop(*columns_to_drop)

    # edges_annotated_with_year_features.show()
    # citations_duplicated_edges_plus_jaccard_and_frequency.show()
    print('count', citations_duplicated_edges_plus_jaccard_and_frequency.count())


    print('cocitations completed')
    return citations_duplicated_edges_plus_jaccard_and_frequency




def handle_bib_coupling(spark):
    references_duplicated_edges = read_df(spark, jdbc_url, 'hm31.out_edges_features_cert', jdbc_properties)
    print("read from db")
    # references_duplicated_edges = read_df(spark, jdbc_url, 'hm31.limited', jdbc_properties)
    # references_duplicated_edges = references_duplicated_edges.limit(40)


    calculate_bib_coupling_jaccard_similarity_udf = F.udf(lambda row: calculate_bib_coupling_jaccard_similarity(row), FloatType())
    references_duplicated_edges_plus_jaccard = references_duplicated_edges.withColumn('bib_coupling_jaccard_similarity', calculate_bib_coupling_jaccard_similarity_udf(F.struct(references_duplicated_edges['first_out'], references_duplicated_edges['second_out'])))
    print('bib_coupling_jaccard_similarity completed')


    calculate_bib_coupling_raw_frequency_udf = F.udf(lambda row: calculate_bib_coupling_raw_frequency(row), IntegerType())
    references_duplicated_edges_plus_jaccard_and_frequency = references_duplicated_edges_plus_jaccard.withColumn('bib_coupling_frequency_similarity', calculate_bib_coupling_raw_frequency_udf(F.struct(references_duplicated_edges_plus_jaccard['first_out'], references_duplicated_edges_plus_jaccard['second_out'])))
    print('bib_coupling_frequency_similarity completed')


    columns_to_drop = ['first_out', 'second_out']
    references_duplicated_edges_plus_jaccard_and_frequency = references_duplicated_edges_plus_jaccard_and_frequency.drop(*columns_to_drop)

    # edges_annotated_with_year_features.show()
    # references_duplicated_edges_plus_jaccard_and_frequency.show()
    print('count', references_duplicated_edges_plus_jaccard_and_frequency.count())


    print('bib-coupling completed')
    return references_duplicated_edges_plus_jaccard_and_frequency

    #169940

#Read edges, and for each features, calculate the similarities for that feature
def calculate_raw_edge_similarities(spark):
    # references_duplicated_edges_plus_jaccard_and_frequency = handle_bib_coupling(spark)
    # references_duplicated_edges_plus_jaccard_and_frequency.persist()
    # write_df(references_duplicated_edges_plus_jaccard_and_frequency, jdbc_url, 'hm31.bib_coupling_edge_weights_cert')


    cocitations_duplicated_edges_plus_jaccard_and_frequency = handle_cocitation(spark)
    cocitations_duplicated_edges_plus_jaccard_and_frequency.persist()
    write_df(cocitations_duplicated_edges_plus_jaccard_and_frequency, jdbc_url, 'hm31.cocitations_edge_weights_cert')










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

    # calculate_raw_node_similarities(spark)
    calculate_raw_edge_similarities(spark)









