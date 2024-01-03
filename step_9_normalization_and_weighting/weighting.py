#Switch from sql to local as a parquet file
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import statistics
import pandas
import ast
import json

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
        "driver": "org.postgresql.Driver",
        'jdbc_url' : "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
    }


    spark.sparkContext.setLogLevel("WARN")


    normalized = spark.read.parquet('normalized/')


    coefficients = [1, 2, 3, 4, 5, 6, 7]
    scale = 1

    linear_coefficients = []
    sum_all = sum(coefficients)

    for coefficient in coefficients:
        linear_coefficients.append(coefficient/sum_all)

    columns_to_aggregate = ['year_similarity', 'mesh_median', 'cocitation_jaccard', 'cocitation_frequency', 'bib_jaccard', 'bib_frequency', 'cosine_similarity']
    normalized = normalized.withColumn("weight", sum(col(col_name) * coeff * scale for col_name, coeff in zip(columns_to_aggregate, linear_coefficients)))

    normalized = normalized.drop(*columns_to_aggregate)
    normalized.show()

    coefficients.append(scale)
    coefficients = list(map(str, coefficients))


    # file_str = f'./parquets/edge_features_parquet_{"-".join(coefficients)}/'
    # normalized.coalesce(1).write.parquet(file_str)

