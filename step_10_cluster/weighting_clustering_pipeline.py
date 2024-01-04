#Lets switch to pandas world from Pyspark
import argparse
import time

from pyspark.sql import SparkSession
import pandas as pd
import tables
import igraph as ig
import  leidenalg as la
import numpy as np
import networkx as nx


def convert_pyspark_features_to_pandas(spark, name = './raw_features.h5'):
    df = spark.read.parquet('../step_9_normalization_and_weighting/normalized/')
    df = df.orderBy("id")

    pandas_df = df.toPandas()
    pandas_df.to_hdf(name, key='data', mode='w')
    print(pandas_df)



if __name__ == '__main__':
    # spark = SparkSession.builder \
    #     .appName("parquet_clustering") \
    #     .config("spark.executor.memory", "10g") \
    #     .config("spark.driver.maxResultSize", "4g").getOrCreate()
    #
    # spark.sparkContext.setLogLevel("WARN")

    #Lets switch to pandas from now on
    # convert_pyspark_features_to_pandas(spark)


    parser = argparse.ArgumentParser(description='Example script to get a string argument.')

    # Add the argument for the string
    parser.add_argument('input_string', type=str, help='Input of weights in a comma separated format.\
    like 1,2,3,4,5,6,7. From L to R: Year, MeSH median, Coc-Jaccard, Coc-Freq, Bib-Jaccard, Bib-Freq, Embedding')
    parser.add_argument('scale', nargs='?', type=int, default=None, help='Scale factor for processing.')

    # Parse the command line arguments
    args = parser.parse_args()

    # Access the input string from the parsed arguments
    input_string = args.input_string
    scale = args.scale

    if scale is None:
        scale = 1

    input_array= input_string.split(',')
    weights = []

    for i in input_array:
        weights.append(int(i))

    assert len(weights) == 7

    print(weights)
    print(scale)



