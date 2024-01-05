#Lets switch to pandas world from Pyspark
import argparse
import time
from tqdm import tqdm
from pyspark.sql import SparkSession
import pandas as pd
import tables
import igraph as ig
import  leidenalg as la
import numpy as np
import networkx as nx
from pandarallel import pandarallel #error in   File "/home/hm31/step_1/venv/lib/python3.6/site-packages/pandarallel/progress_bars.py", line 8, in <module>
#Made this change: https://github.com/qilingframework/qiling/commit/902e01beb94e2e27e50d1456e51e0ef99937aff1

def convert_pyspark_features_to_pandas(spark, name):
    df = spark.read.parquet('../step_9_normalization_and_weighting/normalized/')
    df = df.orderBy("id")

    pandas_df = df.toPandas()
    pandas_df.to_hdf(name, key='data', mode='w')
    print(pandas_df)

def handle_arguments():
    parser = argparse.ArgumentParser(description='Example script to get a string argument.')

        # Add the argument for the string
    parser.add_argument('input_string', type=str, help='Input of weights in a comma separated format.\
        like 1,2,3,4,5,6,7. From L to R: Year, MeSH median, Coc-Jaccard, Coc-Freq, Bib-Jaccard, Bib-Freq, Embedding')

    """
    weight = scale*(row['year_similarity']* weights[0] + row['mesh_median']* weights[1]+
                    row['cocitation_jaccard']* weights[2] + row['cocitation_frequency']* weights[3] + row['bib_jaccard']* weights[4]
                    +  row['bib_frequency']* weights[5] + row['cosine_similarity']* weights[6] )
    """

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

    addition = 0
    for i in input_array:
        addition += int(i)

    for i in input_array:
        weights.append(float(i)/float(addition))

    print(weights)


    assert len(weights) == 7

    print(weights)
    print(scale)

    return weights, scale

def apply_row_transformation(df, weights, scale):
    pandarallel.initialize(progress_bar=False) # initialize(36) or initialize(os.cpu_count()-1)
    new_df = df[['id']].copy()
    new_df['weight'] = df.parallel_apply(calculate_weight, axis=1, weights=weights, scale=scale)

    return new_df

def calculate_weight(row, weights, scale):
    # Assuming weights is a list or array containing the coefficients for linear combination
    weight = scale*(row['year_similarity']* weights[0] + row['mesh_median']* weights[1]+
                    row['cocitation_jaccard']* weights[2] + row['cocitation_frequency']* weights[3] + row['bib_jaccard']* weights[4]
                    +  row['bib_frequency']* weights[5] + row['cosine_similarity']* weights[6] )


    return weight

#Load igraph and nxgraph
def load_graphs( nx_path, ig_path):
    G_networkx = nx.read_gpickle(nx_path)
    H_igraph = ig.read(ig_path)
    return G_networkx, H_igraph


def leiden(graph, leiden_partition, pandas_df):

    t1 = time.time()
    if leiden_partition=="Modularity":
        partition = la.ModularityVertexPartition
    elif leiden_partition=="CPM":
        partition = la.CPMVertexPartition

    # part = la.find_partition(graph, partition, weights=weights["weight"])
    # part = la.find_partition(graph, partition)
    # print(pandas_df)
    part = la.find_partition(graph, partition, weights=pandas_df['weight'])

    t2 = time.time()
    print(f'clustering {t2-t1}')
    return part



if __name__ == '__main__':
    # spark = SparkSession.builder \
    #     .appName("parquet_clustering") \
    #     .config("spark.executor.memory", "10g") \
    #     .config("spark.driver.maxResultSize", "4g").getOrCreate()
    #
    # spark.sparkContext.setLogLevel("WARN")

    name = 'files/raw_features.h5'
    #Lets switch to pandas from now on
    # convert_pyspark_features_to_pandas(spark, name)

    #weights, scale = handle_arguments()
    nx_path = f"files/graphs/base_nx.gpickle"
    ig_path = f"files/graphs/base_ig.pickle"

    df = pd.read_hdf(name, key='data')
    print(df.columns)

    print(len(df))

    # print(df.tail())
    nw = [1,2,3,4,5,6,7]
    scale_factor = 1

    weights = []

    for i in nw:
            weights.append(i/sum(nw))



    t1 = time.time()
    result_df = apply_row_transformation(df, weights, scale_factor)
    t2 = time.time()

    # print(result_df.head())
    print(f'Calculate weights: {t2-t1}')


    G_nx, H_ig = load_graphs( nx_path, ig_path)
    t1 = time.time()
    print(f'Load graphs: {t1 - t2}')

    x = leiden(H_ig, 'Modularity', result_df)
    t2 = time.time()

    print(f'Cluster: {t2-t1}')

    mod = nx.community.modularity(G_nx, x)
    t1 = time.time()
    print(f'Modularity: {t1-t2}')

    print(mod)


    # Calculate weights: 61.890968561172485
    # Load graphs: 161.23537063598633

    # clustering 429.36126685142517
    # Cluster: 429.36149644851685
    # Modularity: 183.89536952972412












