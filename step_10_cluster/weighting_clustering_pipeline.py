#Lets switch to pandas world from Pyspark
import argparse
import os
import time
from tqdm import tqdm
from pyspark.sql import SparkSession
import pandas as pd
import tables
import igraph as ig
import  leidenalg as la
import numpy as np
import multiprocessing
from multiprocessing import Process, Manager
import json
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
from pandarallel import pandarallel #error in   File "/home/hm31/step_1/venv/lib/python3.6/site-packages/pandarallel/progress_bars.py", line 8, in <module>
#Made this change: https://github.com/qilingframework/qiling/commit/902e01beb94e2e27e50d1456e51e0ef99937aff1


def calculate_CPM_single_community(nx_graph, community_nodes, resolution, manager_dict):
    induced_community = nx.induced_subgraph(nx_graph, community_nodes)
    e_c = induced_community.number_of_edges()
    n = induced_community.number_of_nodes()

    CPM_score = e_c - resolution * (n)*(n-1)/2

    manager_dict[community_nodes[0]] = CPM_score




def calculate_CPM_wrapper(nx_graph, communities, resolution):
    lst = []
    manager_dict = Manager().dict()

    for community in communities:
        if len(community) > 1:
            lst.append((nx_graph, community, resolution, manager_dict))



    pool = multiprocessing.Pool(processes=2)
    pool.starmap(calculate_CPM_single_community, lst)


    cpm_value = 0

    for key, value in manager_dict.items():
        cpm_value += value

    return cpm_value

#31627





def convert_pyspark_features_to_pandas(spark, name):
    df = spark.read.parquet('../step_9_normalization_and_weighting/normalized/')
    df = df.orderBy("id")

    pandas_df = df.toPandas()
    pandas_df.to_hdf(name, key='data', mode='w')

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



    assert len(weights) == 7


    return weights, scale

def record_clustering_statistics(clustering_dict, nx_Graph, discard_singletons = True, load = False, file_name = None):

    if load == False:
        singleton_degrees = []

        modularity = clustering_dict['Modularity']
        clustering_dict.pop('Modularity', None)

        n_clusters = len(clustering_dict)
        cluster_statistics = np.zeros(n_clusters)
        singleton = 0
        total = 0

        #Modularity is also stored in this dict
        for key, value in clustering_dict.items():
            if 'r' in key:
                continue


            cluster_statistics[int(key)] = len(value)
            if len(value) == 1:
                singleton += 1
                singleton_degrees.append(G_nx.degree(value[0]))

            total += len(value)

        if discard_singletons == True:
            cluster_statistics = cluster_statistics[cluster_statistics> 1]


        if cluster_statistics.shape[0] == 0:
            cluster_statistics = np.asarray([1])

        if singleton == 0:
            singleton_degrees = [0]

        singleton_degrees = np.asarray(singleton_degrees)

        t1 = time.time()

        if 'cpm' in file_name.lower():
            resolution = float(file_name.split('_')[-1][:-5]) #CPM_resolution.json format assumption


            cpm_score = calculate_CPM_wrapper(nx_Graph, clustering_dict.values(), resolution)

        else:
            cpm_score = 0

        t2 = time.time()

        print(f'elapsed CPM {t2 - t1} seconds cpm {cpm_score}')
        # def calculate_CPM_wrapper(nx_graph, communities, resolution):


        min_size = np.amin(cluster_statistics)
        max_size = np.amax(cluster_statistics)
        mean_size = np.mean(cluster_statistics)
        median_size = np.median(cluster_statistics)
        coverage = 1.0 * (total - singleton)/total * 100

        first_quant = np.quantile(cluster_statistics, .25)
        third_quant = np.quantile(cluster_statistics, .75)

        cluster_statistics_dict = {'min': min_size, 'max': max_size, 'mean': mean_size, 'median': median_size,
                        'q1': first_quant, 'q3': third_quant, 'coverage': coverage,
                        'modularity': modularity, '#clusters': n_clusters, 'singletons': singleton, 'CPM_score': cpm_score}

        min_size = np.amin(singleton_degrees)
        max_size = np.amax(singleton_degrees)
        mean_size = np.mean(singleton_degrees)
        median_size = np.median(singleton_degrees)


        first_quant = np.quantile(singleton_degrees, .25)
        third_quant = np.quantile(singleton_degrees, .75)

        singleton_statistics_dict = {'min': min_size, 'max': max_size, 'mean': mean_size, 'median': median_size,
                                   'q1': first_quant, 'q3': third_quant}

        log_clusters = np.zeros_like(cluster_statistics)
        for i in range(cluster_statistics.shape[0]):
            log_clusters[i] = np.log10(cluster_statistics[i])

        json_idx = file_name.index('json')
        base_file_name = file_name[:json_idx-1].split('/')[-1]

        results_dict= {'cluster_statistics' : cluster_statistics_dict, 'singleton_statistics': singleton_statistics_dict}

        plt.hist(log_clusters, bins=20, color='blue', alpha=0.7)
        plt.title(f'Histogram of log cluster sizes {base_file_name}')
        plt.xlabel('log cluster size')
        plt.ylabel('N')
        plt.grid(True)
        # plt.show()

        plt.savefig(f'files/results/{base_file_name}.png')


        with open(f'files/results/{base_file_name}.json', 'w') as json_file:
            json.dump(results_dict, json_file, cls= NpEncoder)






    else:
        with open(file_name, 'r') as file:
           clustering_dict_loaded = json.load(file)

        record_clustering_statistics(clustering_dict_loaded, nx_Graph, True, False, file_name)





class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)



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

def test():
    G = nx.erdos_renyi_graph(50, 0.5, seed=123, directed=False)
    w = np.random.rand(len(G.edges()))

    H = ig.Graph.from_networkx(G)

    kwargs = {'resolution_parameter': 0.5}

    # Create a CPMVertexPartition instance with additional parameters

    result_partition = la.find_partition(H, la.CPMVertexPartition, weights=w, **kwargs)
    # result_partition = la.find_partition(H, la.ModularityVertexPartition, weights=w, **kwargs)


    # part = la.find_partition(graph=H, partition_type = la.ModularityVertexPartition, weights=w)


    clustering = {}


    for idx, cluster in enumerate(result_partition):
        clustering[idx] = cluster


    exit(0)




def leiden(graph, leiden_partition, pandas_df, seed=4311, resolution_parameter = None):

    t1 = time.time()
    if leiden_partition=="Modularity":
        # part = la.find_partition(graph, la.ModularityVertexPartition, seed= seed, weights=pandas_df['weight'])
        part = la.find_partition(graph, la.ModularityVertexPartition, seed= seed, weights = None)

    elif leiden_partition=="CPM":
        kwargs = {'resolution_parameter': resolution_parameter}
        # part = la.find_partition(graph, la.CPMVertexPartition, seed= seed, weights=pandas_df['weight'], **kwargs)
        part = la.find_partition(graph, la.CPMVertexPartition, seed= seed, weights = None, **kwargs)




    # Create a CPMVertexPartition instance with additional parameters


    t2 = time.time()
    print(f'clustering {t2-t1}')
    return part

#Summarize clustering statustics
def summarize(nx_Graph, address):
    files = os.listdir(address)

    for file in files:
        if '.json' in file:
            record_clustering_statistics(None, nx_Graph, True, True, f'{address}{file}')



def calculate_weighted_modularity(nx_graph, weights):
    file_name = 'files/clusterings/Modularity.json'
    with open(file_name, 'r') as file:
        weighted = json.load(file)

    clusters = []

    for key, value in weighted.items():
        if 'm' in key.lower():
            continue
        clusters.append(value)


    w = nx.community.modularity(nx_graph, clusters, weight = weights['weight'])
    uw = nx.community.modularity(nx_graph, clusters, weight = None)

    print(f'weighted: w {w} uw {uw}')



    file_name = 'files/clusterings/Modularity_UW.json'
    with open(file_name, 'r') as file:
        unweighted = json.load(file)


    clusters = []

    for key, value in unweighted.items():
        if 'm' in key.lower():
            continue
        clusters.append(value)


    w = nx.community.modularity(nx_graph, clusters, weight = weights['weight'])
    uw = nx.community.modularity(nx_graph, clusters, weight = None)

    print(f'unweighted: w {w} uw {uw}')





if __name__ == '__main__': # 248213


    # test()
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

    # summarize(G_nx, 'files/clusterings/')

    calculate_weighted_modularity(G_nx, result_df)
    exit(0)

    #def leiden(graph, leiden_partition, pandas_df, resolution_parameter = None):
    resolution_values = [0.95, 0.75, 0.50, 0.25, 0.05, 0.01, 0.001, 0.0001]

    for resolution_value in resolution_values:
        # break
        x = leiden(H_ig, 'CPM', result_df, resolution_value)
        # t2 = time.time()


        mod = nx.community.modularity(G_nx, x, weight = None)
        # t1 = time.time()



        clsutering_dict = {}

        for idx, cluster in enumerate(x):
            clsutering_dict[idx] = cluster


        clsutering_dict['Modularity'] = mod

        with open(f'files/clusterings/CPM_UW_{resolution_value}.json', 'w') as json_file:
            json.dump(clsutering_dict, json_file)

        # t2 = time.time()

        # print(f'saving {t2-t1}')



    x = leiden(H_ig, 'Modularity', result_df)
    mod = nx.community.modularity(G_nx, x, weight = None)


    clsutering_dict = {}

    for idx, cluster in enumerate(x):
        clsutering_dict[idx] = cluster


    clsutering_dict['Modularity'] = mod

    with open(f'files/clusterings/Modularity_UW.json', 'w') as json_file:
        json.dump(clsutering_dict, json_file)


    summarize('files/clusterings/')




