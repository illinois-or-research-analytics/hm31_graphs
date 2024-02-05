#Lets switch to pandas world from Pyspark
import argparse
import os
import time
from tqdm import tqdm
import copy
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

# NW "stats": {"cpm1": 6058436.700006721, "cpm10": 5823119.450006348}
# 0.0 {"stats": {"cpm1": 4768031.250018665, "cpm10": 3303690.100015588},
# 0.05 {"stats": {"cpm1": 4670639.600033565, "cpm10": 3174907.0500167413}

def calculate_CPM_wrapper(nx_graph, communities, resolution, min_prune):
    lst = []
    manager_dict = Manager().dict()

    #Prune communities that do not reach minimum
    for community in communities:
        if len(community) > min_prune:
            lst.append((nx_graph, community, resolution, manager_dict))



    pool = multiprocessing.Pool(processes=1)
    pool.starmap(calculate_CPM_single_community, lst)


    cpm_value = 0

    for key, value in manager_dict.items():
        cpm_value += value

    return cpm_value

#241730



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



def apply_row_transformation(df, weights, scale, cores):
    #pandarallel.initialize(progress_bar=False) # initialize(36) or initialize(os.cpu_count()-1)
    pandarallel.initialize(nb_workers=cores, progress_bar=False) # initialize(36) or initialize(os.cpu_count()-1)
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




def leiden(iGraph, leiden_partition, pandas_df, resolution_parameter, seed=4311):

    t1 = time.time()
    if leiden_partition=="Modularity":
        if pandas_df is None:
            part = la.find_partition(iGraph, la.ModularityVertexPartition, seed= seed, weights = None)

        else:
            part = la.find_partition(iGraph, la.ModularityVertexPartition, seed= seed, weights=pandas_df['weight'])


    elif leiden_partition=="CPM":
        kwargs = {'resolution_parameter': resolution_parameter}

        if pandas_df is None:
            part = la.find_partition(iGraph, la.CPMVertexPartition, seed= seed, weights=None, **kwargs)

        else:
            part = la.find_partition(iGraph, la.CPMVertexPartition, seed= seed, weights=pandas_df['weight'], **kwargs)


    # part = la.find_partition(graph, la.CPMVertexPartition, seed= seed, weights = None, **kwargs)




    # Create a CPMVertexPartition instance with additional parameters


    t2 = time.time()
    print(f'clustering {t2-t1}')
    return part

#Summarize clustering statustics
def summarize(nx_Graph, address):
    files = os.listdir(address)

    for file in files:
        if (not 'CPM_UW_0.0001' in file) and (not 'Mod' in file):
            continue
            #FIX

        if '.json' in file:
            record_clustering_statistics(None, nx_Graph, True, True, f'{address}{file}')



def calculate_weighted_modularity(nx_graph_unweighted, weights):
    nx_graph_weighted = copy.deepcopy(nx_graph_unweighted)

    if len(nx_graph_weighted.edges) == len(weights):
        print('inside if')
        # Iterate through edges and assign 'weight' attribute from the DataFrame
        for edge, weight in tqdm(zip(nx_graph_weighted.edges, weights['weight'])):
            nx_graph_weighted[edge[0]][edge[1]]['weight'] = weight

        print('added weights')

    # Print the graph to verify



    file_name = 'files/clusterings/Modularity.json'
    with open(file_name, 'r') as file:
        weighted = json.load(file)

    clusters = []
    print('b')

    for key, value in weighted.items():
        if 'm' in key.lower():
            continue
        clusters.append(value)

    print('c')

    w = nx.community.modularity(nx_graph_weighted, clusters, weight = 'weight' )
    print('d')

    uw = nx.community.modularity(nx_graph_unweighted, clusters, weight = None)

    print(f'weighted: w {w} uw {uw}')



    file_name = 'files/clusterings/Modularity_UW.json'
    with open(file_name, 'r') as file:
        unweighted = json.load(file)


    clusters = []

    for key, value in unweighted.items():
        if 'm' in key.lower():
            continue
        clusters.append(value)


    w = nx.community.modularity(nx_graph_weighted, clusters, weight = 'weight')
    uw = nx.community.modularity(nx_graph_unweighted, clusters, weight = None)

    print(f'unweighted: w {w} uw {uw}')

def calculate_node_coverage(clustering_dict, min_acceptable_cluster_size):
    total_nodes = 0
    elligible_covered_nodes = 0

    clustering_array = clustering_dict.values() #Shall be list of clusters

    for cluster in clustering_array:
        if type(cluster) != list:
            continue

        total_nodes += len(cluster)
        if len(cluster) >=  min_acceptable_cluster_size:
            elligible_covered_nodes += len(cluster)

    return (elligible_covered_nodes/total_nodes) * 100

def CPM_weighting_plotter():
    base_dir = 'files/results/sweep_bi_feature/'
    files = os.listdir(base_dir)


    w_cpm_10_list = []
    w_cpm_1_list = []

    coverage1_list = []
    coverage10_list = []


    res_list = []

    for file_str in files:
        if not 'json' in file_str:
            continue

        with open(base_dir + file_str, 'r') as file:
            data_dict = json.load(file)



        if 'unweighted' in file_str:
            uw_cpm_10 = data_dict['stats']['cpm10']
            uw_cpm_1 = data_dict['stats']['cpm1']

        else:
            cpm_10 = data_dict['stats']['cpm10']
            cpm_1 = data_dict['stats']['cpm1']

            coverage1 = calculate_node_coverage(data_dict['clusters'], 2)
            coverage10 = calculate_node_coverage(data_dict['clusters'], 11)

            coverage1_list.append(coverage1)
            coverage10_list.append(coverage10)

            w_cpm_1_list.append(cpm_1)
            w_cpm_10_list.append(cpm_10)
            res_val = np.round(float(file_str.split('_')[-1][:-5]),2)
            res_list.append(res_val)

    cpm_10_ratio = [f/uw_cpm_10 for f in w_cpm_10_list]
    cpm_1_ratio = [f/uw_cpm_1 for f in w_cpm_1_list]

    plt.rcParams.update({'font.size': 18})  # Adjust the font slsize as needed

    base_dir = 'figures/sweep_bi_feature/'

    plt.figure(figsize=(20, 10))  # Adjust the width and height as needed

    plt.plot(res_list, cpm_10_ratio, 'o-', color='orange', linewidth=0.5, markersize=8, label='CPM10 Ratio')
    plt.plot(res_list, cpm_1_ratio, 'o-', color='blue', linewidth=0.5, markersize=8, label='CPM1 Ratio')

    # Set labelskis and title
    plt.xlabel('Topological importance')
    plt.title('Ratio of CPM10 weighted to CPM10 unweighted for topological importance ratio uniform divide. Res = 0.05')
    plt.ylabel('Topological Importance Ratio')

    # Set x ticks
    plt.xticks(res_list)

    # Show legend
    plt.legend()

    # Save the plot as an image file (e.g., PNG)
    plt.savefig(f'{base_dir}combined_topological_ratios.png')

    # Show the plot
    plt.show()


    plt.figure(figsize=(20, 10))  # Adjust the width and height as needed

    plt.plot(res_list, coverage10_list, 'o-', color='orange', linewidth=0.5, markersize=8, label='coverage10 %')
    plt.plot(res_list, coverage1_list, 'o-', color='blue', linewidth=0.5, markersize=8, label='coverage1 %')

    # Set labels and title
    plt.xlabel('Topological importance')
    plt.title('Node coverage for min_cluster > 1 and min_cluster > 10. Res = 0.05')
    plt.ylabel('Topological Importance Ratio')

    # Set x ticks
    plt.xticks(res_list)

    # Show legend
    plt.legend()

    # Save the plot as an image file (e.g., PNG)
    plt.savefig(f'{base_dir}node_coverages.png')

    # Show the plot
    plt.show()








def CPM_gt10_plotter(files_dir = 'files/results/sweeping_res_constant_weight/clusterings/'):
    files = os.listdir(files_dir)
    selected_files = [f for f in files if 'CPM' in f and 'json' in f and 'UW' in f]
    selected_files.sort()

    #cluster types{ key:[nodes]}

    gt10 = []
    x_label = []

    coverage10_list = []
    coverage1_list = []

    for file in selected_files:
        file_url = files_dir + file

        gt10_counter = 0

        with open(file_url, 'r') as json_file:
            clustering_dict = json.load(json_file)


        for key, value in clustering_dict.items():
            if type(value) == list and len(value) > 10:
                gt10_counter += 1

        gt10.append(np.log10(gt10_counter))
        res = float(file.split('_')[-1][:-5])
        # x_label.append(res)
        x_label.append(np.round(np.log10(res),2))

        cov1 = calculate_node_coverage(clustering_dict, 2)
        cov10 = calculate_node_coverage(clustering_dict, 11)

        coverage1_list.append(cov1)
        coverage10_list.append(cov10)


    plt.rcParams.update({'font.size': 16})  # Adjust the font size as needed


    print(gt10)
    print(x_label)

    base_dir = 'figures/res_sweep_constant_weights/'

    plt.figure(figsize=(40, 10))  # Adjust the width and height as needed

    plt.plot(x_label, gt10, 'o-', linewidth=0.5, markersize=8)

    # Set labels and title
    plt.xlabel('Log10 resolution value')
    plt.ylabel('Log10 number of clusters of size > 10')
    plt.title('Log10 Clusters greater than 10 with respect to CPM resolution value')
    plt.xticks(x_label)
    # Save the plot as an image file (e.g., PNG)
    plt.savefig(f'{base_dir}cpm_gt_10.png')

    # Show the plot
    plt.show()




    plt.figure(figsize=(40, 10))  # Adjust the width and height as needed

    plt.plot(x_label, coverage10_list, 'o-', color='orange', linewidth=0.5, markersize=8, label='Coverage10 %')
    plt.plot(x_label, coverage1_list, 'o-', color='blue', linewidth=0.5, markersize=8, label='Coverage1 %')

    plt.xlabel('Log10 resolution value')
    plt.ylabel('Coverage %')
    plt.title('Coverage % for different resolution values and constant weights')
    plt.xticks(x_label)
    # Show legend
    plt.legend()

    # Save the plot as an image file (e.g., PNG)
    plt.savefig(f'{base_dir}res_sweep_node_coverage.png')

    # Show the plot
    plt.show()


def sweep_bi_feature(nx_Graph, iGraph, raw_df, best_found_res = 0.05):
    #Guideline for weighting
    # weight = scale*(row['year_similarity']* weights[0] + row['mesh_median']* weights[1]+
    #                 row['cocitation_jaccard']* weights[2] + row['cocitation_frequency']* weights[3] + row['bib_jaccard']* weights[4]
    #                 +  row['bib_frequency']* weights[5] + row['cosine_similarity']* weights[6] )

    #What indices of weights are topological and what are metadata?
    topological_indices = [2, 3, 4, 5]
    semantic_indices = [0, 1, 6]

    topological_importance_ratios = np.arange(0.0, 1.0, 0.05)

    for topo_importance_ratio in topological_importance_ratios:
        print(f'topo importance ratio {topo_importance_ratio}')

        feature_wise_topological_importance = topo_importance_ratio/(len(topological_indices))
        feature_wise_semantic_importance = (1-topo_importance_ratio)/(len(semantic_indices))

        current_weighting = []

        for i in range(len(topological_indices) + len(semantic_indices)):
            if i in topological_indices:
                current_weighting.append(feature_wise_topological_importance)
            else:
                current_weighting.append(feature_wise_semantic_importance)

        t0 = time.time()
        print(current_weighting)

        result_df = apply_row_transformation(raw_df, current_weighting, 1, 48)

        t1 = time.time()
        print(f'weights calculated {t1-t0}')

        part = leiden(iGraph, 'CPM', result_df, best_found_res)

        t2 = time.time()

        print(f'clustered {t2-t1}')

        clsutering_dict = {"stats": {}, "clusters": {}}

        for idx, cluster in enumerate(part):
            clsutering_dict["clusters"][idx] = cluster

        t0 = time.time()
        cpm_val_1_prune = calculate_CPM_wrapper(nx_Graph, list(clsutering_dict['clusters'].values()), best_found_res, 1)
        t1 = time.time()
        print(f'cpm1 {t1-t0}')

        cpm_val_10_prune = calculate_CPM_wrapper(nx_Graph, list(clsutering_dict['clusters'].values()), best_found_res, 10)
        t2 = time.time()

        print(f'cpm10 {t2-t1}')

        clsutering_dict['stats']['cpm1'] = cpm_val_1_prune
        clsutering_dict['stats']['cpm10'] = cpm_val_10_prune

        with open(f'files/sweep_bi_feature/CPM_topo_ratio_{topo_importance_ratio}.json', 'w') as json_file:
            json.dump(clsutering_dict, json_file)

        print("\n")


    part = leiden(iGraph, 'CPM', None, best_found_res)

    clsutering_dict = {}

    for idx, cluster in enumerate(part):
        clsutering_dict["clusters"][idx] = cluster

    cpm_val_1_prune = calculate_CPM_wrapper(nx_Graph, clsutering_dict.values(), best_found_res, 1)
    cpm_val_10_prune = calculate_CPM_wrapper(nx_Graph, clsutering_dict.values(), best_found_res, 10)

    clsutering_dict['stats']['cpm1'] = cpm_val_1_prune
    clsutering_dict['stats']['cpm10'] = cpm_val_10_prune

    with open(f'files/sweep_bi_feature/CPM_UW.json', 'w') as json_file:
        json.dump(clsutering_dict, json_file)


def plot_sweep_topological_features_only():
    result_dir = 'files/results/topo_only_individual_sweep/'

    class_of_features = {}

    all_results = os.listdir(result_dir)


    for file_str in all_results:
        if not '.json' in file_str:
            continue

        feature_array = file_str.split('_')
        feature_name = feature_array[0] + '_' + feature_array[1]

        if not feature_name in class_of_features:
            class_of_features[feature_name] = {}

        feature_value = float(feature_array[2][:-5])

        class_of_features[feature_name][feature_value] = result_dir + file_str


    for feature in class_of_features:
        class_of_features[feature] = dict(sorted(class_of_features[feature].items()))

    print(class_of_features)

    for feature, results_dict in class_of_features.items():
        cpm_1_array = []
        cpm_10_array = []

        coverage_1_array = []
        coverage_10_array = []

        feature_values = []

        for point_value, json_file in results_dict.items():
            with open(json_file, 'r') as file:
                clustering_dict_loaded = json.load(file)

            feature_values.append(point_value)

            cpm1 = clustering_dict_loaded['stats']['cpm1']
            cpm10 = clustering_dict_loaded['stats']['cpm10']

            cov1 = calculate_node_coverage(clustering_dict_loaded['clusters'], 2)
            cov10 = calculate_node_coverage(clustering_dict_loaded['clusters'], 11)

        figure_save_dir_base = 'figures/topo_only_individual_sweep/'

        plt.figure(figsize=(20, 10))  # Adjust the width and height as needed

        plt.plot(feature_values, coverage_10_array, 'o-', color='orange', linewidth=0.5, markersize=8, label='Coverage10 %')
        plt.plot(feature_values, coverage_1_array, 'o-', color='blue', linewidth=0.5, markersize=8, label='Coverage1 %')

        plt.xlabel(f'{feature} values')
        plt.ylabel('Coverage %')
        plt.title(f'Coverage % for sweeping {feature} and zero semantic')
        plt.xticks(feature_values)
        # Show legend
        plt.legend()

        # Save the plot as an image file (e.g., PNG)
        plt.savefig(f'{figure_save_dir_base}_{feature}_coverage.png')

        # Show the plot
        plt.show()



        plt.figure(figsize=(30, 15))  # Adjust the width and height as needed

        plt.plot(feature_values, cpm_10_array, 'o-', color='orange', linewidth=0.5, markersize=8, label='cpm10 %')
        plt.plot(feature_values, cpm_1_array, 'o-', color='blue', linewidth=0.5, markersize=8, label='cpm1 %')

        plt.xlabel(f'{feature} values')
        plt.ylabel('cpm')
        plt.title(f'cpm for sweeping {feature} and zero semantic')
        plt.xticks(feature_values)
        plt.legend()

        plt.savefig(f'{figure_save_dir_base}_{feature}_cpm.png')

        plt.show()







def sweep_topological_features_only(iGraph, nx_Graph, raw_df, best_found_res, points = 8):
    segment_width = 1.0 / points

    segments = np.linspace(0, 1, points + 1)


    # weight = scale*(row['year_similarity']* weights[0] + row['mesh_median']* weights[1]+
    #                 row['cocitation_jaccard']* weights[2] + row['cocitation_frequency']* weights[3] + row['bib_jaccard']* weights[4]
    #                 +  row['bib_frequency']* weights[5] + row['cosine_similarity']* weights[6] )

    #What indices of weights are topological and what are metadata?

    topo_features = {2: 'cocitation_jaccard', 3: 'cocitation_frequency', 4: 'bib_jaccard', 5: 'bib_frequency'}


    topological_indices = [2, 3, 4, 5]
    semantic_indices = [0, 1, 6]

    for specific_topo_feature_index in topological_indices:

        for current_topo_feature_value in segments:
            current_weights = []

            for all_feature_index in range(len(topological_indices) + len(semantic_indices)):
                if all_feature_index in semantic_indices:
                    current_weights.append(0)

                else:
                    if all_feature_index == specific_topo_feature_index:
                        current_weights.append(current_topo_feature_value)

                    else:
                        current_weights.append( (1-current_topo_feature_value)/3 )

            print(f"{len(current_weights)} {topo_features[specific_topo_feature_index]}{specific_topo_feature_index} {current_weights}")
            save_dir = f'files/results/topo_only_individual_sweep/{topo_features[specific_topo_feature_index]}_{current_topo_feature_value}.json'
            addenum = f'{topo_features[specific_topo_feature_index]}_{current_topo_feature_value}.json'

            current_files = os.listdir('files/results/topo_only_individual_sweep/')
            if addenum in current_files:
                print('skipped')
                continue
            standardize_clustering(iGraph, nx_Graph, raw_df, current_weights, best_found_res, save_dir)




def standardize_clustering(iGraph, nx_Graph, raw_df, current_weighting, best_found_res, save_dir):
    t0 = time.time()
    result_df = apply_row_transformation(raw_df, current_weighting, 1, 32)
    t1 = time.time()
    print(f'weights calculated {t1-t0}')

    part = leiden(iGraph, 'CPM', result_df, best_found_res)

    t2 = time.time()

    print(f'clustered {t2-t1}')

    clsutering_dict = {"stats": {}, "clusters": {}}

    for idx, cluster in enumerate(part):
        clsutering_dict["clusters"][idx] = cluster

    t0 = time.time()
    cpm_val_1_prune = calculate_CPM_wrapper(nx_Graph, list(clsutering_dict['clusters'].values()), best_found_res, 1)
    t1 = time.time()
    print(f'cpm1 {t1-t0}')

    cpm_val_10_prune = calculate_CPM_wrapper(nx_Graph, list(clsutering_dict['clusters'].values()), best_found_res, 10)
    t2 = time.time()

    print(f'cpm10 {t2-t1}')

    clsutering_dict['stats']['cpm1'] = cpm_val_1_prune
    clsutering_dict['stats']['cpm10'] = cpm_val_10_prune

    with open(save_dir, 'w') as json_file:
        json.dump(clsutering_dict, json_file)

    print()







if __name__ == '__main__': # 248213

    # clustering_dir = 'files/clusterings/'
    # CPM_gt10_plotter(clustering_dir)

    # test()
    # spark = SparkSession.builder \
    #     .appName("parquet_clustering") \
    #     .config("spark.executor.memory", "10g") \
    #     .config("spark.driver.maxResultSize", "4g").getOrCreate()
    #
    # spark.sparkContext.setLogLevel("WARN")
    plot_sweep_topological_features_only()
    exit()

    # CPM_weighting_plotter()
    # CPM_gt10_plotter()
    name = 'files/raw_features.h5'
    #Lets switch to pandas from now on
    # convert_pyspark_features_to_pandas(spark, name)

    #weights, scale = handle_arguments()
    nx_path = f"files/graphs/base_nx.gpickle"
    ig_path = f"files/graphs/base_ig.pickle"

    df = pd.read_hdf(name, key='data')
    print('loaded df')
    G_nx, H_ig = load_graphs( nx_path, ig_path)
    print('loaded graphs')

    sweep_topological_features_only(iGraph=H_ig, nx_Graph=G_nx, raw_df=df, best_found_res=0.05, points=8)
    exit(0)

    # sweep_bi_feature(G_nx, H_ig, df, best_found_res = 0.05)




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


    t1 = time.time()
    print(f'Load graphs: {t1 - t2}')

    summarize(G_nx, 'files/clusterings/')

    exit(0)

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




#https://www.linkedin.com/in/hani-khassaf/