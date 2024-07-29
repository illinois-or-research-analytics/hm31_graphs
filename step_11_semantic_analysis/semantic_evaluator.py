import json
import os

import pandas as pd
import psycopg2
from psycopg2 import sql
import multiprocessing
import re
import time
import numpy as np
import json
from multiprocessing import Manager, Pool, cpu_count
from itertools import starmap
import tqdm
import matplotlib.pyplot as plt
import networkx as nx
import pickle
from scipy.interpolate import make_interp_spline



# Set the PGDATABASE environment variable
os.environ["PGDATABASE"] = "ernieplus"

def fetch_embeddings_from_table(squashed_node_id_list):
    try:
        conn = psycopg2.connect("")
        cur = conn.cursor()

        node_ids = ','.join(cur.mogrify("%s", (node_id,)).decode('utf-8') for node_id in squashed_node_id_list)

        table_name = 'hm31.cenm_squashed_cleaned_embeddings'

        query = f"SELECT squashed_id, embedding FROM {table_name} WHERE squashed_id IN ({node_ids})"
        cur.execute(query)

        results = cur.fetchall()

        embeddings_lst = [embedding for node_id, embedding in results]
        embedding_arr = np.asarray(embeddings_lst)

        return embedding_arr

    except Exception as e:
        print("Error:", e)
        return "fetch error"

    finally:
        cur.close()
        conn.close()

import psycopg2
import numpy as np

def fetch_original_ids():
    try:
        conn = psycopg2.connect("")
        cur = conn.cursor()

        query = "SELECT original_id FROM hm31.cenm_node_id_mapping ORDER BY squashed_id"
        cur.execute(query)

        results = cur.fetchall()

        original_id_list = [result[0] for result in results]

        with open('original_ids.pkl', 'wb') as f:
            pickle.dump(original_id_list, f)

        return original_id_list

    except Exception as e:
        print("Error:", e)
        return "fetch error"

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()



def calculate_average_similarity(embedding_matrix, squashed_node_id_list):
        n = embedding_matrix.shape[0]
        statistics_list = []
        all_similarities = []

    # if n < 10000:
    #     A = np.matmul(embedding_matrix, embedding_matrix.transpose())
    #     lower_triangle = np.tril(A)
    #
    #     average_similarity = (2 * np.sum(lower_triangle))/(n*(n-1))
    #     return average_similarity
    #
    # else:
        sum = 0
        for i in range(n):
            first_node_id = original_ids[squashed_node_id_list[i]]
            for j in range(i):
                second_node_id = original_ids[squashed_node_id_list[j]]

                v1 = embedding_matrix[i,:]
                v2 = embedding_matrix[j,:]

                similarity = np.dot(v1, v2)/(np.linalg.norm(v1)* np.linalg.norm(v2))
                sum += similarity
                statistics_list.append({'first_node_id': first_node_id, 'second_node_id': second_node_id, 'similarity': similarity})
                all_similarities.append(similarity)

        # similarity_df = pd.DataFrame(statistics_list)
        # similarity_df.to_json()
        sum = (2* sum)/(n*(n-1))
        stats = {'cluster_size': n, 'Q1': np.quantile(all_similarities, 0.25), 'Q2': np.median(all_similarities), 'Q3': np.quantile(all_similarities, 0.75), 'mean': sum, 'min': min(all_similarities), 'max': max(all_similarities)}

        return sum, statistics_list, stats


def similarity_wrapper(squashed_node_id_list, cluster_index, manager_dict):
    squashed_node_id_list.sort()
    embeddings = fetch_embeddings_from_table(squashed_node_id_list)
    average_similarity, similarity_df, stats = calculate_average_similarity(embeddings, squashed_node_id_list)

    # manager_dict[cluster_index] = (average_similarity, embeddings.shape[0], similarity_df, stats)
    manager_dict[cluster_index] = (average_similarity, embeddings.shape[0], stats)


def calculate_single_community_degrees(nx_graph, community_nodes, manager_dict = None):
    induced_community = nx.induced_subgraph(nx_graph, community_nodes)
    degree_list = [d for n, d in induced_community.degree()]

    manager_dict[community_nodes[0]] = degree_list


def load_graphs( nx_path):
    G_networkx = nx.read_gpickle(nx_path)
    return G_networkx


def read_json_clustering(file_path):
    with open(file_path, 'r') as file:
        clustering_dict = json.load(file)

    clusters = clustering_dict['clusters']
    return clusters

def plot_with_errors(cluster_sizes, average_similarities):
    stats_dict = {}
    for i in range(len(cluster_sizes)):
        cluster_size = cluster_sizes[i]
        if not cluster_size in stats_dict:
            stats_dict[cluster_size] = []
        stats_dict[cluster_size].append(average_similarities[i])
    stats_dict = {key: stats_dict[key] for key in sorted(stats_dict)}

    # print(stats_dict)
    avg = []
    var = []
    unique_sizes = []

    for a_cluster_size in stats_dict.keys():
        # print(a_cluster_size, stats_dict[a_cluster_size])
        avg.append(np.mean(stats_dict[a_cluster_size]))
        var.append(np.var(stats_dict[a_cluster_size]))
        unique_sizes.append(a_cluster_size)

    avg = np.asarray(avg)
    # print(avg)
    var = np.asarray(var)
    unique_sizes = np.asarray(unique_sizes)

    errors = np.sqrt(var)
    lower_bounds = avg - errors
    upper_bounds = avg + errors

    # plt.scatter(unique_sizes, avg, c='blue', marker='o', alpha=0.7)
    # plt.fill_between(unique_sizes, lower_bounds, upper_bounds, color='blue', alpha=0.2)
    #
    # plt.xlabel('Cluster Sizes')
    # plt.ylabel('Average Similarity')
    # plt.title('Scatter Plot with Error Regions')
    # plt.show()
    print(len(unique_sizes), len(avg))
    x_new = np.linspace(unique_sizes.min(), unique_sizes.max(), 300)
    spl_avg = make_interp_spline(unique_sizes, avg, k=2)
    spl_lower = make_interp_spline(unique_sizes, lower_bounds, k=3)
    spl_upper = make_interp_spline(unique_sizes, upper_bounds, k=3)

    y_avg_smooth = spl_avg(x_new)
    y_lower_smooth = spl_lower(x_new)
    y_upper_smooth = spl_upper(x_new)

    y_avg_smooth = np.clip(y_avg_smooth, 0, 1)
    y_lower_smooth = np.clip(y_lower_smooth, 0, 1)
    y_upper_smooth = np.clip(y_upper_smooth, 0, 1)


# Plot the smooth curve
    plt.plot(x_new, y_avg_smooth, label='Average Similarity', color='blue')

    # Plot the shaded error region
    plt.fill_between(x_new, y_lower_smooth, y_upper_smooth, color='blue', alpha=0.2, label='Error Region')
    coefficients = np.polyfit(unique_sizes, avg, 1)
    trendline = np.polyval(coefficients, x_new)

    plt.plot(x_new, trendline, label='Trendline', color='red', linestyle='--')


    plt.xlabel('Cluster Sizes')
    plt.ylabel('Average Similarity')
    plt.title('Smooth Curve with Shaded Error Regions')
    plt.legend()
    # plt.show()
    plt.savefig('similarity_vs_cluster_size_gt10_exc600_smooth_trend.png')




if __name__ == "__main__":# 3374836

    file_path = '../step_10_cluster/files/results/topo_only_scale_1_10/cocitation_jaccard_0.25.json'

    clusters = read_json_clustering(file_path)
    non_singletons = []

    with open('original_ids.pkl', 'rb') as f:
        original_ids = pickle.load(f)


    arguments = []
    idx = 0

    manager = Manager()
    manager_dict = manager.dict()

    total = 0

    for cluster_name, cluster_list in clusters.items():
        # if len(cluster_list) > 10:
        if len(cluster_list) > 10 and len(cluster_list) < 600:
            arguments.append((cluster_list, idx, manager_dict))
            idx += 1
            total += len(cluster_list)

    start = time.time()

    with Pool(16) as pool:
        # results = pool.starmap(similarity_wrapper, tqdm.tqdm(arguments, total=len(arguments)))
        results = pool.starmap(similarity_wrapper, arguments)

    obtained_total = 0

    cluster_sizes = []
    avg_similarity = []
    idx = 0
    whole_stat_dict = {}

    total_similarities = 0

    for key, value in manager_dict.items():
        obtained_total += value[1]
        cluster_sizes.append(value[1])
        avg_similarity.append(value[0])
        whole_stat_dict[idx] = {'stats': value[2]}
        idx += 1
        total_similarities += (value[1]*(value[1]-1))/2

    with open('stats_gt10.json', 'w') as file:
        json.dump(whole_stat_dict, file, indent=4)

    # (average_similarity, embeddings.shape[0], similarity_df, stats)

    print(obtained_total, total)
    print(total_similarities)
    # plt.scatter(cluster_sizes, avg_similarity, c='blue', marker='o', alpha=0.7)

    # # Add labels and title
    # plt.xlabel('Cluster sizes')
    # plt.ylabel('Average similarity')
    # plt.title('Scatter Plot of Similarity vs Cluster size')
    #
    # plt.show()
    # # plt.savefig('similarity_vs_cluster_size_gt100.png')

    plot_with_errors(cluster_sizes, avg_similarity)

    end = time.time()
    print(f'elasped {end - start}')
    #estimate: ~ 10 min