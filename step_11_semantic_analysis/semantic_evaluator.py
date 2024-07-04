import json
import os
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



# Set the PGDATABASE environment variable
os.environ["PGDATABASE"] = "ernieplus"

def fetch_embeddings_from_table(squashed_node_id_list):
    try:
        squashed_node_id_list.sort()
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("")
        cur = conn.cursor()

        # Convert the node_id_list to a batch query format
        node_ids = ','.join(cur.mogrify("%s", (node_id,)).decode('utf-8') for node_id in squashed_node_id_list)

        table_name = 'hm31.cenm_squashed_cleaned_embeddings'
        # Execute the SELECT query to fetch the corresponding embeddings
        query = f"SELECT squashed_id, embedding FROM {table_name} WHERE squashed_id IN ({node_ids})"
        cur.execute(query)

        # Fetch all results
        results = cur.fetchall()

        # Process the results if needed
        embeddings_lst = [embedding for node_id, embedding in results]
        embedding_arr = np.asarray(embeddings_lst)

        return embedding_arr

    except Exception as e:
        print("Error:", e)
        return "fetch error"

    finally:
        cur.close()
        conn.close()


def calculate_average_similarity(embedding_matrix):
    n = embedding_matrix.shape[0]

    if n < 100:
        A = np.matmul(embedding_matrix, embedding_matrix.transpose())
        lower_triangle = np.tril(A)


        average_similarity = (2 * np.sum(lower_triangle))/(n*(n-1))
        return average_similarity

    else:
        sum = 0
        for i in range(n):
            for j in range(i):
                sum += np.matmul(embedding_matrix[i,:], embedding_matrix[j, :])
        sum = (2* sum)/(n*(n-1))
        return sum


def similarity_wrapper(node_id_lst, cluster_index, manager_dict):
    embeddings = fetch_embeddings_from_table(node_id_lst)
    average_similarity = calculate_average_similarity(embeddings)

    manager_dict[cluster_index] = (average_similarity, embeddings.shape[0])


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

if __name__ == "__main__":
    file_path = '../step_10_cluster/files/results/topo_only_scale_1_10/cocitation_jaccard_0.25.json'

    clusters = read_json_clustering(file_path)
    non_singletons = []

    arguments = []
    idx = 0

    manager = Manager()
    manager_dict = manager.dict()

    total = 0

    for cluster_name, cluster_list in clusters.items():
        if len(cluster_list) > 100:
            arguments.append((cluster_list, idx, manager_dict))
            idx += 1
            total += len(cluster_list)




    # def wrapper(node_id_lst, cluster_index, manager_dict):

    start = time.time()

    with Pool(16) as pool:
        results = pool.starmap(similarity_wrapper, tqdm.tqdm(arguments, total=len(arguments)))

    obtained_total = 0

    cluster_sizes = []
    avg_similarity = []

    for key, value in manager_dict.items():
        obtained_total += value[-1]
        cluster_sizes.append(value[-1])
        avg_similarity.append(value[0])

    # print(obtained_total, total)

    plt.scatter(cluster_sizes, avg_similarity, c='blue', marker='o', alpha=0.7)

    # Add labels and title
    plt.xlabel('Cluster sizes')
    plt.ylabel('Average similarity')
    plt.title('Scatter Plot of Similarity vs Cluster size')

    plt.show()
    # plt.savefig('similarity_vs_cluster_size_gt100.png')


    end = time.time()






    #estimate: ~ 1.5 hr