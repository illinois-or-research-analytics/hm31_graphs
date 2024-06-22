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

# Set the PGDATABASE environment variable
os.environ["PGDATABASE"] = "ernieplus"

def fetch_embeddings_from_table(node_id_list):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("")
        cur = conn.cursor()

        # Convert the node_id_list to a batch query format
        node_ids = ','.join(cur.mogrify("%s", (node_id,)).decode('utf-8') for node_id in node_id_list)

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
    A = np.matmul(embedding_matrix.transpose(), embedding_matrix)
    lower_triangle = np.tril(A)
    n = A.shape[1]

    average_similarity = (2 * np.sum(lower_triangle))/(n*(n-1))
    return average_similarity


def wrapper(node_id_lst, cluster_index, manager_dict):
    embeddings = fetch_embeddings_from_table(node_id_lst)
    average_similarity = calculate_average_similarity(embeddings)

    manager_dict[cluster_index] = (average_similarity, embeddings.shape[0])


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


    for cluster_name, cluster_list in clusters.items():
        if len(cluster_list) > 100:
            arguments.append((cluster_list, idx, manager_dict))
            idx += 1





    # def wrapper(node_id_lst, cluster_index, manager_dict):

    start = time.time()

    # inputs = zip(f, s, t)
    with Pool(8) as pool:
        results = pool.starmap(wrapper, tqdm.tqdm(arguments, total=len(arguments)))

    end = time.time()


    #estimate: ~ 1.5 hr