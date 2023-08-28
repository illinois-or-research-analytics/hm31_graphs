"""
Assuming reformatted.tsv exists in the current directory
"""
import csv
from tqdm import tqdm
import xmltodict



def open_csv_first_cluster_nodes(file='reformatted.tsv'):
    data = []
    with open(file, 'r', newline='') as csv_file:
        csv_reader = csv.reader(csv_file)
        i = 0
        for row in tqdm(csv_reader):
            data.append(row)
            # We add this part so we don't process the rest of lines
            i += 1
            # if i > 2003:
            #     break

    return data

def obtain_first_cluster_edges(first_cluster_nodes, file='CEN.tsv'):
    data = []
    with open(file, 'r', newline='') as csv_file:
        csv_reader = csv.reader(csv_file)
        i = 0
        first_cluster_edges = []
        for row in tqdm(csv_reader):
            first, second = row[0].split("\t")
            first = int(first)
            second = int(second)
            i+= 1
            if first in first_cluster_nodes and second in first_cluster_nodes:
                first_cluster_edges.append((first, second))

    return first_cluster_edges

def write_tuple_list_to_tsv(tuple_list, file='first_cluster_edges.tsv'):
    with open(file, 'w', newline='') as tsv_file:
        tsv_writer = csv.writer(tsv_file, delimiter='\t')
        for tuple_item in tuple_list:
            tsv_writer.writerow(tuple_item)

"""Get info about clusters. It turns out first (id = 1) cluster is not the largest one"""
def obtain_cluster_statistics():
    clusters = {}
    data = open_csv_first_cluster_nodes()
    i=0
    for row in tqdm(data):
            # 'row' is a list containing values of each column in the current row
            node, cluster = row[0].split(' ');
            node = int(node)
            cluster = int(cluster)

            if not cluster in clusters:
                clusters[cluster] = 0

            clusters[cluster] += 1



    print(clusters[1])
    clusters = sorted(clusters.items(), key=lambda x: x[1], reverse=True)
    print(clusters[0:10])


def obtain_first_cluster():
    first_cluster = []
    data = open_csv_first_cluster_nodes()
    for row in tqdm(data):
        # 'row' is a list containing values of each column in the current row
        node, cluster = row[0].split(' ');
        node = int(node)
        cluster = int(cluster)

        if cluster == 1:
            first_cluster.append(node)

    return(first_cluster)



def write_int_list_to_tsv(int_list, file='output.tsv'):
    with open(file, 'w', newline='') as tsv_file:
        tsv_writer = csv.writer(tsv_file, delimiter='\t')
        for number in int_list:
            tsv_writer.writerow([number])


def assert_edges_are_within_first_cluster():
    edges = open_csv_first_cluster_nodes('../data/first_cluster_edges.tsv')
    nodes = open_csv_first_cluster_nodes('../data/first_cluster_nodes.tsv')

    # print(type(nodes))
    # print("nodes : ",nodes[0:10])
    # print("edges : ", edges[0:10])

    nodes_array = []
    edge_array = []

    for node in nodes:
        nodes_array.append(int(node[0]))

    for edge in edges:
        first, second = edge[0].split("\t")
        first = int(first)
        second = int(second)
        edge_array.append((first, second))

        assert  first in nodes_array
        assert  second in nodes_array



    return nodes_array, edge_array
#Save first cluster nodes
# first_cluster_nodes = obtain_first_cluster()
# write_int_list_to_tsv( first_cluster_nodes, 'first_cluster_nodes.tsv')
# first_cluster_edges = obtain_first_cluster_edges(first_cluster_nodes)
# write_tuple_list_to_tsv(first_cluster_edges)
# assert_edges_are_within_first_cluster()
