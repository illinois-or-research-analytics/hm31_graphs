import time
from load_data import *
import numpy as np
import jupyter

def create_lookup_dict(nodes_array):
    global nodes_lookup_dict
    for i in range(len(nodes_array)):
        nodes_lookup_dict[nodes_array[i]] = i

#Each node is not its index
def construct_adjacency_matrix(nodes_array, calculate = False):

    if calculate == False:
        cocitation_matrix = np.load("adj_matrix.npy")
        return cocitation_matrix

    adj_matrix = np.zeros((len(nodes_array), len(nodes_array)))

    for edge in edge_array:
        first, second = edge
        first =nodes_lookup_dict[first]
        second =nodes_lookup_dict[second]

        adj_matrix[first][second] = 1
    np.save("adj_matrix.npy", adj_matrix)

    return adj_matrix


def construct_cocitation_matrix(adj_matrix, calculate = False):
    vals = []
    if calculate == False:
        cocitation_matrix = np.load("cocitation_matrix.npy")

        for i in range(adj_matrix.shape[0]):
            for j in range(i):
                vals.append(cocitation_matrix[i][j])

        return cocitation_matrix, vals

    cocitation_matrix = np.zeros_like(adj_matrix)

    for i in range(adj_matrix.shape[0]):
        for j in range(i):
            cocitation_matrix[i][j] = np.dot(adj_matrix[:, i], adj_matrix[:, j])
            vals.append(cocitation_matrix[i][j])


    cocitation_matrix = cocitation_matrix + cocitation_matrix.transpose()
    np.save("cocitation_matrix.npy", cocitation_matrix)


    return cocitation_matrix, vals


def construct_bib_coupling_matrix(adj_matrix, calculate = False):
    vals = []

    if calculate == False:
        bib_coupling_matrix = np.load("bib_coupling_matrix.npy")
        for i in range(adj_matrix.shape[0]):
            for j in range(i):
                vals.append(bib_coupling_matrix[i][j])

        return bib_coupling_matrix, vals

    bib_coupling_matrix = np.zeros_like(adj_matrix)

    for i in range(adj_matrix.shape[0]):
        for j in range(i):
            bib_coupling_matrix[i][j] = np.dot(adj_matrix[i , :], adj_matrix[j , :])
            vals.append(bib_coupling_matrix[i][j])

    bib_coupling_matrix = bib_coupling_matrix + bib_coupling_matrix.transpose()
    np.save("bib_coupling_matrix.npy", bib_coupling_matrix)

    return bib_coupling_matrix, vals

#Manually calculate co-citation and bib-coupling and verify with the original matrix
#Also verify the original matrix is symmetric
def calc_manual(mode, first_node, second_node, adj_matrix, obtained_matrix):
    allowable_modes = ['co-citation', 'bib-couple']
    assert  mode in allowable_modes

    answer = 0

    #co-citation
    if mode == 'bib-couple':
        for node in range(adj_matrix.shape[0]):
            if adj_matrix[first_node][node] == 1 and adj_matrix[second_node][node] == 1:
                answer +=1

        assert (answer == obtained_matrix[first_node][second_node] and answer == obtained_matrix [second_node][first_node])


    #co-cite
    else:
        for node in range(adj_matrix.shape[0]):
            if adj_matrix[node][first_node] == 1 and adj_matrix[node][second_node] == 1:
                answer += 1

        assert (answer == obtained_matrix[first_node][second_node] and answer == obtained_matrix[second_node][first_node])

    print(f'node {first_node} and nose {second_node} have {answer} {mode}')

def report_stats(vals, mode):

    median = np.median(vals)
    mean = np.mean(vals)
    min = np.amin(vals)
    max = np.amax(vals)
    total = np.sum(vals)

    print(f'{mode} median {median} mean {mean} min {min} max {max}, total {total}')

#######################################################################################
nodes_array, edge_array = assert_edges_are_within_first_cluster()
print(len(edge_array))
nodes_lookup_dict = {}

create_lookup_dict(nodes_array)
adj_matrix = construct_adjacency_matrix(nodes_array)

cocitation_matrix, co_citation_vals = construct_cocitation_matrix(adj_matrix, False)
bib_coupling_matrix, bib_coupling_vals = construct_bib_coupling_matrix(adj_matrix, False)


calc_manual(mode= 'co-citation', first_node = 3, second_node = 5, adj_matrix = adj_matrix, obtained_matrix = cocitation_matrix)
calc_manual(mode= 'bib-couple', first_node = 9, second_node = 3, adj_matrix = adj_matrix, obtained_matrix = bib_coupling_matrix)

report_stats(co_citation_vals, 'co-citation')
report_stats(bib_coupling_vals, 'bib-couple')


