
from load_data import *

nodes_array, edge_array = assert_edges_are_within_first_cluster()

import csv

csv_file_path = '../data/exosome.csv'

with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)

    for line_number, row in enumerate(csv_reader, start=1):
        # 'line_number' is the counter for the current line
        # 'row' is a list representing a line in the CSV file
        print(f"Line {line_number}: {row}")  # Print line number and row content
        if line_number > 100:
            break


# %%
