import os
import psycopg2
from psycopg2 import sql
import multiprocessing

os.environ["PGDATABASE"] = "ernieplus"

def find_common_count(arr1, arr2):
    size1, size2 = len(arr1), len(arr2)
    i, j, count = 0, 0, 0

    while i < size1 and j < size2:
        if arr1[i] == arr2[j]:
            # Found an element in the intersection
            count += 1
            i += 1
            j += 1
        elif arr1[i] < arr2[j]:
            # Move the pointer in the first array
            i += 1
        else:
            # Move the pointer in the second array
            j += 1

    return count
def select_rows_with_range(start_index, end_index):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("")  # Replace with your connection parameters
        cur = conn.cursor()

        # Execute the SELECT query to obtain rows within the specified range
        cur.execute("SELECT * FROM hm31.in_edges_features_cert OFFSET %s LIMIT %s", (start_index, end_index - start_index))

        # Fetch the results
        results = cur.fetchall()

        # Print the results
        #         for row in results:
        #             print("Row:", type(row))

        return results

    except Exception as e:
        print("Error:", e)

    finally:
        cur.close()
        conn.close()

def find_common_count(arr1, arr2):
    size1, size2 = len(arr1), len(arr2)
    i, j, count = 0, 0, 0

    while i < size1 and j < size2:
        if arr1[i] == arr2[j]:
            # Found an element in the intersection
            count += 1
            i += 1
            j += 1
        elif arr1[i] < arr2[j]:
            # Move the pointer in the first array
            i += 1
        else:
            # Move the pointer in the second array
            j += 1

    return count

def process_cocitation_row(row):
    first_in = row[1]

    second_in = row[3]

    first_in_nodes = first_in.split(',')
    second_in_nodes = second_in.split(',')

    first_in_nodes.sort()
    second_in_nodes.sort()

    intersection_count = find_common_count(first_in_nodes, second_in_nodes)

    jaccard = 0.0

    if len(first_in_nodes) + len(second_in_nodes) == 0:
        jaccard = 0.0

    else:
        jaccard = float(intersection_count/(len(first_in_nodes) + len(second_in_nodes) - intersection_count))

    return jaccard, intersection_count

def process_batch(rows):
    results = []
    for row in rows:
        first_node = row[0]
        second_node = row[2]

        jaccard, intersection_count = process_cocitation_row(row)
        results.append((first_node, second_node, jaccard, intersection_count))

    return results


def insert_values_into_table(values_list):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("")
        cur = conn.cursor()


        args = ','.join(cur.mogrify("(%s, %s, %s, %s)", i).decode('utf-8') for i in values_list)

        cur.execute("INSERT INTO hm31.cocitation_edge_weights_cert VALUES " + (args))

        conn.commit()

    except Exception as e:
        print("Error:", e)

    finally:
        cur.close()
        conn.close()



def get_table_count():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("")  # Replace with your connection parameters
        cur = conn.cursor()

        # Execute the SELECT COUNT(*) query
        cur.execute("SELECT COUNT(*) FROM hm31.in_edges_features_cert")

        # Fetch the result
        result = cur.fetchone()

        # Print the result
        return result[0]

    except Exception as e:
        print("Error:", e)

    finally:
        cur.close()
        conn.close()

def insert_wrapper(start_index, end_index):
    rows = select_rows_with_range(start_index, end_index)
    results = process_batch(rows)
    insert_values_into_table(results)


if __name__ == '__main__': #81958
    count = int(get_table_count())
    lst = []

    SPLIT_LENGTH = 100
    CHUNCKS = count // SPLIT_LENGTH + 1

    for i in range(CHUNCKS):
        lst.append((i*SPLIT_LENGTH,(i+1)* SPLIT_LENGTH))

    lst.append((44509700, 44509707))

    with multiprocessing.Pool(processes=50) as pool:
        results = pool.starmap(insert_wrapper, lst)

