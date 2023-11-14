from pyspark.sql import SparkSession
import re
import os
from tqdm import tqdm
import multiprocessing

def uz(file, data_dir):


    os.system(f'cd {data_dir} && gunzip {file}')
    # os.system(f'cd {data_dir} && rm {file}')


def dl(file, data_dir):
    os.system(f'cd {data_dir} && gunzip {file}')
    # os.system(f'cd {data_dir} && rm {file}')

if __name__ == "__main__":
    data_dir = '/shared/hm31/xml_data/'
    files = os.listdir(data_dir)

    lst = []
    other = []
    for file in files:
        if '.gz' in file:
            print(file)
            lst.append(1)
            # os.system(f'cd {data_dir} && gunzip {file}')
            # os.system(f'cd {data_dir} && gunzip {file}')

        else:
            other.append(1)
    print(len(lst))
    print(len(other))

    print(len(files))












