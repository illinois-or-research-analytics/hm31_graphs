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
    data_dir = 'xml_data/'
    files = os.listdir(data_dir)

    lst = []
    for file in files:
        if '.gz' in file:
            print(file)












