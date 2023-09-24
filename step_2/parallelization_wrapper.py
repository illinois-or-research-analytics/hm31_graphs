import subprocess

from pyspark.sql import SparkSession
import re
import os
import json
import time
import numpy as np

if __name__ == "__main__":
    data_dir = '/shared/hm31/xml_data/'


    xml_files = os.listdir(data_dir)
    SPLIT = 100
    indices = np.ceil(len(xml_files)//SPLIT)

    for i in range(indices):
        command = f"spark-submit main_parallel.py --master=local[{(256)//len(indices)-1}] --driver-memory {95//len(indices)} {SPLIT} {i}"

        # Run the shell command asynchronously
        process = subprocess.Popen(command, shell=True)

    # Stop the SparkSession