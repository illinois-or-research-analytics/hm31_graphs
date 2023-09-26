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
    indices = int(len(xml_files)//SPLIT) + 1

    for i in range(indices):
        command = f"spark-submit main_parallel.py  {SPLIT} {i}"

        # Run the shell command asynchronously
        process = subprocess.Popen(command, shell=True)

    # Stop the SparkSession