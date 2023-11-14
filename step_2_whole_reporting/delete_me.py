from pyspark.sql import SparkSession
import re
import os
import json
import time
from pyspark import SparkConf, SparkContext

def count_occurence(rdd):

    print("before")

    substrings = ['ArticleTitle', 'Abstract']

    # print(type(rdd),'yipi-ka-yay')

    lines = rdd[1].map(lambda x: x.value)

    # Initialize a dictionary to store counts for each substring
    substring_counts = {substring: 0 for substring in substrings}

    for substring_to_count in substrings:
        pattern = r'\b{}\b'.format(re.escape(substring_to_count))
        substring_count = lines.flatMap(lambda line: re.findall(pattern, line, re.IGNORECASE)).count()
        substring_counts[
            substring_to_count] = substring_count / 2  # Assuming you want to divide by 2 as in your example

    print("after")
    print(substring_count)


if __name__ == "__main__":
    data_dir = '/shared/hm31/xml_data/'
    data_dir = '/home/hm31/step_1/test/'
    data_dir = '/shared/hossein_hm31/xml_data'
    data_dir = '/shared/hossein_hm31/test_dir'
    #data_dir = 'test/'



    # conf = SparkConf().setMaster("local[50]").setAppName("WordCount")
    conf = SparkConf().setMaster("local[50]").setAppName("WordCount") \
        .set("spark.driver.maxResultSize", "4g")


    sc = SparkContext(conf = conf)
    sc.setLogLevel('WARN')


    rdd = sc.wholeTextFiles(data_dir,512)
    rdd.map(count_occurence).collect()
    # rdd.collect()


    # file = rdd.collect()
    # f0 = file[0]
    #
    # print(type())
    # print(type(f0[0]),type(f0[1]))
    #
    # print(f0[0])
    # print(rdd.count())




