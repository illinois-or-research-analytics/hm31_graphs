from pyspark.sql import SparkSession
import re
import os
import json
import time
from pyspark import SparkConf, SparkContext

def count_occurence(rdd, spark_context):

    print("before")

    substrings = ['ArticleTitle', 'Abstract']


    lines = rdd[1].map(lambda x: x.value)

    substring_counts = {substring: 0 for substring in substrings}

    for substring_to_count in substrings:
        pattern = r'\b{}\b'.format(re.escape(substring_to_count))
        substring_count = lines.flatMap(lambda line: re.findall(pattern, line, re.IGNORECASE)).count()
        substring_counts[
            substring_to_count] = substring_count / 2

    print("after")
    print(substring_count)


if __name__ == "__main__":
    data_dir = '/shared/hm31/xml_data/'
    data_dir = '/home/hm31/step_1/test/'
    data_dir = '/shared/hossein_hm31/xml_data'
    data_dir = '/shared/hossein_hm31/test_dir'



    conf = SparkConf().setMaster("local[50]").setAppName("WordCount") \
        .set("spark.driver.maxResultSize", "4g")


    sc = SparkContext(conf = conf)
    sc.setLogLevel('WARN')


    rdd = sc.wholeTextFiles(data_dir,512)
    rdd.map(count_occurence).collect()



