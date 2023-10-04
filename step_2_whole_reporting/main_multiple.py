from pyspark.sql import SparkSession
import re
import os
import json
import time
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    data_dir = '/shared/hm31/xml_data/'
    data_dir = '/home/hm31/step_1/test/'
    #data_dir = 'test/'


    #conf = SparkConf().setMaster("local").setAppName("WordCount")


    conf = SparkConf().setMaster("local[200]").setAppName("WordCount")
    #conf = SparkConf().setAppName("WordCount")
    #conf.set("spark.driver.memory", "200g")
    #conf.set("spark.executor.memory", "8g")  # Example: Increase executor memory
    #conf.set("spark.driver.maxResultSize", "8g")  # Example: Increase executor memory

    sc = SparkContext(conf = conf)

    rdd = sc.wholeTextFiles(data_dir,512)
    words = rdd.flatMap(lambda x: x.split())
    print("kkk",words.count())
