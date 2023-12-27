from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
import json

missing = [17636573, 17636577, 17636589, 17636596, 17636622, 17636628, 17636639, 17636642, 17636652, 17636674, 17636677,
           17636689, 17636698, 17636700, 17636737, 17636763, 17636788, 17636822, 17943818, 18253972, 18254063, 18425874,
           18425887, 18646091, 18843611, 19588329,19588331,19588338, 19588368,19821291,19821312,20091530,20091572,20464727,
           20927727,20951348,21249661,21249665,21328281,21735387,22258944,22258947,22336784,22513924,22513935,27852100,
           28004389,28334434,28334435,28368089,28387447,28542713,28715610, 28789596,28954281,28991361,29409139,29411867, 30184244,30489630,37066509]




if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", "postgresql-42.5.2.jar").config("spark.executor.extraClassPath","postgresql-42.5.2.jar") \
        .config("spark.local.dir", "/shared/hm31") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
    jdbc_properties = {
        "user": "hm31",
        "password": "graphs",
        "driver": "org.postgresql.Driver"
    }

    jdbc_properties = {
        "user": "hm31",
        "password": "graphs",
        "driver": "org.postgresql.Driver",
        'jdbc_url' : "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
    }



    spark.sparkContext.setLogLevel("WARN")

    parquet_path = '/shared/hossein_hm31/pubmed_parquet/'

    try:
        df = spark.read.parquet(parquet_path)

        schema = StructType([StructField("pmid", IntegerType(), True)])
        data_tuples = [(value,) for value in missing]

        # Create a PySpark DataFrame
        missing = spark.createDataFrame(data_tuples, schema=schema)

        # missing.show()

        missing.createOrReplaceTempView("missing_nodes")
        df.createOrReplaceTempView("all_embeddings")

        sql_query = """
            select a.* from all_embeddings a inner join missing_nodes u on u.pmid = a.pmid
        """
        result =  spark.sql(sql_query)



        print("num", result.count())

        result.show()


        result.repartition(1).write.parquet('./missing')




        spark.stop()
        exit(0)





    except Exception as e:

        print(f'Error: {e}')
        spark.stop()
        exit(0)



    spark.stop()
    exit(0)
