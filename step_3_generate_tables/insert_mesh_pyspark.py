# importing module
import pyspark
import json
from pyspark.sql import SparkSession



if __name__ == "__main__":
    file_path = 'all_mesh_retrieved.json'

    # Open the JSON file for reading
    with open(file_path, 'r') as json_file:
        # Load the JSON data
        all_mesh_terms = json.load(json_file)

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", "postgresql-42.5.2.jar").config("spark.executor.extraClassPath", "postgresql-42.5.2.jar").getOrCreate()

    dict_list = [{"mesh_term": key, "mesh_tree": value} for key, value in all_mesh_terms.items()]

    # creating a dataframe
    dataframe = spark.createDataFrame(dict_list)

    jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
    jdbc_properties = {
        "user": "",
        "password": "",
        "driver": "org.postgresql.Driver"
    }

    # Specify the target database table
    table_name = "hm31.mesh"  # Replace with your actual table name

    # Write the DataFrame to the PostgreSQL table
    dataframe.select("mesh_term", "mesh_tree").write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=jdbc_properties)

    # show data frame
    dataframe.show()
    #https://stackoverflow.com/questions/68513383/pyspark-dataframe-error-due-to-java-lang-classnotfoundexception-org-postgresql