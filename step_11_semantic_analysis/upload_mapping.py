from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
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
    "driver": "org.postgresql.Driver",
    'jdbc_url' : "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"
    }


    # Path to your CSV file
    csv_file_path = 'mapping.csv'

    # Read CSV file into a DataFrame
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    # Write DataFrame to PostgreSQL table
    write_df(df, jdbc_url, 'hm31.CENM_node_id_mapping', jdbc_properties)

    spark.stop()

def write_df(result, jdbc_url, table_name, jdbc_properties):
    result.repartition(50).write.jdbc(url=jdbc_url, table=table_name, mode="overwrite",
                                      properties=jdbc_properties)

if __name__ == "__main__":
    main()
