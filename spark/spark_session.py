from pyspark.sql import SparkSession
import os

def get_spark():

    warehouse_path = os.path.abspath("data/iceberg")

    return SparkSession.builder \
        .appName("financial-etl") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse",
                f"file:///{warehouse_path.replace(os.sep, '/')}") \
        .getOrCreate()

