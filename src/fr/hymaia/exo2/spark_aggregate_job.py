import os
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.agregate.aggregate_functions import departement_count


def aggregate_job(input_path, output_path):
    spark = SparkSession.builder.master("local[*]").appName("spark_aggregate_job").getOrCreate()
    clean_df = spark.read.format("parquet").load(input_path)

    result_df = departement_count(clean_df)

    result_df.write.mode("overwrite").csv(output_path)
    spark.stop()


def main():
    path = "data/exo2/output.parquet"
    output_path = "data/exo2/aggregate.csv"

    aggregate_job(path, output_path)
