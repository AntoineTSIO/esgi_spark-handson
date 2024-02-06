import os
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.clean.clean_functions import join_dataframes, add_departement_column


def main():
    spark = SparkSession.builder.master("local[*]").appName("spark_clean_job").getOrCreate()

    city_zipcode = "src/resources/exo2/city_zipcode.csv"
    clients_bdd = "src/resources/exo2/clients_bdd.csv"
    output_path = "data/exo2/output.parquet"

    city_zipcode_df = spark.read.csv(city_zipcode, header=True, inferSchema=True)
    clients_bdd_df = spark.read.csv(clients_bdd, header=True, inferSchema=True)

    result_df = join_dataframes(clients_bdd_df, city_zipcode_df)

    result_df_with_departement = add_departement_column(result_df)

    result_df_with_departement.write.mode("overwrite").parquet(output_path)

    spark.stop()
