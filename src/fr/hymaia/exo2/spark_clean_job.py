from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.clean.clean_functions import join_dataframes, add_departement_column, filter_major_clients


def clean_job(spark, input_path1, input_path2, output_path):
    city_zipcode_df = spark.read.csv(input_path1, header=True, inferSchema=True)
    clients_bdd_df = spark.read.csv(input_path2, header=True, inferSchema=True)

    clients_bdd_df = filter_major_clients(clients_bdd_df)
    result_df = join_dataframes(clients_bdd_df, city_zipcode_df)

    result_df_with_departement = add_departement_column(result_df)

    result_df_with_departement.write.mode("overwrite").parquet(output_path)


def main():
    input_path1 = "src/resources/exo2/city_zipcode.csv"
    input_path2 = "src/resources/exo2/clients_bdd.csv"
    output_path = "data/exo2/output.parquet"

    spark = SparkSession.builder.master("local[*]").appName("spark_clean_job").getOrCreate()

    clean_job(spark, input_path1, input_path2, output_path)
