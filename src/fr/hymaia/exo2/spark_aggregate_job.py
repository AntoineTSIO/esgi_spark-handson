from pyspark.sql import SparkSession

from src.fr.hymaia.exo2.agregate.aggregate_functions import departement_count


def main():
    spark = SparkSession.builder.master("local[*]").appName("spark_aggregate_job").getOrCreate()

    path = "data/exo2/output.parquet"
    clean_df = spark.read.format("parquet").load(path)

    result_df = departement_count(clean_df)

    result_df.write.csv("data/exo2/aggregate.csv")

    spark.stop()
