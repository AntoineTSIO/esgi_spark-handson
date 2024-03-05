from src.fr.hymaia.spark_session import spark
from src.fr.hymaia.exo2.agregate.aggregate_functions import departement_count


def aggregate_job(spark_session, input_path, output_path):
    clean_df = spark_session.read.format("parquet").load(input_path)
    result_df = departement_count(clean_df)

    result_df.write.mode("overwrite").option("header", True).csv(output_path)


def main():
    input_path = "data/exo2/output.parquet"
    output_path = "data/exo2/aggregate.csv"

    aggregate_job(spark, input_path, output_path)
