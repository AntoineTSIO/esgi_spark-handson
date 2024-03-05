import pyspark.sql.functions as f
from src.fr.hymaia.spark_session import spark


def main():
    input_path = "src/resources/exo1/data.csv"
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    result_df = wordcount(df, "text")

    output_path = "data/exo1/output"
    result_df.write.partitionBy("count").parquet(output_path)


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
