import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()

    input_path = "src/resources/exo1/data.csv"
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    result_df = wordcount(df, "text")

    output_path = "data/exo1/output"
    result_df.write.partitionBy("count").parquet(output_path)


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
