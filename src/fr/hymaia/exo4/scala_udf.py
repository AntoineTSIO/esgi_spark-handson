import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.column import Column, _to_java_column, _to_seq

start_time = time.time()

# Create Spark session
spark = SparkSession.builder.master("local[*]").appName("scala_udf").config('spark.jars',
                                                                            'src/resources/exo4/udf.jar').getOrCreate()


def addCategoryName(col):
    # On récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


def main():
    # Read CSV
    df = spark.read.csv("./src/resources/exo4/sell.csv", header=True)

    # Apply UDF
    df = df.withColumn("category_name", addCategoryName(col("category")))

    start_time = time.time()
    df.write.csv("data/exo4/scala", header=True, mode="overwrite")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Temps écoulé pour écrire le DataFrame:", elapsed_time, "secondes")
    spark.stop()
