import unittest

from pyspark import Row

from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.spark_aggregate_job import aggregate_job
from src.fr.hymaia.exo2.spark_clean_job import clean_job


class TestIntegrationJobs(unittest.TestCase):

    def test_integration(self):
        city_zipcode_path = "tests/resources/exo2_integration_test/city_zipcode.csv"
        clients_bdd_path = "tests/resources/exo2_integration_test/clients_bdd.csv"
        output_path = "tests/resources/exo2_integration_test/output.csv"
        output_data_path = "tests/resources/exo2_integration_test/final_output.csv"

        sample_city_zipcode_data = [
            (75000, "Paris"),
            (13000, "Marseille"),
        ]
        sample_clients_bdd_data = [
            ("John", 42, 75000),
            ("Jack", 16, 75000),
            ("Jane", 28, 13000),
        ]
        spark.createDataFrame(sample_city_zipcode_data, ["zip", "city"]).write.csv(
            city_zipcode_path, mode="overwrite", header=True)
        spark.createDataFrame(sample_clients_bdd_data, ["name", "age", "zip"]).write.csv(
            clients_bdd_path, mode="overwrite", header=True)

        clean_job(spark, city_zipcode_path, clients_bdd_path, output_path)

        aggregate_job(spark, output_path, output_data_path)

        result_df = spark.read.csv(output_data_path, header=True, inferSchema=True)

        expected_df = spark.createDataFrame(
            [
                Row(departement=13, nb_people=1),
                Row(departement=75, nb_people=1),
            ]
        )
        self.assertEqual(result_df.collect(), expected_df.collect())
        self.assertEqual(result_df.columns, expected_df.columns)
