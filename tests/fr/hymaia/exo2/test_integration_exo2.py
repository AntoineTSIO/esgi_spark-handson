import unittest
import pytest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_aggregate_job import aggregate_job
from src.fr.hymaia.exo2.spark_clean_job import clean_job


class TestIntegrationJobs(unittest.TestCase):

    # def test_integration_exo2(self):
    #     job1script_path = "src/fr/hymaia/exo2/spark_clean_job.py"
    #
    #     subprocess.run(["spark-submit", job1script_path])
    #     output_path = "data/exo2/output.parquet"
    #     assert os.path.exists(output_path), "Le fichier parquet de sortie du job 1 n'a pas été créé."
    #
    #     job2_script_path = "src/fr/hymaia/exo2/spark_aggregate_job.py"
    #
    #     subprocess.run(["spark-submit", job2_script_path])
    #     output_csv_path = "data/exo2/aggregate.csv"
    #     assert os.path.exists(output_csv_path), "Le fichier CSV de sortie du job 2 n'a pas été créé."

    # def test_integration(self, input_data_path, output_data_path):
    #     """Test the integration of clean and aggregate jobs."""
    #     city_zipcode_path, clients_bdd_path, output_path = input_data_path
    #     # Generate some sample data
    #     sample_city_zipcode_data = [
    #         (1, "Paris", 75000),
    #         (2, "Marseille", 13000),
    #     ]
    #     sample_clients_bdd_data = [
    #         (101, "John Doe", 1),
    #         (102, "Jane Doe", 2),
    #     ]
    #     # Write sample data to CSV files
    #     self.createDataFrame(sample_city_zipcode_data, ["id", "city", "zipcode"]).write.csv(
    #         city_zipcode_path, mode="overwrite", header=True)
    #     self.createDataFrame(sample_clients_bdd_data, ["client_id", "client_name", "zipcode"]).write.csv(
    #         clients_bdd_path, mode="overwrite", header=True)
    #
    #     # Execute clean job
    #     clean_job(self, city_zipcode_path, clients_bdd_path, output_path)
    #
    #     # Execute aggregate job
    #     aggregate_job(self, output_path, output_data_path)
    #
    #     # Read and assert the result
    #     result_df = self.read.csv(output_data_path, header=True, inferSchema=True)
    #     assert result_df.collect() == [
    #         ("Paris", 1),
    #         ("Marseille", 1),
    #     ]
    pass
