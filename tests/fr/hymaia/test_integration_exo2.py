import os
import subprocess
import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import clean_job as clean_main
from src.fr.hymaia.exo2.spark_aggregate_job import aggregate_job as aggregate_main


class TestIntegrationJobs(unittest.TestCase):
    def test_integration_exo2(self):
        job1script_path = "src/fr/hymaia/exo2/spark_clean_job.py"

        subprocess.run(["spark-submit", job1script_path])
        output_path = "data/exo2/output.parquet"
        assert os.path.exists(output_path), "Le fichier parquet de sortie du job 2 n'a pas été créé."

        job2_script_path = "src/fr/hymaia/exo2/spark_aggregate_job.py"

        subprocess.run(["spark-submit", job2_script_path])

        output_csv_path = "data/exo2/aggregate.csv"
        assert os.path.exists(output_csv_path), "Le fichier CSV de sortie du job 2 n'a pas été créé."
