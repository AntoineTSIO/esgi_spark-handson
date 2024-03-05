from pyspark import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.utils import AnalysisException

from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.clean.clean_functions import join_dataframes, add_departement_column, filter_major_clients


class TestMain(unittest.TestCase):
    def test_join_dataframes(self):
        input1 = spark.createDataFrame(
            [
                Row(name="Alice", age=25, zip="12345"),
                Row(name="Bob", age=30, zip="67890")
            ]
        )
        input2 = spark.createDataFrame(
            [
                Row(zip="12345", city="Paris"),
                Row(zip="67890", city="New York")
            ]
        )
        expected_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("zip", StringType(), True),
            StructField("city", StringType(), True)
        ])
        expected_data = [
            ("Alice", 25, "12345", "Paris"),
            ("Bob", 30, "67890", "New York")
        ]
        expected = spark.createDataFrame(expected_data, schema=expected_schema)

        actual = join_dataframes(input1, input2)

        self.assertEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.columns, expected.columns)

    def test_add_departement_column(self):
        input = spark.createDataFrame(
            [
                Row(name="Alice", age=25, zip="12345", city="Paris"),
                Row(name="Michel", age=70, zip="20050", city="Lyon"),
                Row(name="Gérard", age=55, zip="20932", city="Marseille"),
                Row(name="Tristan", age=10, zip="97156", city="Grenoble"),
                Row(name="Gérard", age=55, zip="00000", city="Tourcoing"),
                Row(name="Gérard", age=55, zip="98888", city="Chambéry"),
                Row(name="Alice", age=25, zip="2345", city="Paris")
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name="Alice", age=25, zip="12345", city="Paris", departement="12"),
                Row(name="Michel", age=70, zip="20050", city="Lyon", departement="2A"),
                Row(name="Gérard", age=55, zip="20932", city="Marseille", departement="2B"),
                Row(name="Tristan", age=10, zip="97156", city="Grenoble", departement="971"),
                Row(name="Alice", age=25, zip="2345", city="Paris", departement="02")
            ]
        )

        actual = add_departement_column(input)

        self.assertEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.columns, expected.columns)

    # def test_keep_adult_with_unknown_col(self):
    #     input = spark.createDataFrame(
    #         [
    #             Row(unknown_name="Alice", age=25, zip="92929"),
    #             Row(unknown_name="Bob", age=17, zip="92929")
    #         ]
    #     )
    #     self.assertRaises(AnalysisException, filter_major_clients, input)

    def test_filter_major_clients(self):
        input_data = spark.createDataFrame(
            [
                Row(name="Alice", age=25, zip="92929"),
                Row(name="Bob", age=17, zip="92929"),
                Row(name="Charlie", age=30, zip="92929")
            ]
        )

        expected_data = spark.createDataFrame(
            [
                Row(name="Alice", age=25, zip="92929"),
                Row(name="Charlie", age=30, zip="92929")
            ]
        )

        result_df = filter_major_clients(input_data)

        self.assertEqual(result_df.collect(), expected_data.collect())
        self.assertEqual(result_df.columns, expected_data.columns)
