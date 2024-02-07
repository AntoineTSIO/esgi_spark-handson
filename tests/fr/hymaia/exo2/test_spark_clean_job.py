from pyspark import Row
from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.clean.clean_functions import join_dataframes, add_departement_column


class TestMain(unittest.TestCase):
    def test_join_dataframes(self):
        # GIVEN
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
        expected = spark.createDataFrame(
            [
                Row(name="Alice", age=25, zip="12345", city="Paris"),
                Row(name="Bob", age=30, zip="67890", city="New York")
            ]
        )

        actual = join_dataframes(input1, input2)

        self.assertEqual(actual.collect(), expected.collect())

    def test_add_departement_column(self):
        # GIVEN
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
