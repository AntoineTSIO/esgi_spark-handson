from pyspark import Row
from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.agregate.aggregate_functions import departement_count


class TestMain(unittest.TestCase):

    def test_add_departement_count(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Alice", age=25, zip="12345", city="Paris", departement="12"),
                Row(name="Michel", age=70, zip="12345", city="Lyon", departement="12"),
                Row(name="Gérard", age=55, zip="20932", city="Marseille", departement="2B")
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(departement="12", nb_people=2),
                Row(departement="2B", nb_people=1),
            ]
        )

        actual = departement_count(input)

        self.assertEqual(actual.collect(), expected.collect())
