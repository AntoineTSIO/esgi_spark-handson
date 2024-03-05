import unittest
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo4.scala_udf import addCategoryName


class TestCategorizeCategory(unittest.TestCase):

    def test_food_category(self):
        self.assertEqual(addCategoryName(4), "food")

    def test_furniture_category(self):
        self.assertEqual(addCategoryName(7), "furniture")

