import unittest
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo4.python_udf import categorize_category
from src.fr.hymaia.exo4.python_udf import categorize_category_udf


class TestCategorizeCategory(unittest.TestCase):

    def test_food_category(self):
        self.assertEqual(categorize_category(4), "food")

    def test_furniture_category(self):
        self.assertEqual(categorize_category(7), "furniture")


class TestCategorizeCategoryUDF(unittest.TestCase):

    def test_udf(self):
        df = spark.createDataFrame([(1,), (7,), (10,)], ["category"])
        df_result = df.withColumn("category_type", categorize_category_udf(df["category"])).collect()

        expected_result = [("1", "food"), ("7", "furniture"), ("10", "furniture")]
        for row, expected in zip(df_result, expected_result):
            self.assertEqual((str(row["category"]), row["category_type"]), expected)