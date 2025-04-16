import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
from functions import validate_data, process_dataset

class TestDataProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestDataProcessing") \
            .getOrCreate()

        cls.order_items_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("order_id", IntegerType(), nullable=False),
            StructField("user_id", IntegerType(), nullable=False),
            StructField("days_since_prior_order", IntegerType(), nullable=True),
            StructField("product_id", IntegerType(), nullable=False),
            StructField("add_to_cart_order", IntegerType(), nullable=True),
            StructField("reordered", IntegerType(), nullable=True),
            StructField("order_timestamp", TimestampType(), nullable=False),
            StructField("date", DateType(), nullable=False)
        ])

        cls.orders_schema = StructType([
            StructField("order_num", IntegerType(), nullable=True),
            StructField("order_id", IntegerType(), nullable=False),
            StructField("user_id", IntegerType(), nullable=False),
            StructField("order_timestamp", TimestampType(), nullable=False),
            StructField("total_amount", DoubleType(), nullable=True),
            StructField("date", DateType(), nullable=False)
        ])

        cls.products_schema = StructType([
            StructField("product_id", IntegerType(), nullable=False),
            StructField("department_id", IntegerType(), nullable=True),
            StructField("department", StringType(), nullable=True),
            StructField("product_name", StringType(), nullable=False)
        ])

    def test_validate_data_order_items(self):
        data = [
            (1, 100, 1, None, 200, 1, 0, "2025-04-16 12:00:00", "2025-04-16"),
            (2, None, 1, None, 201, 2, 0, "2025-04-16 12:00:00", "2025-04-16"),
            (3, 101, 1, None, None, 3, 0, "2025-04-16 12:00:00", "2025-04-16")
        ]
        columns = ["id", "order_id", "user_id", "days_since_prior_order", "product_id", 
                   "add_to_cart_order", "reordered", "order_timestamp", "date"]
        df = self.spark.createDataFrame(data, columns)

        valid_records, invalid_records = validate_data(df, "order_items")

        self.assertEqual(valid_records.count(), 1)
        self.assertEqual(invalid_records.count(), 2)

    def test_process_dataset(self):
        data = [
            (1, 100, 1, "2025-04-16 12:00:00", 10.0, "2025-04-16"),
            (2, 101, 1, "2025-04-16 12:00:00", -5.0, "2025-04-16")
        ]
        columns = ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]
        df = self.spark.createDataFrame(data, columns)

        valid_data, rejected_data = process_dataset(df, self.orders_schema, "orders", "dummy_output_path")

        self.assertEqual(valid_data.count(), 1)
        self.assertEqual(rejected_data.count(), 1)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == "__main__":
    unittest.main()
