import os
import sys
import unittest
import tempfile
import shutil
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
from pyspark.sql.functions import col, lit
import datetime

# Add the directory containing your script to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the functions from your script
# Assuming your script is in a file named 'glue_delta_processor.py'
from glue_delta_processor import validate_data, process_dataset


class TestGlueDeltaProcessor(unittest.TestCase):
    """Test suite for AWS Glue Delta Lake processing script"""

    @classmethod
    def setUpClass(cls):
        """Set up SparkSession that will be used for all tests"""
        cls.spark = (SparkSession.builder
                    .appName("GlueScriptTest")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
                    # This enables Delta Lake in local mode for testing
                    .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
                    .master("local[*]")
                    .getOrCreate())
        
        # Define the schemas (copying from the main script)
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
        
        # Create a temporary directory for Delta tables
        cls.temp_dir = tempfile.mkdtemp()
        
        # Generate sample data for testing
        cls.sample_products = cls.spark.createDataFrame([
            (1, 100, "Produce", "Apples"),
            (2, 100, "Produce", "Bananas"),
            (3, 200, "Bakery", "Bread"),
            (4, 300, "Dairy", "Milk"),
            (5, None, None, "Unknown Product")  # Missing department info but valid
        ], ["product_id", "department_id", "department", "product_name"])
        
        cls.sample_orders = cls.spark.createDataFrame([
            (1, 1001, 501, datetime.datetime(2023, 1, 1, 10, 30), 25.99, datetime.date(2023, 1, 1)),
            (2, 1002, 502, datetime.datetime(2023, 1, 2, 11, 45), 32.50, datetime.date(2023, 1, 2)),
            (3, 1003, 503, datetime.datetime(2023, 1, 3, 9, 15), 18.75, datetime.date(2023, 1, 3)),
            (None, 1004, 504, datetime.datetime(2023, 1, 4, 14, 20), 45.00, datetime.date(2023, 1, 4))
        ], ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"])
        
        cls.sample_order_items = cls.spark.createDataFrame([
            (10001, 1001, 501, 3, 1, 1, 0, datetime.datetime(2023, 1, 1, 10, 30), datetime.date(2023, 1, 1)),
            (10002, 1001, 501, 3, 2, 2, 0, datetime.datetime(2023, 1, 1, 10, 30), datetime.date(2023, 1, 1)),
            (10003, 1002, 502, 7, 3, 1, 0, datetime.datetime(2023, 1, 2, 11, 45), datetime.date(2023, 1, 2)),
            (10004, 1003, 503, 5, 4, 1, 0, datetime.datetime(2023, 1, 3, 9, 15), datetime.date(2023, 1, 3)),
            (10005, 1005, 505, 10, 5, 1, 0, datetime.datetime(2023, 1, 5, 16, 10), datetime.date(2023, 1, 5))  # Invalid order_id
        ], ["id", "order_id", "user_id", "days_since_prior_order", "product_id", "add_to_cart_order", "reordered", 
            "order_timestamp", "date"])
        
        # Invalid records for testing validation
        cls.invalid_products = cls.spark.createDataFrame([
            (None, 400, "Electronics", "Headphones"),  # Missing product_id
            (6, 400, "Electronics", None)  # Missing product_name
        ], ["product_id", "department_id", "department", "product_name"])
        
        cls.invalid_orders = cls.spark.createDataFrame([
            (5, None, 505, datetime.datetime(2023, 1, 5, 16, 10), 52.25, datetime.date(2023, 1, 5)),  # Missing order_id
            (6, 1006, 506, None, 67.80, datetime.date(2023, 1, 6))  # Missing timestamp
        ], ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"])

    @classmethod
    def tearDownClass(cls):
        """Clean up resources after all tests"""
        cls.spark.stop()
        # Remove the temporary directory
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        """Set up before each test method"""
        # Create output paths for each test
        self.products_output = os.path.join(self.temp_dir, 'test_products')
        self.orders_output = os.path.join(self.temp_dir, 'test_orders')
        self.order_items_output = os.path.join(self.temp_dir, 'test_order_items')
        self.rejected_path = os.path.join(self.temp_dir, 'test_rejected')

    def test_validate_data_products(self):
        """Test validation of products data"""
        # Combine valid and invalid products for testing
        mixed_products = self.sample_products.union(self.invalid_products)
        
        # Validate the data
        valid_products, invalid_products = validate_data(mixed_products, "products")
        
        # Check counts
        self.assertEqual(valid_products.count(), 5)  # 5 valid products
        self.assertEqual(invalid_products.count(), 2)  # 2 invalid products
        
        # Check specific validation errors
        null_product_id = invalid_products.filter(col("product_id").isNull()).count()
        null_product_name = invalid_products.filter(col("product_name").isNull()).count()
        self.assertEqual(null_product_id, 1)
        self.assertEqual(null_product_name, 1)

    def test_validate_data_orders(self):
        """Test validation of orders data"""
        # Combine valid and invalid orders for testing
        mixed_orders = self.sample_orders.union(self.invalid_orders)
        
        # Validate the data
        valid_orders, invalid_orders = validate_data(mixed_orders, "orders")
        
        # Check counts
        self.assertEqual(valid_orders.count(), 4)  # 4 valid orders
        self.assertEqual(invalid_orders.count(), 2)  # 2 invalid orders
        
        # Check specific validation errors
        null_order_id = invalid_orders.filter(col("order_id").isNull()).count()
        null_timestamp = invalid_orders.filter(col("order_timestamp").isNull()).count()
        self.assertEqual(null_order_id, 1)
        self.assertEqual(null_timestamp, 1)

    def test_validate_data_order_items_with_references(self):
        """Test validation of order items with referential integrity"""
        # Create reference data
        reference_data = {
            "products": self.sample_products,
            "orders": self.sample_orders
        }
        
        # Validate order items with reference data
        valid_items, invalid_items = validate_data(self.sample_order_items, "order_items", reference_data)
        
        # One item should be invalid (order_id 1005 doesn't exist in orders)
        self.assertEqual(valid_items.count(), 4)
        self.assertEqual(invalid_items.count(), 1)
        
        # Check that the invalid item has order_id 1005
        invalid_order_id = invalid_items.first()["order_id"]
        self.assertEqual(invalid_order_id, 1005)

    def test_process_dataset_products(self):
        """Test processing and writing products to Delta table"""
        # Process the products dataset
        processed_products = process_dataset(
            self.sample_products, 
            self.products_schema, 
            "products", 
            self.products_output
        )
        
        # Check that processed data has the correct count
        self.assertEqual(processed_products.count(), 5)
        
        # Check that Delta table was created
        products_from_delta = self.spark.read.format("delta").load(self.products_output)
        self.assertEqual(products_from_delta.count(), 5)
        
        # Test that primary keys are distinct
        unique_product_ids = products_from_delta.select("product_id").distinct().count()
        self.assertEqual(unique_product_ids, 5)

    def test_process_dataset_orders_with_update(self):
        """Test processing orders with updates to existing Delta table"""
        # First process the original orders
        process_dataset(
            self.sample_orders, 
            self.orders_schema, 
            "orders", 
            self.orders_output
        )
        
        # Check initial count
        initial_orders = self.spark.read.format("delta").load(self.orders_output)
        self.assertEqual(initial_orders.count(), 4)
        
        # Create updated orders with one modified record
        updated_orders = self.spark.createDataFrame([
            (2, 1002, 502, datetime.datetime(2023, 1, 2, 11, 45), 40.00, datetime.date(2023, 1, 2)),  # Updated amount
            (5, 1005, 505, datetime.datetime(2023, 1, 5, 16, 10), 52.25, datetime.date(2023, 1, 5)),  # New record
        ], ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"])
        
        # Process the updated orders
        process_dataset(
            updated_orders, 
            self.orders_schema, 
            "orders", 
            self.orders_output
        )
        
        # Check final data
        final_orders = self.spark.read.format("delta").load(self.orders_output)
        self.assertEqual(final_orders.count(), 5)  # 4 original + 1 new
        
        # Check that the update worked correctly
        updated_amount = final_orders.filter(col("order_id") == 1002).first()["total_amount"]
        self.assertEqual(updated_amount, 40.00)

    def test_end_to_end_processing(self):
        """Test the full end-to-end processing flow"""
        # Process products first
        products_data = process_dataset(
            self.sample_products, 
            self.products_schema, 
            "products", 
            self.products_output
        )
        
        # Process orders next
        orders_data = process_dataset(
            self.sample_orders, 
            self.orders_schema, 
            "orders", 
            self.orders_output
        )
        
        # Create reference data for order items validation
        reference_data = {
            "products": products_data,
            "orders": orders_data
        }
        
        # Process order items with referential integrity checks
        order_items_data = process_dataset(
            self.sample_order_items, 
            self.order_items_schema, 
            "order_items", 
            self.order_items_output,
            reference_data
        )
        
        # Check that the correct number of records were processed
        self.assertEqual(products_data.count(), 5)
        self.assertEqual(orders_data.count(), 4)
        self.assertEqual(order_items_data.count(), 4)  # One should be rejected
        
        # Check rejected data was written
        rejected_path_full = f"{self.rejected_path}/order_items"
        try:
            rejected_items = self.spark.read.format("delta").load(rejected_path_full)
            self.assertEqual(rejected_items.count(), 1)
            rejected_order_id = rejected_items.first()["order_id"]
            self.assertEqual(rejected_order_id, 1005)
        except:
            self.fail("Failed to read rejected data from Delta table")


if __name__ == "__main__":
    unittest.main()


