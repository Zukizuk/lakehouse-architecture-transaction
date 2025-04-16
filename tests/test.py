import pytest
import os
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
from pyspark.sql.functions import col
import datetime

# Import the functions to test
# You'll need to update this import to match your script structure
from scripts.glue.script import validate_data, process_dataset

class TestDeltaProcessor:
    @classmethod
    def setup_class(cls):
        """Initialize Spark session and sample data once for all tests"""
        # Create a local Spark session with Delta Lake support
        cls.spark = (SparkSession.builder
                    .appName("PytestDeltaTests")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    # Using local mode for testing
                    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
                    .master("local[*]")
                    .getOrCreate())
        
        # Define schemas
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
        
        # Create test data
        cls.valid_products = cls.spark.createDataFrame([
            (1, 100, "Produce", "Apples"),
            (2, 200, "Bakery", "Bread")
        ], ["product_id", "department_id", "department", "product_name"])
        
        cls.invalid_products = cls.spark.createDataFrame([
            (None, 300, "Dairy", "Milk"),  # Missing product_id
            (3, 400, "Electronics", None)  # Missing product_name
        ], ["product_id", "department_id", "department", "product_name"])
        
        cls.valid_orders = cls.spark.createDataFrame([
            (1, 1001, 501, datetime.datetime(2023, 1, 1, 10, 30), 25.99, datetime.date(2023, 1, 1))
        ], ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"])
        
        # Create temporary directory for Delta tables
        cls.temp_dir = tempfile.mkdtemp()
    
    @classmethod
    def teardown_class(cls):
        """Clean up resources after tests"""
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir)
    
    def setup_method(self):
        """Set up before each test method"""
        # Create fresh output paths for each test
        self.products_output = os.path.join(self.temp_dir, 'test_products')
        self.rejected_path = os.path.join(self.temp_dir, 'rejected')
    
    def test_validate_data_valid_products(self):
        """Test validation with valid products data"""
        valid_data, invalid_data = validate_data(self.valid_products, "products")
        
        assert valid_data.count() == 2
        assert invalid_data.count() == 0
    
    def test_validate_data_invalid_products(self):
        """Test validation with invalid products data"""
        valid_data, invalid_data = validate_data(self.invalid_products, "products")
        
        assert valid_data.count() == 0
        assert invalid_data.count() == 2
        
        # Check specific validation errors
        errors = [row["validation_errors"] for row in invalid_data.collect()]
        assert "Null product_id primary key" in errors
        assert "Null product name" in errors
    
    def test_validate_data_mixed_products(self):
        """Test validation with mix of valid and invalid products"""
        mixed_products = self.valid_products.union(self.invalid_products)
        valid_data, invalid_data = validate_data(mixed_products, "products")
        
        assert valid_data.count() == 2
        assert invalid_data.count() == 2
    
    def test_process_dataset_new_delta_table(self):
        """Test processing a dataset and creating a new Delta table"""
        processed_data = process_dataset(
            self.valid_products,
            self.products_schema,
            "products",
            self.products_output
        )
        
        # Verify processed data
        assert processed_data.count() == 2
        
        # Verify Delta table was created
        delta_data = self.spark.read.format("delta").load(self.products_output)
        assert delta_data.count() == 2
    
    def test_process_dataset_with_update(self):
        """Test updating an existing Delta table"""
        # First create the initial table
        process_dataset(
            self.valid_products,
            self.products_schema,
            "products",
            self.products_output
        )
        
        # Create an update with one changed record and one new record
        updated_products = self.spark.createDataFrame([
            (1, 100, "Produce", "Green Apples"),  # Updated name
            (3, 300, "Dairy", "Cheese")  # New record
        ], ["product_id", "department_id", "department", "product_name"])
        
        # Process the update
        process_dataset(
            updated_products,
            self.products_schema,
            "products",
            self.products_output
        )
        
        # Read the final state of the Delta table
        final_data = self.spark.read.format("delta").load(self.products_output)
        
        # Check counts
        assert final_data.count() == 3  # 1 unchanged + 1 updated + 1 new
        
        # Check the update worked
        updated_name = final_data.filter(col("product_id") == 1).first()["product_name"]
        assert updated_name == "Green Apples"
        
        # Check the new record was added
        new_records = final_data.filter(col("product_id") == 3).count()
        assert new_records == 1
    
    def test_referential_integrity_validation(self):
        """Test validation with referential integrity checks"""
        # Create order items with one valid and one invalid reference
        order_items = self.spark.createDataFrame([
            (101, 1001, 501, 3, 1, 1, 0, datetime.datetime(2023, 1, 1, 10, 30), datetime.date(2023, 1, 1)),  # Valid
            (102, 9999, 502, 3, 1, 2, 0, datetime.datetime(2023, 1, 1, 10, 30), datetime.date(2023, 1, 1))   # Invalid order_id
        ], ["id", "order_id", "user_id", "days_since_prior_order", "product_id", "add_to_cart_order", 
            "reordered", "order_timestamp", "date"])
        
        # Create reference data
        reference_data = {
            "orders": self.valid_orders,
            "products": self.valid_products
        }
        
        # Validate with reference data
        valid_items, invalid_items = validate_data(order_items, "order_items", reference_data)
        
        # Check results
        assert valid_items.count() == 1
        assert invalid_items.count() == 1
        
        # Verify the invalid record has the expected order_id
        invalid_order_id = invalid_items.first()["order_id"]
        assert invalid_order_id == 9999