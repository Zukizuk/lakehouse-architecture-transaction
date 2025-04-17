import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
from functions import validate_data, process_dataset

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestDataProcessing") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def order_items_schema():
    return StructType([
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

@pytest.fixture(scope="module")
def orders_schema():
    return StructType([
        StructField("order_num", IntegerType(), nullable=True),
        StructField("order_id", IntegerType(), nullable=False),
        StructField("user_id", IntegerType(), nullable=False),
        StructField("order_timestamp", TimestampType(), nullable=False),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("date", DateType(), nullable=False)
    ])

@pytest.fixture(scope="module")
def products_schema():
    return StructType([
        StructField("product_id", IntegerType(), nullable=False),
        StructField("department_id", IntegerType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=False)
    ])

def test_validate_data_order_items(spark, order_items_schema):
    # Convert string timestamps and dates to proper Python types
    timestamp = datetime.strptime("2025-04-16 12:00:00", "%Y-%m-%d %H:%M:%S")
    dt = date(2025, 4, 16)
    
    data = [
        (1, 100, 1, None, 200, 1, 0, timestamp, dt),
        (2, None, 1, None, 201, 2, 0, timestamp, dt),
        (3, 101, 1, None, None, 3, 0, timestamp, dt)
    ]
    
    df = spark.createDataFrame(data, schema=order_items_schema)
    
    valid_records, invalid_records = validate_data(df, "order_items")
    
    assert valid_records.count() == 1
    assert invalid_records.count() == 2

def test_process_dataset(spark, orders_schema):
    # Convert string timestamps and dates to proper Python types
    timestamp = datetime.strptime("2025-04-16 12:00:00", "%Y-%m-%d %H:%M:%S")
    dt = date(2025, 4, 16)
    
    data = [
        (1, 100, 1, timestamp, 10.0, dt),
        (2, 101, 1, timestamp, -5.0, dt)
    ]
    
    df = spark.createDataFrame(data, schema=orders_schema)
    
    valid_data, rejected_data = process_dataset(df, orders_schema, "orders", "dummy_output_path")
    
    assert valid_data.count() == 1
    assert rejected_data.count() == 1