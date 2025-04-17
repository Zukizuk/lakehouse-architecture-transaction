import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
from functions import validate_data, process_dataset
from pyspark.sql import Row

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
        StructField("order_id", IntegerType(), nullable=True),  # Changed to True to accept None
        StructField("user_id", IntegerType(), nullable=False),
        StructField("days_since_prior_order", IntegerType(), nullable=True),
        StructField("product_id", IntegerType(), nullable=True),  # Changed to True to accept None
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

def test_validate_order_items_valid(spark):
    ts1 = datetime(2023, 1, 10, 10, 0, 0)
    d1 = date(2023, 1, 10)
    valid_oi_data = [
        Row(id=10001, order_id=501, user_id=1001, days_since_prior_order=5, product_id=101, add_to_cart_order=1, reordered=0, order_timestamp=ts1, date=d1),
        Row(id=10002, order_id=501, user_id=1001, days_since_prior_order=5, product_id=102, add_to_cart_order=2, reordered=0, order_timestamp=ts1, date=d1)
    ]
    input_df = spark.createDataFrame(valid_oi_data, schema=order_items_schema)
    mock_products_df = spark.createDataFrame([Row(product_id=101), Row(product_id=102)], schema=StructType([StructField("product_id", IntegerType())]))
    mock_orders_df = spark.createDataFrame([Row(order_id=501)], schema=StructType([StructField("order_id", IntegerType())]))
    reference_data = {"products": mock_products_df, "orders": mock_orders_df}
    valid_records, invalid_records = validate_data(input_df, "order_items", reference_data=reference_data)
    assert valid_records.count() == 2
    assert invalid_records.count() == 0

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