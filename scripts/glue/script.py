import sys
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
from pyspark.conf import SparkConf

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if level == "INFO":
        logger.info(f"[{timestamp}] {message}")
    elif level == "ERROR":
        logger.error(f"[{timestamp}] {message}")
    else:
        logger.info(f"[{timestamp}] {message}")

# Initialize Spark and Glue contexts with Delta Lake configuration
log("Initializing Spark and Glue contexts")

conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Job
job = Job(glueContext)

# Resolve input arguments
log("Resolving job arguments")
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'S3_BUCKET_PATH', 
    'ORDER_ITEMS_OUTPUT_PATH', 
    'ORDERS_OUTPUT_PATH', 
    'PRODUCTS_OUTPUT_PATH',
    'REJECTED_PATH'
])

s3_bucket_path = args['S3_BUCKET_PATH']
order_items_output = args['ORDER_ITEMS_OUTPUT_PATH']
orders_output = args['ORDERS_OUTPUT_PATH']
products_output = args['PRODUCTS_OUTPUT_PATH']
rejected_path = args['REJECTED_PATH']
job_name = args['JOB_NAME']

job.init(job_name, args)
log(f"Starting job: {job_name}")

# Define schemas for the datasets
log("Defining data schemas")
order_items_schema = StructType([
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

orders_schema = StructType([
    StructField("order_num", IntegerType(), nullable=True),
    StructField("order_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("order_timestamp", TimestampType(), nullable=False),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("date", DateType(), nullable=False)
])

products_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=True),
    StructField("department", StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=False)
])

# Function to validate data and separate valid from invalid records
def validate_data(df, schema_name, reference_data=None):
    log(f"Validating {schema_name} dataset")
    
    # Add validation error column
    validated = df.withColumn("validation_errors", lit(None).cast(StringType()))
    
    # Apply validation rules based on schema
    if schema_name == "order_items":
        # Core validations
        validated = validated.withColumn(
            "validation_errors",
            when(col("id").isNull(), "Null primary identifier")
            .when(col("order_id").isNull(), "Null order_id")
            .when(col("product_id").isNull(), "Null product_id")
            .when(col("order_timestamp").isNull(), "Invalid timestamp")
            .otherwise(col("validation_errors"))
        )
        
        # Referential integrity checks if reference data provided
        if reference_data and "orders" in reference_data:
            order_ids = reference_data["orders"].select("order_id").distinct()
            order_ids_list = order_ids.rdd.flatMap(lambda x: x).collect()
            
            validated = validated.withColumn(
                "validation_errors",
                when(~col("order_id").isin(order_ids_list) & col("validation_errors").isNull(), 
                     "Invalid order_id reference")
                .otherwise(col("validation_errors"))
            )
        
        if reference_data and "products" in reference_data:
            product_ids = reference_data["products"].select("product_id").distinct()
            product_ids_list = product_ids.rdd.flatMap(lambda x: x).collect()
            
            validated = validated.withColumn(
                "validation_errors",
                when(~col("product_id").isin(product_ids_list) & col("validation_errors").isNull(), 
                     "Invalid product_id reference")
                .otherwise(col("validation_errors"))
            )
    
    elif schema_name == "orders":
        validated = validated.withColumn(
            "validation_errors",
            when(col("order_id").isNull(), "Null order_id primary key")
            .when(col("order_timestamp").isNull(), "Invalid timestamp")
            .when((col("total_amount").isNotNull()) & (col("total_amount") <= 0), "Non-positive total amount")
            .otherwise(col("validation_errors"))
        )
    
    elif schema_name == "products":
        validated = validated.withColumn(
            "validation_errors",
            when(col("product_id").isNull(), "Null product_id primary key")
            .when(col("product_name").isNull(), "Null product name")
            .otherwise(col("validation_errors"))
        )
    
    # Split into valid and invalid records
    valid_records = validated.filter(col("validation_errors").isNull()).drop("validation_errors")
    invalid_records = validated.filter(col("validation_errors").isNotNull())
    
    # Log validation results
    valid_count = valid_records.count()
    invalid_count = invalid_records.count()
    total_count = valid_count + invalid_count
    log(f"Validation results for {schema_name}: {valid_count}/{total_count} valid records ({(valid_count/total_count)*100:.2f}%)")
    
    return valid_records, invalid_records

# Function to process and write a dataset to Delta Lake
def process_dataset(raw_df, schema, schema_name, output_path, reference_data=None):
    try:
        log(f"Processing {schema_name} dataset")
        
        # Cast data to match schema
        typed_df = raw_df
        for field in schema.fields:
            typed_df = typed_df.withColumn(field.name, col(field.name).cast(field.dataType))
        
        # Apply validation rules
        valid_data, rejected_data = validate_data(typed_df, schema_name, reference_data)
        
        # Log and write rejected records if any
        rejected_count = rejected_data.count()
        if rejected_count > 0:
            log(f"Found {rejected_count} rejected {schema_name} records")
            
            # Enhance rejected records with metadata
            rejected_data = rejected_data.withColumn("rejection_time", current_timestamp())
            rejected_data = rejected_data.withColumn("source", lit(schema_name))
            
            # Write rejected records to Delta Lake
            rejected_path_full = f"{rejected_path}/{schema_name}"
            
            # Write with partitioning by date if available
            if "date" in rejected_data.columns:
                rejected_data.write.format("delta").mode("append").partitionBy("date").save(rejected_path_full)
            else:
                rejected_data.write.format("delta").mode("append").save(rejected_path_full)
        
        # Deduplicate valid data based on primary key
        primary_key = "id" if schema_name == "order_items" else "order_id" if schema_name == "orders" else "product_id"
        deduplicated_data = valid_data.dropDuplicates([primary_key])
        
        # Determine partition column
        partition_col = "date" if schema_name in ["order_items", "orders"] else "department"
        
        # Write to Delta Lake (initial load or upsert)
        try:
            # Check if Delta table exists
            delta_table = DeltaTable.forPath(spark, output_path)
            
            # Determine merge condition based on primary key
            merge_condition = f"existing.{primary_key} = updates.{primary_key}"
            
            # Merge/upsert logic
            log(f"Performing Delta merge operation for {schema_name}")
            delta_table.alias("existing").merge(
                deduplicated_data.alias("updates"),
                merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            
            log(f"Successfully merged {schema_name} records into the Delta table")
        except Exception as e:
            # If Delta table does not exist, create it
            log(f"Delta table for {schema_name} does not exist, creating at {output_path}")
            
            # Write with partitioning if partition column exists
            if partition_col in deduplicated_data.columns:
                deduplicated_data.write.format("delta").mode("overwrite").partitionBy(partition_col).save(output_path)
            else:
                deduplicated_data.write.format("delta").mode("overwrite").save(output_path)
                
            log(f"Successfully wrote {schema_name} records to new Delta table")
        
        return deduplicated_data
        
    except Exception as e:
        log(f"Error processing {schema_name} dataset: {str(e)}", "ERROR")
        raise e

try:
    # Read products first as it's referenced by order_items
    log("Reading products data")
    products_path = f"{s3_bucket_path}/products.csv"
    products_raw = spark.read.format("csv").option("header", "true").load(products_path)
    products_data = process_dataset(products_raw, products_schema, "products", products_output)
    
    # Read orders next as it's referenced by order_items
    log("Reading orders data")
    orders_path = f"{s3_bucket_path}/orders.csv"
    orders_raw = spark.read.format("csv").option("header", "true").load(orders_path)
    orders_data = process_dataset(orders_raw, orders_schema, "orders", orders_output)
    
    # Read order_items last and validate against products and orders
    log("Reading order_items data")
    order_items_path = f"{s3_bucket_path}/order_items.csv"
    order_items_raw = spark.read.format("csv").option("header", "true").load(order_items_path)
    
    # Create reference data dictionary for validation
    reference_data = {
        "products": products_data,
        "orders": orders_data
    }
    
    # Process order_items with referential integrity checks
    order_items_data = process_dataset(order_items_raw, order_items_schema, "order_items", order_items_output, reference_data)
    
    log("All datasets processed successfully!")

except Exception as e:
    log(f"Error in main processing flow: {str(e)}", "ERROR")
    raise e
finally:
    # Commit the Glue job
    job.commit()