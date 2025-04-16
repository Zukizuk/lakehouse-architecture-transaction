import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when
from pyspark.sql.types import StructType, StringType

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log(message, level="INFO"):
    logger.info(message) if level == "INFO" else logger.error(message)

def validate_data(df: DataFrame, schema_name: str, reference_data=None):
    log(f"Validating {schema_name} dataset")
    
    validated = df.withColumn("validation_errors", lit(None).cast(StringType()))
    
    if schema_name == "order_items":
        validated = validated.withColumn(
            "validation_errors",
            when(col("id").isNull(), "Null primary identifier")
            .when(col("order_id").isNull(), "Null order_id")
            .when(col("product_id").isNull(), "Null product_id")
            .when(col("order_timestamp").isNull(), "Invalid timestamp")
            .otherwise(col("validation_errors"))
        )
        
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
    
    valid_records = validated.filter(col("validation_errors").isNull()).drop("validation_errors")
    invalid_records = validated.filter(col("validation_errors").isNotNull())
    
    valid_count = valid_records.count()
    invalid_count = invalid_records.count()
    total_count = valid_count + invalid_count
    log(f"Validation results for {schema_name}: {valid_count}/{total_count} valid records ({(valid_count/total_count)*100:.2f}%)")
    
    return valid_records, invalid_records

def process_dataset(raw_df: DataFrame, schema: StructType, schema_name: str, output_path: str, reference_data=None):
    try:
        log(f"Processing {schema_name} dataset")
        
        typed_df = raw_df
        for field in schema.fields:
            typed_df = typed_df.withColumn(field.name, col(field.name).cast(field.dataType))
        
        valid_data, rejected_data = validate_data(typed_df, schema_name, reference_data)
        
        rejected_count = rejected_data.count()
        if rejected_count > 0:
            log(f"Found {rejected_count} rejected {schema_name} records")
            rejected_data = rejected_data.withColumn("rejection_time", current_timestamp())
            rejected_data = rejected_data.withColumn("source", lit(schema_name))
            # Here you would write rejected_data to Delta Lake or another storage
            # For testing purposes, we can just return it
            return valid_data, rejected_data
        
        primary_key = "id" if schema_name == "order_items" else "order_id" if schema_name == "orders" else "product_id"
        deduplicated_data = valid_data.dropDuplicates([primary_key])
        
        # Here you would write deduplicated_data to Delta Lake or another storage
        # For testing purposes, we can just return it
        return deduplicated_data
        
    except Exception as e:
        log(f"Error processing {schema_name} dataset: {str(e)}", "ERROR")
        raise e

