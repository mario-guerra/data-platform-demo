#!/usr/bin/env python3
"""
PySpark Structured Streaming job for processing Kafka orders into Delta Lake
Reads from Kafka -> Bronze Delta -> Silver Delta with aggregations
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as spark_sum, count, avg, max as spark_max,
    current_timestamp, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta import configure_spark_with_delta_pip


def create_spark_session():
    """Create Spark session with Delta Lake configuration"""
    builder = SparkSession.builder \
        .appName("KafkaToDeltas") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_order_schema():
    """Define the schema for order events"""
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("region", StringType(), True)
    ])


def write_to_bronze(df, checkpoint_location):
    """Write raw Kafka data to Bronze Delta table"""
    bronze_path = "s3a://delta/bronze/orders"
    
    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_location}/bronze") \
        .option("path", bronze_path) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query


def process_to_silver(spark, checkpoint_location):
    """Process Bronze data to Silver with aggregations"""
    bronze_path = "s3a://delta/bronze/orders"
    silver_path = "s3a://delta/silver/orders_summary"
    
    # Read from bronze as a stream
    bronze_df = spark.readStream \
        .format("delta") \
        .load(bronze_path)
    
    # Parse the order data and add watermark
    parsed_df = bronze_df.select(
        from_json(col("value"), get_order_schema()).alias("order"),
        col("timestamp").alias("kafka_timestamp")
    ).select("order.*", "kafka_timestamp") \
     .withColumn("event_time", to_timestamp(col("timestamp"))) \
     .withWatermark("event_time", "10 minutes")
    
    # Aggregate by 5-minute windows
    windowed_df = parsed_df \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("region"),
            col("currency"),
            col("product")
        ) \
        .agg(
            count("*").alias("order_count"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum(col("quantity") * col("price")).alias("total_revenue"),
            avg("price").alias("avg_price"),
            spark_max("price").alias("max_price")
        ) \
        .withColumn("processing_time", current_timestamp())
    
    # Write to silver
    query = windowed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_location}/silver") \
        .option("path", silver_path) \
        .trigger(processingTime="60 seconds") \
        .start()
    
    return query


def main():
    """Main streaming application"""
    # Get configuration
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    checkpoint_location = "s3a://delta/checkpoints"
    
    print(f"Starting streaming job with Kafka servers: {kafka_bootstrap_servers}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read from Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", "orders") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Select key and value as strings
        kafka_df = kafka_df.selectExpr(
            "CAST(key AS STRING)",
            "CAST(value AS STRING)",
            "timestamp",
            "partition",
            "offset"
        )
        
        # Start bronze ingestion
        bronze_query = write_to_bronze(kafka_df, checkpoint_location)
        
        # Start silver processing (with a small delay to ensure bronze table exists)
        import time
        time.sleep(10)
        silver_query = process_to_silver(spark, checkpoint_location)
        
        print("Streaming queries started successfully!")
        print("Bronze Delta path: s3a://delta/bronze/orders")
        print("Silver Delta path: s3a://delta/silver/orders_summary")
        print("Press Ctrl+C to stop...")
        
        # Wait for termination
        bronze_query.awaitTermination()
        silver_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("Stopping streaming application...")
    except Exception as e:
        print(f"Error in streaming application: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()