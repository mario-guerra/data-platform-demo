package etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables.DeltaTable

/**
 * Batch ETL job to process Bronze data into Gold Delta tables
 * Reads from Bronze orders and creates daily currency statistics
 */
object BatchETL extends App {

  // Create Spark session with Delta configuration
  val spark = SparkSession.builder()
    .appName("BatchETL-BronzeToGold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  try {
    println("Starting Batch ETL: Bronze to Gold processing...")
    
    val bronzePath = "s3a://delta/bronze/orders"
    val goldPath = "s3a://delta/gold/daily_currency_stats"
    
    // Check if bronze table exists
    if (!DeltaTable.isDeltaTable(spark, bronzePath)) {
      println(s"Warning: Bronze table does not exist at $bronzePath")
      println("Please run the streaming job first to populate bronze data")
      System.exit(1)
    }
    
    // Read from bronze Delta table
    val bronzeDF = spark.read.format("delta").load(bronzePath)
    
    println(s"Bronze table record count: ${bronzeDF.count()}")
    
    // Parse the JSON value column to extract order data
    val orderSchema = StructType(Array(
      StructField("order_id", StringType, true),
      StructField("customer_id", StringType, true),
      StructField("product", StringType, true),
      StructField("quantity", IntegerType, true),
      StructField("price", DoubleType, true),
      StructField("currency", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("region", StringType, true)
    ))
    
    val parsedDF = bronzeDF
      .select(from_json(col("value"), orderSchema).as("order"))
      .select("order.*")
      .withColumn("event_date", to_date(col("timestamp")))
      .withColumn("revenue", col("quantity") * col("price"))
    
    // Create daily currency statistics
    val dailyCurrencyStats = parsedDF
      .groupBy("event_date", "currency", "region")
      .agg(
        count("*").as("total_orders"),
        sum("quantity").as("total_quantity"),
        sum("revenue").as("total_revenue"),
        avg("price").as("avg_price"),
        max("price").as("max_price"),
        min("price").as("min_price"),
        countDistinct("customer_id").as("unique_customers"),
        countDistinct("product").as("unique_products")
      )
      .withColumn("avg_order_value", col("total_revenue") / col("total_orders"))
      .withColumn("processing_timestamp", current_timestamp())
      .orderBy("event_date", "currency", "region")
    
    println("Daily currency statistics computed. Sample data:")
    dailyCurrencyStats.show(20, truncate = false)
    
    // Write to Gold Delta table using merge (upsert) logic
    if (DeltaTable.isDeltaTable(spark, goldPath)) {
      println("Gold table exists, performing upsert...")
      
      val goldTable = DeltaTable.forPath(spark, goldPath)
      
      goldTable.alias("gold")
        .merge(
          dailyCurrencyStats.alias("updates"),
          "gold.event_date = updates.event_date AND gold.currency = updates.currency AND gold.region = updates.region"
        )
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
        
    } else {
      println("Creating new Gold table...")
      dailyCurrencyStats
        .write
        .format("delta")
        .mode("overwrite")
        .option("path", goldPath)
        .save()
    }
    
    // Show final statistics
    val goldDF = spark.read.format("delta").load(goldPath)
    println(s"Gold table record count: ${goldDF.count()}")
    
    println("Batch ETL completed successfully!")
    
    // Optional: Show some insights
    println("\nTop revenue by currency (last 7 days):")
    goldDF
      .filter(col("event_date") >= date_sub(current_date(), 7))
      .groupBy("currency")
      .agg(sum("total_revenue").as("weekly_revenue"))
      .orderBy(desc("weekly_revenue"))
      .show()
    
  } catch {
    case e: Exception =>
      println(s"Error in Batch ETL: ${e.getMessage}")
      e.printStackTrace()
      System.exit(1)
  } finally {
    spark.stop()
  }
}