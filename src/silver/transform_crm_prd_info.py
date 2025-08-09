from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import sys, os

def transform_crm_prd_info():
    print("Transform crm_prd_info to silver layer")

    spark = SparkSession.builder \
            .appName("Transform crm_prd_info to silver layer") \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Start transform crm_prd_info")
        
        batch_start_time = datetime.now()

        # Read data from bronze layer, filtering for records updated in the last day
        df = spark.table("bronze.crm_prd_info")
        
        # Define window specification for partitioning by product key and ordering by start date
        w = Window.partitionBy("prd_key").orderBy("prd_start_dt")
        
        # Transform columns and apply business logic for silver layer
        out = df.withColumn("original_prd_key", col("prd_key")) \
            .select(
            # Cast product ID to integer
            col("prd_id").cast("int"),

            # Extract and format category ID from product key
            regexp_replace(substring(col("original_prd_key"), 1, 5), "-", "_").alias("cat_id"),

            # Extract product key, handling cases where length > 6
            when(length(col("original_prd_key")) > 6, substring(col("original_prd_key"), 7, 100))
                .otherwise(col("original_prd_key")).alias("prd_key"),

            col("prd_nm"),

            # Ensure product cost is not null and cast to decimal
            coalesce(col("prd_cost"), lit(0)).cast("decimal(10,2)").alias("prd_cost"),

            # Map product line codes to descriptive names
            when(upper(trim(col("prd_line"))) == "M", "Mountain")
                .when(upper(trim(col("prd_line"))) == "R", "Road")
                .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
                .when(upper(trim(col("prd_line"))) == "T", "Touring")
                .otherwise("n/a").alias("prd_line"),

            # Cast product start date to date type
            col("prd_start_dt").cast("date"),

            # # Calculate product end date as one day before the next start date in the window
             (lead(col("prd_start_dt")).over(w) - expr("INTERVAL 1 DAY")).cast("date").alias("prd_end_dt"),
            # col("prd_end_dt").cast("date"),
            # Add ETL load timestamp
            current_timestamp().alias("dwh_create_date")
            ).dropDuplicates(["prd_id"])
        
        out.write.mode("overwrite").saveAsTable("silver.crm_prd_info")

        number_record = out.count()
        duration = (datetime.now() - batch_start_time).total_seconds()
        print(f"== Silver Layer Loaded {number_record} records in {duration:.0f} seconds")
    except Exception as e:
        print(f" ETL error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_crm_prd_info()