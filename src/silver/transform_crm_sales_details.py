from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import sys, os

def transform_crm_cust_info():
    print("Transform crm_sales_details to silver layer")

    spark = SparkSession.builder \
            .appName("Transform crm_sales_details to silver layer") \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Start transform crm_sales_details")
        
        batch_start_time = datetime.now()

        df = spark.table("bronze.crm_sales_details") \
            .filter(col("src_update_at") > (current_timestamp() - expr("INTERVAL 1 DAY")))
        
        out = df.select(
            col("sls_ord_num"),
            col("sls_prd_key"),
            col("sls_cust_id").cast("int"),
            when((col("sls_order_dt").isNull()) | (col("sls_order_dt") == "0") | (length(col("sls_order_dt").cast("string")) != 8), None)
                .otherwise(to_date(col("sls_order_dt").cast("string"), "yyyyMMdd")).alias("sls_order_dt"),
            when((col("sls_ship_dt").isNull()) | (col("sls_ship_dt") == "0") | (length(col("sls_ship_dt").cast("string")) != 8), None)
                .otherwise(to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd")).alias("sls_ship_dt"),
            when((col("sls_due_dt").isNull()) | (col("sls_due_dt") == "0") | (length(col("sls_due_dt").cast("string")) != 8), None)
                .otherwise(to_date(col("sls_due_dt").cast("string"), "yyyyMMdd")).alias("sls_due_dt"),
            when((col("sls_sales").isNull()) | (col("sls_sales") <= 0) | (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
                col("sls_quantity") * abs(col("sls_price")))
                .otherwise(col("sls_sales")).cast("int").alias("sls_sales"),
            col("sls_quantity").cast("int"),
            when((col("sls_price").isNull()) | (col("sls_price") <= 0),
                col("sls_sales") / when(col("sls_quantity") != 0, col("sls_quantity")).otherwise(1))
                .otherwise(col("sls_price")).cast("int").alias("sls_price"),
            current_timestamp().alias("dwh_create_date")
        )
        out.write.mode("overwrite").saveAsTable("silver.crm_sales_details")

        number_record = out.count()
        duration = (datetime.now() - batch_start_time).total_seconds()
        print(f"== Silver Layer Loaded {number_record} records in {duration:.0f} seconds")
    except Exception as e:
        print(f" ETL error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_crm_cust_info()