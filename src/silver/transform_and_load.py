from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import sys, os

def transform_to_silver():
    print("Transform data from Bronze to Silver Layer")

    spark = SparkSession.builder \
            .appName("Bronze to Silver ETL") \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.sql("SHOW DATABASES")
        spark.sql("SHOW TABLES IN bronze")
        batch_start_time = datetime.now()

        transform_crm_cust_info(spark)
        transform_crm_prd_info(spark)
        transform_crm_sales_details(spark)
        transform_erp_cust_az12(spark)
        transform_erp_loc_a101(spark)
        transform_erp_px_cat_g1v2(spark)

        duration = (datetime.now() - batch_start_time).total_seconds()
        print(f"== Silver Layer Loaded in {duration:.0f} seconds")
    except Exception as e:
        print(f" ETL error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

def transform_crm_cust_info(spark):
    df = spark.table("bronze.crm_cust_info")
    w = Window.partitionBy("cst_id").orderBy(desc("cst_create_date"))
    out = df.filter(col("cst_id").isNotNull()) \
        .withColumn("flag_last", row_number().over(w)) \
        .filter(col("flag_last") == 1) \
        .select(
            col("cst_id").cast("int"),
            col("cst_key"),
            trim(col("cst_firstname")).alias("cst_firstname"),
            trim(col("cst_lastname")).alias("cst_lastname"),
            when(upper(trim(col("cst_marital_status"))) == "S", "Single")
                .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
                .otherwise("n/a").alias("cst_marital_status"),
            when(upper(trim(col("cst_gndr"))) == "F", "Female")
                .when(upper(trim(col("cst_gndr"))) == "M", "Male")
                .otherwise("n/a").alias("cst_gndr"),
            col("cst_create_date").cast("date"),
            current_timestamp().alias("dwh_create_date")
        )
    out.write.mode("overwrite").saveAsTable("silver.crm_cust_info")

def transform_crm_prd_info(spark):
    df = spark.table("bronze.crm_prd_info")
    w = Window.partitionBy("prd_key").orderBy("prd_start_dt")
    out = df.withColumn("original_prd_key", col("prd_key")) \
        .select(
            col("prd_id").cast("int"),
            regexp_replace(substring(col("original_prd_key"), 1, 5), "-", "_").alias("cat_id"),
            when(length(col("original_prd_key")) > 6, substring(col("original_prd_key"), 7, 100))
                .otherwise(col("original_prd_key")).alias("prd_key"),
            col("prd_nm"),
            coalesce(col("prd_cost"), lit(0)).cast("decimal(10,2)").alias("prd_cost"),
            when(upper(trim(col("prd_line"))) == "M", "Mountain")
                .when(upper(trim(col("prd_line"))) == "R", "Road")
                .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
                .when(upper(trim(col("prd_line"))) == "T", "Touring")
                .otherwise("n/a").alias("prd_line"),
            col("prd_start_dt").cast("date"),
            (lead(col("prd_start_dt")).over(w) - expr("INTERVAL 1 DAY")).cast("date").alias("prd_end_dt"),
            current_timestamp().alias("dwh_create_date")
        )
    out.write.mode("overwrite").saveAsTable("silver.crm_prd_info")

def transform_crm_sales_details(spark):
    df = spark.table("bronze.crm_sales_details")
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

def transform_erp_cust_az12(spark):
    df = spark.table("bronze.erp_cust_az12")
    out = df.select(
        when(col("cid").startswith("NAS"), substring(col("cid"), 4, 100)).otherwise(col("cid")).alias("cid"),
        when(col("bdate") > current_date(), None).otherwise(col("bdate")).cast("date").alias("bdate"),
        when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
            .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
            .otherwise("n/a").alias("gen"),
        current_timestamp().alias("dwh_create_date")
    )
    out.write.mode("overwrite").saveAsTable("silver.erp_cust_az12")

def transform_erp_loc_a101(spark):
    df = spark.table("bronze.erp_loc_a101")
    out = df.select(
        regexp_replace(col("cid"), "-", "").alias("cid"),
        when(trim(col("cntry")) == "DE", "Germany")
            .when(trim(col("cntry")).isin("US", "USA"), "United States")
            .when((trim(col("cntry")) == "") | col("cntry").isNull(), "n/a")
            .otherwise(trim(col("cntry"))).alias("cntry"),
        current_timestamp().alias("dwh_create_date")
    )
    out.write.mode("overwrite").saveAsTable("silver.erp_loc_a101")

def transform_erp_px_cat_g1v2(spark):
    df = spark.table("bronze.erp_px_cat_g1v2")
    out = df.select(
        col("id"), col("cat"), col("subcat"), col("maintenance"),
        current_timestamp().alias("dwh_create_date")
    )
    out.write.mode("overwrite").saveAsTable("silver.erp_px_cat_g1v2")

if __name__ == "__main__":
    transform_to_silver()
