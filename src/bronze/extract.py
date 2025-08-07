from pyspark.sql import SparkSession
from datetime import datetime
import sys

def extract():
    print("== Starting PostgreSQL to Bronze Layer... ==")

    try:
        spark = SparkSession.builder \
            .appName("Extract Postgres DB to Bronze Layer") \
            .enableHiveSupport() \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("spark.jars", "/usr/local/spark/jars/postgresql-42.7.7.jar") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        postgres_url = "jdbc:postgresql://localhost:5432/crm_erp"
        props = {
            "user": "postgres",
            "password": "6666",
            "driver": "org.postgresql.Driver"
        }

        table_mappings = [
            ("crm_cust_info", "bronze.crm_cust_info"),
            ("crm_prd_info", "bronze.crm_prd_info"),
            ("crm_sales_details", "bronze.crm_sales_details"),
            ("erp_loc_a101", "bronze.erp_loc_a101"),
            ("erp_cust_az12", "bronze.erp_cust_az12"),
            ("erp_px_cat_g1v2", "bronze.erp_px_cat_g1v2")
        ]

        start_time = datetime.now()
        total_rows, success_count, error_count = 0, 0, 0

        for src_table, tgt_table in table_mappings:
            print(f"\n== Loading {src_table} → {tgt_table} ...")
            try:
                df = spark.read.jdbc(postgres_url, src_table, properties=props)
                row_count = df.count()
                df.write.mode("overwrite").saveAsTable(tgt_table)
                print(f"== Loaded {row_count} rows")
                total_rows += row_count
                success_count += 1
            except Exception as e:
                print(f"== Failed to load {src_table}: {e}")
                error_count += 1

        elapsed = (datetime.now() - start_time).total_seconds()
        print("\n=== ETL Summary: ===")
        print(f"= Success: {success_count}/{len(table_mappings)}")
        print(f"= Total rows: {total_rows}")
        print(f"= Failed: {error_count}")
        print(f"= Duration: {elapsed:.1f}s")

    except Exception as e:
        print(f"== Critical ETL error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    extract()
