from pyspark.sql import SparkSession
from datetime import datetime
import sys
import uuid

def log_job_to_postgres(spark, job_name, start_time, end_time, record_count, status, error_message=None):
    log_id = str(uuid.uuid4())
    run_date = start_time.date()
    created_date = datetime.now()
    job_name = job_name[:255]
    error_message = error_message[:255] if error_message is not None else None
    data = [(log_id, job_name, run_date, start_time, end_time, record_count, status, error_message, created_date)]
    columns = [
        "log_id", "job_name", "run_date", "start_time", "end_time", "record_count",
        "status", "error_message", "created_date"
    ]
    df = spark.createDataFrame(data, columns)
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://localhost:5432/crm_erp") \
      .option("dbtable", "job_log") \
      .option("user", "postgres") \
      .option("password", "6666") \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()

def config_job_to_postgres(spark, job_name, source_schema, source_table, source_db_type, source_ip,
                           destination_schema, destination_table, destination_db_type, destination_ip,
                           load_type, schedule_type, is_active):
    config_id = str(uuid.uuid4())
    created_date = datetime.now()
    load_type = 0 if load_type == "full" else 1
    job_name = job_name[:255]

    data = [(config_id, job_name, source_schema, source_table, source_db_type, source_ip,
             destination_schema, destination_table, destination_db_type, destination_ip,
             load_type, schedule_type, is_active, created_date)]
    columns = [
        "config_id", "job_name", "source_schema", "source_table", "source_db_type", "source_ip",
        "destination_schema", "destination_table", "destination_db_type", "destination_ip",
        "load_type", "schedule_type", "is_active", "created_date"
    ]
    df = spark.createDataFrame(data, columns)
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://localhost:5432/crm_erp") \
      .option("dbtable", "job_config") \
      .option("user", "postgres") \
      .option("password", "6666") \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()

def extract():
    print(f"== Starting PostgreSQL to Bronze Layer ... ==")
    spark = None
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
        spark.sparkContext.setLogLevel("ERROR")

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
            print(f"\n== Loading {src_table} -> {tgt_table} ...")
            job_start = datetime.now()
            job_name = f"extract_{src_table}_to_{tgt_table}"
            try:
                # Log job config before extraction
                df_exist = spark.table(tgt_table)
                exist_count = df_exist.count()

                if exist_count == 0:
                    mode = "full"
                else:
                    mode = "increment"

                config_job_to_postgres(
                    spark,
                    job_name=job_name,
                    source_schema="public",
                    source_table=src_table,
                    source_db_type="postgres",
                    source_ip="localhost",
                    destination_schema="bronze",
                    destination_table=tgt_table.split(".")[1],
                    destination_db_type="hive",
                    destination_ip="localhost",
                    load_type=mode,
                    schedule_type="manual",
                    is_active=1
                )

                if mode == "increment":
                    query = f"(SELECT * FROM {src_table} WHERE src_update_at > (NOW() - INTERVAL '1 day')) AS tmp"
                    df = spark.read.jdbc(postgres_url, query, properties=props)
                else:
                    df = spark.read.jdbc(postgres_url, src_table, properties=props)
                row_count = df.count()
                df.write.mode("overwrite" if mode == "full" else "append").saveAsTable(tgt_table)
                print(f"== Loaded {row_count} rows")
                total_rows += row_count
                success_count += 1
                log_job_to_postgres(
                    spark,
                    job_name=job_name,
                    start_time=job_start,
                    end_time=datetime.now(),
                    record_count=row_count,
                    status="success",
                    error_message=None
                )
            except Exception as e:
                print(f"== Failed to load {src_table}: {e}")
                error_count += 1
                log_job_to_postgres(
                    spark,
                    job_name=job_name,
                    start_time=job_start,
                    end_time=datetime.now(),
                    record_count=0,
                    status="failed",
                    error_message=str(e)
                )

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
        if spark:
            spark.stop()

if __name__ == "__main__":
    extract()
