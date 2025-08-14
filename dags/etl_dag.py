from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 8),
    "retries": 1,
}

SPARK_CONN_ID = "spark_default"  # cấu hình trong Airflow Connections
BASE_PATH = "/home/ldduc/D/data-warehouse-apache-ecosystem/src"

COMMON_SPARK_CONF = {
    "spark.yarn.queue": "default"
}

COMMON_SPARK_PARAMS = {
    "conn_id": SPARK_CONN_ID,
    "conf": COMMON_SPARK_CONF,
    "proxy_user": "hiveuser",
    "executor_memory": "2g",
    "driver_memory": "1g",
    "num_executors": 2,
    "executor_cores": 1
}

with DAG(
    dag_id='etl_crm_erp',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    extract_to_bronze = SparkSubmitOperator(
        task_id="extract_from_db_to_bronze",
        application=f"{BASE_PATH}/bronze/extract.py",
        **COMMON_SPARK_PARAMS
    )

    transform_crm_cust_info = SparkSubmitOperator(
        task_id="transform_crm_cust_info_to_silver",
        application=f"{BASE_PATH}/silver/transform_crm_cust_info.py",
        **COMMON_SPARK_PARAMS
    )

    transform_crm_prd_info = SparkSubmitOperator(
        task_id="transform_crm_prd_info_to_silver",
        application=f"{BASE_PATH}/silver/transform_crm_prd_info.py",
        **COMMON_SPARK_PARAMS
    )

    transform_crm_sales_details = SparkSubmitOperator(
        task_id="transform_crm_sales_details_to_silver",
        application=f"{BASE_PATH}/silver/transform_crm_sales_details.py",
        **COMMON_SPARK_PARAMS
    )

    transform_erp_cust_az12 = SparkSubmitOperator(
        task_id="transform_erp_cust_az12_to_silver",
        application=f"{BASE_PATH}/silver/transform_erp_cust_az12.py",
        **COMMON_SPARK_PARAMS
    )

    transform_erp_loc_a101 = SparkSubmitOperator(
        task_id="transform_erp_loc_a101_to_silver",
        application=f"{BASE_PATH}/silver/transform_erp_loc_a101.py",
        **COMMON_SPARK_PARAMS
    )

    transform_erp_px_cat_g1v2 = SparkSubmitOperator(
        task_id="transform_erp_px_cat_g1v2_to_silver",
        application=f"{BASE_PATH}/silver/transform_erp_px_cat_g1v2.py",
        **COMMON_SPARK_PARAMS
    )

    view_silver_to_gold = SparkSubmitOperator(
        task_id="view_silver_to_gold",
        application=f"{BASE_PATH}/gold/view_gold.py",
        **COMMON_SPARK_PARAMS
    )

    extract_to_bronze >> [
        transform_crm_cust_info,
        transform_crm_prd_info,
        transform_crm_sales_details,
        transform_erp_cust_az12,
        transform_erp_loc_a101,
        transform_erp_px_cat_g1v2,
    ] >> view_silver_to_gold
