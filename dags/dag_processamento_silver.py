from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import subprocess
import os
import sys

script_path = "/opt/airflow/src/etl_data/silver_layer/transform_data_silver.py"

def run_silver(table_name: str):
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script não encontrado: {script_path}...")
    
    result = subprocess.run([sys.executable, script_path, "--table", f"{table_name}"], capture_output=True, text=True)
    
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    
    if result.returncode != 0:
        raise RuntimeError(f"Script falhou com código {result.returncode}...")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="processamento_silver",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2025, 9, 25),
    catchup=False,
    tags=["silver", "olist"]
) as dag:
    
    await_for_bronze = ExternalTaskSensor(
        task_id="aguarda_bronze",
        external_dag_id="ingestao_bronze",
        external_task_id=None,
        mode="poke",
        poke_interval=1,
        timeout=60*60*2
    )

    task_customers_dataset = PythonOperator(
        task_id="run_customers_dataset",
        python_callable=run_silver,
        op_args=["olist_customers_dataset"]
    )

    task_geolocation_dataset = PythonOperator(
        task_id="run_geolocation_dataset",
        python_callable=run_silver,
        op_args=["olist_geolocation_dataset"]
    )

    task_order_items = PythonOperator(
        task_id="run_order_items",
        python_callable=run_silver,
        op_args=["olist_order_items_dataset"]
    )

    task_order_payments = PythonOperator(
        task_id="run_order_payments",
        python_callable=run_silver,
        op_args=["olist_order_payments_dataset"]
    )

    task_reviews = PythonOperator(
        task_id="run_reviews",
        python_callable=run_silver,
        op_args=["olist_order_reviews_dataset"]
    )

    task_orders_dataset = PythonOperator(
        task_id="run_orders_dataset",
        python_callable=run_silver,
        op_args=["olist_orders_dataset"]
    )

    task_products_dataset = PythonOperator(
        task_id="run_products_dataset",
        python_callable=run_silver,
        op_args=["olist_products_dataset"]
    )

    task_sellers_dataset = PythonOperator(
        task_id="run_sellers_dataset",
        python_callable=run_silver,
        op_args=["olist_sellers_dataset"]
    )

    task_product_category_name_translation = PythonOperator(
        task_id="run_product_category_name_translation",
        python_callable=run_silver,
        op_args=["product_category_name_translation"]
    )

    await_for_bronze >> task_customers_dataset >> task_geolocation_dataset >> task_order_items >> task_order_payments >> \
    task_reviews >> task_orders_dataset >> task_products_dataset >> task_sellers_dataset >> task_product_category_name_translation


   