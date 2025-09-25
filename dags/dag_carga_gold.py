from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import subprocess
import os
import sys

script_path = "/opt/airflow/src/etl_data/gold_layer/transform_data_gold.py"

def run_gold():
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script não encontrado: {script_path}")
    
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    
    if result.returncode != 0:
        raise RuntimeError(f"Script falhou com código {result.returncode}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="carga_gold",
    default_args=default_args,
    schedule_interval="@hourly",  
    start_date=datetime(2025, 9, 25),
    catchup=False,
    tags=["gold", "olist"]
) as dag:

    await_for_silver = ExternalTaskSensor(
        task_id="aguarda_silver",
        external_dag_id="processamento_silver",
        external_task_id=None,
        mode="poke",
        poke_interval=1,
        timeout=60*60*2
    )

    run_script_task = PythonOperator(
        task_id="run_script_gold",
        python_callable=run_gold
    )

    await_for_silver >> run_script_task