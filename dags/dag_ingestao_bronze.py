from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

requirements_path = "/opt/airflow/src/requirements.txt"
schemas_path = "/opt/airflow/src/create_schemas.py"
script_path = "/opt/airflow/src/etl_data/bronze_layer/insert_bronze_olist.py"

def install_requirements():
    if not os.path.exists(requirements_path):
        print(f"requirements.txt não encontrado em {requirements_path}, pulando instalação.")
        return
    
    result = subprocess.run([sys.executable, "-m", "pip", "install", "-r", requirements_path],
                            capture_output=True, text=True)
    
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    
    if result.returncode != 0:
        raise RuntimeError(f"Falha ao instalar os requirements, código {result.returncode}")

def run_create_schemas():
    if not os.path.exists(schemas_path):
        raise FileNotFoundError(f"Script não encontrado: {schemas_path}")
    
    result = subprocess.run([sys.executable, schemas_path], capture_output=True, text=True)
    
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    
    if result.returncode != 0:
        raise RuntimeError(f"Script falhou com código {result.returncode}")

def run_bronze():
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
    dag_id="ingestao_bronze",
    default_args=default_args,
    schedule_interval="@hourly", 
    start_date=datetime(2025, 9, 25),
    catchup=False,
    tags=["bronze", "olist"]
) as dag:

    install_requirements =  PythonOperator(
        task_id="install_requirements",
        python_callable=install_requirements
    )

    create_schemas = PythonOperator(
        task_id="create_schemas",
        python_callable=run_create_schemas
    )

    run_script_task = PythonOperator(
        task_id="run_script",
        python_callable=run_bronze
    )

    install_requirements >> create_schemas >> run_script_task