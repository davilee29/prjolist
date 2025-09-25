import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import os
import logging
from utils.db_manager import _get_engine
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)

def create_schemas():
    logging.info("Criando schemas para início do processo...")
    sql = """
    CREATE SCHEMA IF NOT EXISTS dw_bronze;
    CREATE SCHEMA IF NOT EXISTS dw_silver;
    CREATE SCHEMA IF NOT EXISTS dw_gold;
    """
    engine = _get_engine('airflow', 'airflow', 5432, 'airflow', 'airflow')
    with engine.begin() as conn:
        conn.execute(text(sql))

    print("Schemas criados ou já existentes.")


if __name__ == "__main__":
    create_schemas()