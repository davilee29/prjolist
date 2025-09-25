import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
import os
import logging
import pandas as pd
from utils.db_manager import _get_engine

logging.basicConfig(level=logging.INFO)

class InsertDataDW:
    def __init__(self):
        self.engine = _get_engine('airflow', 'airflow', 5432, 'airflow', 'dw_bronze')
    
    def _extract_and_load(self, df: pd.DataFrame, table_name: str, schema):
        logging.info("Realizando insert dos arquivos para a camada bronze...")
        with self.engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False, schema=schema)
        
    def read_insert_data(self):
        logging.info("Iniciando processo de leitura das tabelas para inserção...")
        path = r"/opt/airflow/src/data"
        for file in os.listdir(path):
            df = pd.read_csv(path + '/' + file, sep=',')
            self._extract_and_load(df, f'{Path(file).stem}', 'dw_bronze')

if __name__ == "__main__":
    app = InsertDataDW()
    app.read_insert_data()
