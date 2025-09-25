import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
import logging
import pandas as pd
from utils.db_manager import _get_engine
import re
import unicodedata
import argparse


logging.basicConfig(level=logging.INFO)

class TransformDataSilver:
    def __init__(self):
       self.engine = _get_engine('airflow', 'airflow', 5432, 'airflow', 'dw_silver')
    
    def _normalize_columns(self, df: pd.DataFrame, columns: list):
        logging.info("Realizando normalização das colunas...")
        def normalize_text(text):
            if pd.isnull(text):
                return text
            text = str(text).strip()
            text = re.sub(r'\s+', ' ', text)
            text = unicodedata.normalize('NFKD', text)
            text = ''.join(c for c in text if not unicodedata.combining(c))
            return text.lower()

        for col in columns:
            if col in df.columns:
                df[col] = df[col].apply(normalize_text)
            else:
                logging.warning(f"Coluna '{col}' não encontrada...")
                
        return df              

    def _columns_to_timestamp(self, df: pd.DataFrame, columns: list):
        logging.info("Realizando tratativa para colunas tipo TIMESTAMP...")         
        for col in columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except Exception as e:
                    logging.error(f"Erro ao converter a coluna '{col}' para datetime: {e}")
        return df 
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Realizando tratativa para dados duplicados...")
        return df.drop_duplicates()
    
    def _read_data_postgres(self, table_name: str):
        df = pd.read_sql_table(f"{table_name}", self.engine, schema="dw_bronze")
        return df
    
    def transform_insert_data(self,
                              df: pd.DataFrame,
                              columns_list: list,
                              normalize_text: bool = False,
                              columns_timestamp: bool = False,
                              remove_duplicates:bool = False,
                              create_new_field:bool = False):
        if normalize_text:
            df = self._normalize_columns(df, columns=columns_list)
        if columns_timestamp:
            df = self._columns_to_timestamp(df, columns=columns_list)
        if remove_duplicates:
            df = self._remove_duplicates(df)

        return df

    def write_silver_data_postgres(self, df, table_name: str):
        logging.info("Realizando inserção de dados transformados para a camada SILVER...")
        with self.engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False, schema='dw_silver')
        logging.info(f"Inserção realizada para a tabela {table_name}")

    def process_table(self, table_name: str):
        logging.info(f"[INICIALIZANDO] Processo para a tabela {table_name}...")

        if table_name == 'olist_customers_dataset':
            normalize_columns = ['customer_city']
            df = self._read_data_postgres(table_name)
            df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str).str.zfill(5).str[:5]
            df = self.transform_insert_data(df, columns_list=normalize_columns, normalize_text=True)
            df = self.transform_insert_data(df, columns_list=[], remove_duplicates=True)
            self.write_silver_data_postgres(df, table_name)

        elif table_name == 'olist_geolocation_dataset':
            normalize_columns = ['geolocation_city']
            df = self._read_data_postgres(table_name)
            df = self.transform_insert_data(df, columns_list=normalize_columns, normalize_text=True)
            df = self.transform_insert_data(df, columns_list=[], remove_duplicates=True)
            self.write_silver_data_postgres(df, table_name)

        elif table_name == 'olist_orders_dataset':
            timestamp_columns = [
                'order_purchase_timestamp', "order_approved_at",
                "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"
            ]
            normalize_columns = ['order_status']
            df = self._read_data_postgres(table_name)
            df["delivered"] = df["order_delivered_customer_date"].notna().astype(int)
            df = self.transform_insert_data(df, columns_list=timestamp_columns, columns_timestamp=True)
            df = self.transform_insert_data(df, columns_list=normalize_columns, normalize_text=True)
            df = self.transform_insert_data(df, columns_list=[], remove_duplicates=True)
            self.write_silver_data_postgres(df, table_name)

        elif table_name == 'olist_order_items_dataset':
            timestamp_columns = ['shipping_limit_date']
            df = self._read_data_postgres(table_name)
            df = self.transform_insert_data(df, columns_list=timestamp_columns, columns_timestamp=True)
            df = self.transform_insert_data(df, columns_list=[], remove_duplicates=True)
            self.write_silver_data_postgres(df, 'olist_orders_items_dataset')

        elif table_name == 'olist_order_payments_dataset':
            normalize_columns = ['payment_type']
            df = self._read_data_postgres(table_name)
            df = self.transform_insert_data(df, columns_list=normalize_columns, normalize_text=True)
            df = self.transform_insert_data(df, columns_list=[], remove_duplicates=True)
            self.write_silver_data_postgres(df, table_name)

        elif table_name == 'olist_order_reviews_dataset':
            timestamp_columns = ['review_creation_date', 'review_answer_timestamp']
            df = self._read_data_postgres(table_name)
            df = self.transform_insert_data(df, columns_list=timestamp_columns, columns_timestamp=True)
            df = self.transform_insert_data(df, columns_list=[], remove_duplicates=True)
            self.write_silver_data_postgres(df, table_name)

        elif table_name == 'olist_products_dataset':
            normalize_columns = ['product_category_name']
            df = self._read_data_postgres(table_name)
            df = self.transform_insert_data(df, columns_list=normalize_columns, normalize_text=True)
            df = self.transform_insert_data(df, columns_list=[], remove_duplicates=True)
            self.write_silver_data_postgres(df, table_name)

        elif table_name == 'olist_sellers_dataset':
            normalize_columns = ['seller_city']
            df = self._read_data_postgres(table_name)
            df = self.transform_insert_data(df, columns_list=normalize_columns, normalize_text=True)
            df = self.transform_insert_data(df, columns_list=[], remove_duplicates=True)
            self.write_silver_data_postgres(df, table_name)

        elif table_name == 'product_category_name_translation':
            normalize_columns = ['product_category_name', 'product_category_name_english']
            df = self._read_data_postgres(table_name)
            df = self.transform_insert_data(df, columns_list=normalize_columns, normalize_text=True)
            df = self.transform_insert_data(df, columns_list=[], remove_duplicates=True)
            self.write_silver_data_postgres(df, table_name)

        else:
            logging.error(f"Tabela {table_name} não reconhecida ou não suportada para transformação.")
            return

        logging.info(f"[FINALIZADO] Processo finalizado para a tabela {table_name}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transformação e inserção de dados em camada SILVER.")
    parser.add_argument('--table', required=True, help='Nome da tabela para processar')
    args = parser.parse_args()
    app = TransformDataSilver()
    app.process_table(args.table)