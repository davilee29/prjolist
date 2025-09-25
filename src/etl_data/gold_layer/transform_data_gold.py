import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
import logging
import pandas as pd
from utils.db_manager import _get_engine

logging.basicConfig(level=logging.INFO)


class TransformDataGold:
    def __init__(self):
        self.engine = _get_engine('airflow', 'airflow', 5432, 'airflow', 'dw_bronze')
    
    def _build_query(self):
        return """
        with total_pedidos as (select ood.customer_id,
                                    count(distinct ood.order_id) total_pedidos,
                                    sum(oopd.payment_value) total_gasto
                            from dw_silver.olist_orders_dataset ood
                            join dw_silver.olist_order_payments_dataset oopd on ood.order_id = oopd.order_id
                            group by customer_id),
            dias_compras as (select customer_id,
                                    min(order_purchase_timestamp) primeira_compra,
                                    max(order_purchase_timestamp) ultima_compra,
                                    (CURRENT_DATE - max(order_purchase_timestamp)::date)::int dias_desde_ultima_compra
                                from dw_silver.olist_orders_dataset ood 
                                group by customer_id),
            media_entrega as (select   customer_id, 
                                    avg((order_delivered_customer_date::date - order_purchase_timestamp::date)) avg_media_entrega
                                from dw_silver.olist_orders_dataset
                                where delivered = 1
                                group by customer_id),
            categoria_maior_gasto as (select  customer_id,
                                            product_category_name,
                                            max(valor_total)
                                        from 
                                        (select customer_id,
                                            product_category_name,
                                            sum(oopd.payment_value) valor_total
                                        from dw_silver.olist_orders_items_dataset ooid 
                                        join dw_silver.olist_orders_dataset ood on ooid.order_id = ood.order_id
                                        join dw_silver.olist_products_dataset opd on ooid.product_id = opd.product_id
                                        join dw_silver.olist_order_payments_dataset oopd on ooid.order_id = oopd.order_id
                                        group by customer_id,
                                            product_category_name) as tb
                                        group by customer_id, product_category_name)

        select ocd.customer_unique_id,
            ocd.customer_id,
            ocd.customer_state estado_cliente,
            ocd.customer_city cidade_cliente,
            tp.total_pedidos,
            tp.total_gasto,
            dc.primeira_compra data_primeira_compra,
            dc.ultima_compra data_ultima_compra,
            dc.dias_desde_ultima_compra,
            me.avg_media_entrega avg_delivery_time,
            cmg.product_category_name categoria_mais_comprada
        from dw_silver.olist_customers_dataset ocd 
        join total_pedidos tp on ocd.customer_id = tp.customer_id
        join dias_compras dc on ocd.customer_id = dc.customer_id
        left join media_entrega me on ocd.customer_id = me.customer_id
        join categoria_maior_gasto cmg on ocd.customer_id = cmg.customer_id
        """

    def create_table_vendas(self, table_name: str):
        logging.info("Realizando criação da tabela  'dm_vendas_clientes'...")
        query = self._build_query()
        try:
            with self.engine.begin() as conn:
                df = pd.read_sql(query, conn)
                df.to_sql(table_name, conn, index=False, if_exists='replace', schema='dw_gold')
                logging.info(f"Inserção realizada para a tabela {table_name}")
        except Exception as e:
            logging.error(f"Erro na criação da tabela: {e}")

if __name__ == "__main__":
    app = TransformDataGold()
    app.create_table_vendas("dm_vendas_clientes")