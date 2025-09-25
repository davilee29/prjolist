-- Quais são os 10 clientes que mais gastaram (maior total_gasto) no estado de São Paulo (SP) e qual a categoria de produto preferida de cada um deles?
select customer_unique_id,
	   total_gasto,
	   categoria_mais_comprada
from dw_gold.dm_vendas_clientes dvc
where dvc.estado_cliente = 'SP'
order by total_gasto desc
limit 10;