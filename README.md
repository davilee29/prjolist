## Projeto Teste - OLIST

## Sobre

Este projeto é um teste utilizando os dados públicos do OLIST, um dataset brasileiro de e-commerce. A ideia principal é explorar, analisar e processar os dados para desenvolver habilidades em manipulação e análise de grandes volumes de dados usando Python.

## Configuração Inicial

Antes de executar qualquer script do projeto, é necessário preparar o ambiente de dados manualmente:

1. Criar o diretório onde os dados serão armazenados:
   mkdir -p src/data

2. Baixar o dataset zipado no link oficial do Kaggle:
   https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

3. Depois do download, descompacte os arquivos dentro da pasta src/data, ou coloque os arquivos zipados diretamente lá, conforme necessidade da aplicação.

4. Certifique-se de ter docker compose (Docker desktop, caso windows) em sua máquina, é a principal ferramenta do projeto.

## Estrutura do Projeto
- dags/                   : Diretório aonde as dags ficam armazenadas.
- src/                    : Código fonte do projeto, scripts, notebooks, etc.
- src/etl_data/           : Diretório de transformação de dados.
- src/utils               : Diretório de utilitários para execução do projeto.
- README.txt              : Este arquivo com as instruções e informações do projeto.

## Como Rodar

- Garanta que o diretório src/data está criado e os dados baixados do Kaggle estejam lá.
- Execute o comando:
  ```
  docker compose up -d
  ```
- Acesse o UI do Airflow (localhost:8080), e acione todas as DAGS presentes.

## Objetivos

- Entender a estrutura dos dados de e-commerce brasileiro.
- Praticar técnicas de ETL e manipulação de dados com Python.
- Desenvolver análises e relatórios básicos a partir dos dados do OLIST.

## Links Úteis

- Dataset no Kaggle: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Questionamentos do Teste ##
- Tratamento de nulos (order_delivered_customer_date): Para essa tratativa, criei uma nova coluna "delivered", com valores 0 ou 1, para delimitar aquilo que foi entregue ou não de forma efetiva. 
Utilizando desta forma se torna possível uma análise aprofundada tanto no quesito de pedidos entregues quanto não entregues.
- Quais são os 10 clientes que mais gastaram (maior total_gasto) no estado de São Paulo (SP) e qual a categoria de produto preferida de cada um deles ?: O arquivo pergunta.sql responde à essa pergunta com um sql simples porém confiável, resultante de uma transformação escalável do processo.
---

Obrigado por utilizar o prjolist!
