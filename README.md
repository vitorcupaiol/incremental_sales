# Projeto de Pipeline Incremental de Vendas com Delta Lake (Databricks e PySpark)

Este projeto demonstra a construção de um pipeline incremental de dados de Vendas com ingestão em Batch no Databricks utilizando a arquitetura Lakehouse com Delta Lake. Ele simula a ingestão, transformação e análise de dados a partir de arquivos CSV.

## Tecnologias Utilizadas

- Databricks (Community Edition)
- PySpark
- Delta Lake
- Lakehouse Architecture (Bronze, Silver, Gold)
- CSV como fonte de dados

## Estrutura do Projeto
incremental-sales/

├── notebooks/

│ ├── 000 Initial Setup.ipynb

│ ├── 001_Loading_Bronze.ipynb

│ ├── 002_Transformation_Silver.ipynb

│ ├── 003_Load_Gold_Delta.ipynb

│ ├── 003_Load_Gold_Delta_Incremental.ipynb

├── dados-exemplo/

│ └── dados_2011.csv

│ └── dados_2012.csv

│ └── dados_2013.csv

├── images/

│ └── arquitetura.png

└── README.md

## Execução
- Executar o notebook 000 Initial Setup.ipynb para criação dos diretórios.
- Executar o notebook 001_Loading_Bronze.ipynb para criar os arquivos na camada Bronze.
- Executar o notebook 002_Transformation_Silver.ipynb para criar o arquivo unificado na camada Silver.
- Na primeira execução da Gold, executar o notebook 003_Load_Gold_Delta.ipynb para criar as Dimensões e a Fato de Vendas.
- As próximas execuções, executar o notebook 003_Load_Gold_Delta_Incremental.ipynb para adicionar os dados novos nas Dimensões e na Fato de Vendas.

## Visualização
Use o comando display(df_gold) no Databricks para visualizar dashboards interativos com gráficos de barras, pizza e tabelas.

## Autor
Desenvolvido por Vitor Oliveira Cupaiol, como parte de um portfólio de engenharia de dados com foco em Databricks, Delta Lake e PySpark.

## Observações
O projeto pode ser executado no Databricks Community Edition gratuitamente.
A estrutura pode ser expandida com uso de ferramentas como Apache Airflow, DBT, ou ingestão via streaming (Kafka, Auto Loader, etc).

