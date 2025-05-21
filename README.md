# Projeto de Pipeline Incremental de Vendas com Delta Lake (Databricks e PySpark)

Este projeto demonstra a construção de um pipeline incremental de dados de Vendas com ingestão em Batch no Databricks utilizando a arquitetura Lakehouse com Delta Lake. Ele simula a ingestão, transformação e análise de dados a partir de arquivos CSV.

## Tecnologias Utilizadas

- Databricks (Community Edition)
- PySpark
- Delta Lake
- Lakehouse Architecture (Bronze, Silver, Gold)
- CSV como fonte de dados

## Descrição do Pipeline de Vendas com Camadas Bronze, Silver e Gold
- Fonte de Dados
Arquivos CSV contendo os registros de vendas, incluindo informações sobre produtos, fabricantes, categorias, segmentos, clientes e localizações geográficas.

- Camada Bronze (Raw)
Responsável por armazenar os dados brutos provenientes do CSV.
Não há transformação, apenas a ingestão dos dados como estão, preservando a integridade e a fidelidade da fonte.
Objetivo: garantir rastreabilidade e auditoria dos dados originais.

- Camada Silver (Staging / Curated)
Realiza o tratamento e a padronização dos dados.
Limpeza de dados (remoção de nulos, registros duplicados e inconsistências).
Prepara os dados para modelagem analítica.

- Camada Gold (Modelo Analítico)
Dados estruturados no modelo estrela (Star Schema) para análise.
Implementa carga incremental, ou seja, apenas novos registros ou registros alterados são inseridos ou atualizados, otimizando o desempenho e garantindo atualizações eficientes.

Esta camada é composta por:

Tabela Fato: FT_VENDAS — Contém os registros das transações de vendas com métricas quantitativas (valor, quantidade, etc.).

Tabelas Dimensão:

DIM_PRODUTO — Informações dos produtos.
DIM_FABRICANTE — Dados dos fabricantes.
DIM_CATEGORIA — Classificação dos produtos.
DIM_SEGMENTO — Segmento de atuação ou de mercado.
DIM_CLIENTE — Dados dos clientes.
DIM_GEOGRAFIA — Informações geográficas (cidade, estado, região, país).

- Relacionamentos
A tabela FT_VENDAS se relaciona diretamente com:

DIM_PRODUTO
DIM_FABRICANTE
DIM_CATEGORIA
DIM_SEGMENTO
DIM_CLIENTE

A dimensão DIM_GEOGRAFIA se relaciona com DIM_CLIENTE, permitindo análises geográficas dos clientes e, consequentemente, das vendas.

- Objetivo do Pipeline
Proporcionar uma base analítica robusta, escalável e eficiente.
Possibilitar análises de desempenho de vendas, comportamento de clientes, performance de produtos e insights segmentados por localização, categoria, fabricante e outros critérios de negócio.


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

