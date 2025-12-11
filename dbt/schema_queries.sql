-- Script SQL para extrair schema das tabelas do BigQuery
-- Execute este SQL no BigQuery Console ou bq CLI

SELECT
  table_name,
  column_name,
  data_type,
  is_nullable,
  column_default
FROM
  `olist-data-pipeline.olist_data_pipeline.INFORMATION_SCHEMA.COLUMNS`
ORDER BY
  table_name,
  ordinal_position;

-- Para obter informações das tabelas:
SELECT
  table_name,
  table_type,
  creation_time,
  row_count
FROM
  `olist-data-pipeline.olist_data_pipeline.INFORMATION_SCHEMA.TABLES`
WHERE
  table_type = 'BASE_TABLE'
ORDER BY
  table_name;

-- Para extrair schema específico de uma tabela (exemplo):
-- Substitua 'nome_da_tabela' pelo nome real
SELECT
  column_name,
  data_type,
  is_nullable,
  is_partitioning_column,
  clustering_ordinal_position
FROM
  `olist-data-pipeline.olist_data_pipeline.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'nome_da_tabela'
ORDER BY
  ordinal_position;
