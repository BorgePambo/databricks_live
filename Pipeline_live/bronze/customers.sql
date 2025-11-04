-- ============================================================================
-- CAMADA BRONZE - Ingestão de Dados de Clientes
-- ============================================================================
-- Descrição: Este arquivo implementa a ingestão incremental de dados de clientes
--            a partir de arquivos CSV armazenados em Volumes do Databricks.
--
-- Fonte: /Volumes/lakehouse_live/raw_public/customers
-- Formato: CSV com header e schema inferido automaticamente
-- Tipo: STREAMING TABLE para processamento incremental
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze.customers
COMMENT "Ingestão incremental de dados de clientes. Dados brutos preservados da fonte original."
AS
SELECT 
  *,
  -- Timestamp de ingestão: registra quando o dado foi processado no pipeline
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse_live/raw_public/customers",
  "csv",
  map(
    "header", "true",           -- Primeira linha contém cabeçalho
    "inferSchema", "true"       -- Schema é inferido automaticamente do CSV
  )
);

