-- ============================================================================
-- CAMADA BRONZE - Ingestão de Transações Bitcoin
-- ============================================================================
-- Descrição: Este arquivo implementa a ingestão incremental de transações
--            de Bitcoin a partir de arquivos CSV armazenados em Volumes.
--
-- Fonte: /Volumes/lakehouse_live/raw_public/transacation_btc
-- Formato: CSV com header e schema inferido automaticamente
-- Tipo: STREAMING TABLE para processamento incremental
-- Volume: ~5.901 transações Bitcoin
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze.transaction_btc
COMMENT "Ingestão incremental de transações Bitcoin. Dados brutos preservados da fonte original."
AS
SELECT 
  *,
  -- Timestamp de ingestão: registra quando o dado foi processado no pipeline
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse_live/raw_public/transacation_btc",
  "csv",
  map(
    "header", "true",           -- Primeira linha contém cabeçalho
    "inferSchema", "true"       -- Schema é inferido automaticamente do CSV
  )
);

