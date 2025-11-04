-- ============================================================================
-- CAMADA BRONZE - Ingestão de Transações de Commodities
-- ============================================================================
-- Descrição: Este arquivo implementa a ingestão incremental de transações
--            de commodities (GOLD, OIL, SILVER) a partir de arquivos CSV.
--
-- Fonte: /Volumes/lakehouse_live/raw_public/transaction_commodities
-- Formato: CSV com header e schema inferido automaticamente
-- Tipo: STREAMING TABLE para processamento incremental
-- Volume: ~8.819 transações de commodities
-- Ativos: GOLD, OIL, SILVER
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze.transaction_commodities
COMMENT "Ingestão incremental de transações de commodities. Dados brutos preservados da fonte original."
AS
SELECT 
  *,
  -- Timestamp de ingestão: registra quando o dado foi processado no pipeline
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse_live/raw_public/transaction_commodities",
  "csv",
  map(
    "header", "true",           -- Primeira linha contém cabeçalho
    "inferSchema", "true"       -- Schema é inferido automaticamente do CSV
  )
);

