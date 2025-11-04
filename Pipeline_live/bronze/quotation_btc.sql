-- ============================================================================
-- CAMADA BRONZE - Ingestão de Cotações Bitcoin
-- ============================================================================
-- Descrição: Este arquivo implementa a ingestão incremental de cotações
--            de Bitcoin (BTC-USD) a partir de arquivos CSV.
--
-- Fonte: /Volumes/lakehouse_live/raw_public/quotation_btc
-- Formato: CSV com header e schema inferido automaticamente
-- Tipo: STREAMING TABLE para processamento incremental
-- Volume: ~14.182 cotações Bitcoin
-- Símbolo: BTC-USD (será padronizado para BTC na camada Silver)
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze.quotation_btc
COMMENT "Ingestão incremental de cotações Bitcoin. Dados brutos preservados da fonte original."
AS
SELECT 
  *,
  -- Timestamp de ingestão: registra quando o dado foi processado no pipeline
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse_live/raw_public/quotation_btc",
  "csv",
  map(
    "header", "true",           -- Primeira linha contém cabeçalho
    "inferSchema", "true"       -- Schema é inferido automaticamente do CSV
  )
);

