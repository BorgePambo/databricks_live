-- ============================================================================
-- CAMADA BRONZE - Ingestão de Cotações yFinance (Commodities)
-- ============================================================================
-- Descrição: Este arquivo implementa a ingestão incremental de cotações
--            de commodities obtidas via yFinance (GC=F, CL=F, SI=F).
--
-- Fonte: /Volumes/lakehouse_live/raw_public/quotation_yfinance
-- Formato: CSV com header e schema inferido automaticamente
-- Tipo: STREAMING TABLE para processamento incremental
-- Volume: ~27.698 cotações de commodities
-- Símbolos: GC=F (GOLD), CL=F (OIL), SI=F (SILVER)
--           Serão padronizados na camada Silver
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze.quotation_yfinance
COMMENT "Ingestão incremental de cotações yFinance para commodities. Dados brutos preservados da fonte original."
AS
SELECT 
  *,
  -- Timestamp de ingestão: registra quando o dado foi processado no pipeline
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse_live/raw_public/quotation_yfinance",
  "csv",
  map(
    "header", "true",           -- Primeira linha contém cabeçalho
    "inferSchema", "true"       -- Schema é inferido automaticamente do CSV
  )
);

