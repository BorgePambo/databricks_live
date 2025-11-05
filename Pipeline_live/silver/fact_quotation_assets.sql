-- ============================================================================
-- CAMADA SILVER - Fato de Cotações de Ativos
-- ============================================================================
-- Descrição: Une cotações de Bitcoin e Commodities em uma única tabela
--            normalizada com símbolos padronizados e validações de qualidade.
--
-- Fontes: bronze.quotation_btc + bronze.quotation_yfinance
-- Tipo: STREAMING TABLE para processamento incremental
-- Transformações:
--   - União de cotações BTC e yFinance
--   - Padronização de símbolos de ativos
--   - Normalização de timestamps
--   - Hora aproximada para join com transações
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE silver.fact_quotation_assets
COMMENT "Tabela fato unificada de cotações de ativos (BTC, GOLD, OIL, SILVER) com símbolos padronizados e validações de qualidade."
AS
-- Cotações de Bitcoin
SELECT 
  CAST(horario_coleta AS TIMESTAMP) as horario_coleta,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) as data_hora_aproximada,
  preco,
  moeda,
  CASE 
    WHEN UPPER(ativo) IN ('BTC','BTC-USD') THEN 'BTC'
    ELSE 'BTC'
  END AS asset_symbol,
  ativo as ativo_original,
  current_timestamp() as processed_at
FROM STREAM(bronze.quotation_btc)

UNION ALL

-- Cotações de yFinance (Commodities)
SELECT 
  CAST(horario_coleta AS TIMESTAMP) as horario_coleta,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) as data_hora_aproximada,
  preco,
  moeda,
  CASE 
    WHEN UPPER(ativo) = 'GC=F' THEN 'GOLD'
    WHEN UPPER(ativo) = 'CL=F' THEN 'OIL'
    WHEN UPPER(ativo) = 'SI=F' THEN 'SILVER'
    ELSE 'UNKNOWN'
  END AS asset_symbol,
  ativo as ativo_original,
  current_timestamp() as processed_at
FROM STREAM(bronze.quotation_yfinance)

-- Data Quality Expectations
CONSTRAINT preco_positivo EXPECT (preco > 0) ON VIOLATION DROP ROW
CONSTRAINT horario_coleta_valido EXPECT (horario_coleta <= current_timestamp()) ON VIOLATION DROP ROW
CONSTRAINT ativo_not_null EXPECT (ativo_original IS NOT NULL AND ativo_original != '') ON VIOLATION DROP ROW
CONSTRAINT moeda_usd EXPECT (moeda = 'USD') ON VIOLATION DROP ROW;


