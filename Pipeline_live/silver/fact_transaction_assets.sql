-- ============================================================================
-- CAMADA SILVER - Fato de Transações de Ativos
-- ============================================================================
-- Descrição: Une transações de Bitcoin e Commodities em uma única tabela
--            normalizada com símbolos padronizados e validações de qualidade.
--
-- Fontes: bronze.transaction_btc + bronze.transaction_commodities
-- Tipo: STREAMING TABLE para processamento incremental
-- Transformações:
--   - União de transações BTC e Commodities
--   - Padronização de símbolos de ativos
--   - Normalização de timestamps
--   - Hora aproximada para join com cotações
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE silver.fact_transaction_assets
COMMENT "Tabela fato unificada de transações de ativos (BTC, GOLD, OIL, SILVER) com símbolos padronizados e validações de qualidade."
AS
-- Transações Bitcoin
SELECT 
  transaction_id,
  CAST(data_hora AS TIMESTAMP) as data_hora,
  date_trunc('hour', CAST(data_hora AS TIMESTAMP)) as data_hora_aproximada,
  quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  CASE 
    WHEN UPPER(ativo) IN ('BTC','BTC-USD') THEN 'BTC'
    ELSE 'BTC'
  END AS asset_symbol,
  NULL as unidade,
  NULL as commodity_code_original,
  ativo as ativo_original,
  arquivo_origem,
  importado_em,
  current_timestamp() as processed_at
FROM STREAM(bronze.transaction_btc)

UNION ALL

-- Transações de Commodities
SELECT 
  transaction_id,
  CAST(data_hora AS TIMESTAMP) as data_hora,
  date_trunc('hour', CAST(data_hora AS TIMESTAMP)) as data_hora_aproximada,
  quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  CASE 
    WHEN UPPER(commodity_code) = 'GOLD' THEN 'GOLD'
    WHEN UPPER(commodity_code) = 'OIL' THEN 'OIL'
    WHEN UPPER(commodity_code) = 'SILVER' THEN 'SILVER'
    ELSE 'UNKNOWN'
  END AS asset_symbol,
  unidade,
  commodity_code as commodity_code_original,
  NULL as ativo_original,
  arquivo_origem,
  importado_em,
  current_timestamp() as processed_at
FROM STREAM(bronze.transaction_commodities)

-- Data Quality Expectations
CONSTRAINT quantidade_positiva EXPECT (quantidade > 0) ON VIOLATION DROP ROW
CONSTRAINT data_hora_not_null EXPECT (data_hora IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT tipo_operacao_valido EXPECT (tipo_operacao IN ('COMPRA','VENDA')) ON VIOLATION DROP ROW
CONSTRAINT asset_symbol_valido EXPECT (asset_symbol IN ('BTC','GOLD','OIL','SILVER')) ON VIOLATION DROP ROW;


