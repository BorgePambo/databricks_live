-- ============================================================================
-- CAMADA SILVER - Fato de Receita de Transações
-- ============================================================================
-- Descrição: Junta transações, cotações e clientes para calcular receitas
--            e métricas financeiras por transação.
--
-- Fontes: silver.fact_transaction_assets + silver.fact_quotation_assets + silver.dim_clientes
-- Tipo: STREAMING TABLE para processamento incremental
-- Transformações:
--   - Join de transações com cotações (por hora aproximada e símbolo)
--   - Join com dimensão de clientes
--   - Cálculo de valor bruto (quantidade × preço)
--   - Cálculo de receita de taxa (0.25% sobre valor bruto)
--   - Aplicação de sinal para COMPRA (-) e VENDA (+)
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE silver.fact_transaction_revenue
COMMENT "Tabela fato de receita de transações com joins de transações, cotações e clientes, incluindo cálculos financeiros."
AS
SELECT 
  -- Chave surrogate do cliente
  c.customer_sk,
  
  -- ID do cliente
  t.cliente_id,
  
  -- Identificador da transação
  t.transaction_id,
  
  -- Data e hora da transação
  t.data_hora,
  
  -- Hora aproximada (usada no join)
  t.data_hora_aproximada,
  
  -- Informações do ativo
  t.asset_symbol,
  t.quantidade,
  t.tipo_operacao,
  t.moeda,
  t.canal,
  t.mercado,
  
  -- Informações da cotação
  q.preco as preco_cotacao,
  q.horario_coleta as timestamp_cotacao,
  
  -- Valor bruto da transação (quantidade × preço)
  t.quantidade * q.preco as gross_value,
  
  -- Valor bruto com sinal: VENDA (+) / COMPRA (-)
  CASE 
    WHEN t.tipo_operacao = 'VENDA' THEN (t.quantidade * q.preco)
    WHEN t.tipo_operacao = 'COMPRA' THEN -(t.quantidade * q.preco)
    ELSE 0
  END as gross_value_sinal,
  
  -- Receita de taxa: 0.25% sobre o valor bruto
  (t.quantidade * q.preco) * 0.0025 as fee_revenue,
  
  -- Timestamp de processamento
  current_timestamp() as processed_at

FROM STREAM(silver.fact_transaction_assets) t
INNER JOIN STREAM(silver.fact_quotation_assets) q
  ON t.data_hora_aproximada = q.data_hora_aproximada
  AND t.asset_symbol = q.asset_symbol
INNER JOIN STREAM(silver.dim_clientes) c
  ON t.cliente_id = c.customer_id

-- Data Quality Expectations
CONSTRAINT gross_value_positivo EXPECT (gross_value > 0) ON VIOLATION DROP ROW
CONSTRAINT fee_revenue_positivo EXPECT (fee_revenue > 0) ON VIOLATION DROP ROW
CONSTRAINT customer_sk_not_null EXPECT (customer_sk IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT cotacao_valida EXPECT (preco_cotacao > 0 AND timestamp_cotacao <= data_hora) ON VIOLATION DROP ROW;


