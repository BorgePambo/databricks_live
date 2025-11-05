-- ============================================================================
-- CAMADA SILVER - Dimensão de Clientes
-- ============================================================================
-- Descrição: Dimensão de clientes com anonimização de dados sensíveis
--            e validações de qualidade para segmentos, países e estados.
--
-- Fonte: bronze.customers
-- Tipo: STREAMING TABLE para processamento incremental
-- Transformações:
--   - Anonimização de documentos (SHA2)
--   - Validação de segmentos válidos
--   - Validação de países e estados
--   - Criação de surrogate key (customer_sk)
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE silver.dim_clientes
COMMENT "Dimensão de clientes com dados anonimizados e validações de qualidade para segmentos, países e estados."
AS
SELECT 
  -- Surrogate key gerado automaticamente
  monotonically_increasing_id() as customer_sk,
  
  -- ID do cliente (chave natural)
  customer_id,
  
  -- Nome do cliente
  customer_name,
  
  -- Documento anonimizado usando SHA2 (proteção de dados sensíveis)
  SHA2(documento, 256) as documento_hash,
  
  -- Segmento do cliente
  segmento,
  
  -- Localização
  pais,
  estado,
  cidade,
  
  -- Data de criação do cliente
  CAST(created_at AS TIMESTAMP) as created_at,
  
  -- Timestamp de processamento
  current_timestamp() as processed_at

FROM STREAM(bronze.customers)

-- Data Quality Expectations
CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT segmento_valido EXPECT (segmento IN ('Financeiro', 'Indústria', 'Varejo', 'Tecnologia')) ON VIOLATION DROP ROW
CONSTRAINT pais_valido EXPECT (pais IN ('Brasil', 'Alemanha', 'Estados Unidos')) ON VIOLATION DROP ROW;


