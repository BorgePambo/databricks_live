# Iniciativa 001: Segmentação de Cliente - Pipeline Completo

## 📋 Informações Gerais

| Campo | Descrição |
|-------|------------|
| **Nome** | Segmentação de cliente |
| **Área de Negócio** | Pricing |
| **Data de Início** | 14/10/2025 |
| **Data de Finalização** | 15/10/2025 |
| **Status Atual** | ✅ **IMPLEMENTADO E FUNCIONANDO** |

---

## 🎯 1. Contexto e Impacto

### Problema / Desafio Atual
- Não existe priorização de atendimento entre clientes de alto e baixo valor
- As análises atuais são manuais e demoradas, dificultando decisões estratégicas
- Há baixa personalização no relacionamento com clientes e oportunidades perdidas de upsell
- Clientes com pouca atividade tendem ao churn, sem monitoramento preventivo

### Objetivo
- Criar segmentação de clientes baseada em comportamento transacional, rentabilidade e frequência de uso
- Identificar clientes mais valiosos (Top 20/50) e clientes em risco (Bottom 50) para personalização de atendimento e aumento de rentabilidade

### Impacto Esperado
- Aumento de receita com base em clientes prioritários
- Diminuição do churn com ações proativas
- Melhoria no LTV (Lifetime Value) e na lucratividade total da carteira

### KPIs / Indicadores Alvo
- Redução de churn (%)
- Aumento do LTV médio (R$)
- Crescimento da receita de taxa
- Maior frequência média de transações (últimos 30 dias)
- Aumento do ticket médio de transação

---

## 🏗️ 2. Arquitetura Implementada

### 📊 **Pipeline Lakeflow Declarative Pipelines**

```text
📁 Volumes CSV → 🥉 Bronze → 🥈 Silver → 🥇 Gold
```

### 🔄 **Fluxo de Dados Implementado**

1. **Ingestão Bronze**: `cloud_files()` dos volumes CSV → 5 tabelas Bronze
2. **Transformação Silver**: União, normalização, casting de tipos → 4 tabelas Silver
3. **Agregação Gold**: Métricas de negócio e segmentação → 1 tabela Gold

---

## 📁 3. Estrutura de Arquivos Implementada

```text
aula_03/pipeline/transformations/
├── README.md (documentação completa)
├── bronze/
│   ├── customers.sql
│   ├── transacation_btc.sql
│   ├── transaction_commodities.sql
│   ├── quotation_btc.sql
│   └── quotation_yfinance.sql
├── silver/
│   ├── fact_transaction_assets.sql
│   ├── fact_quotation_assets.sql
│   ├── dim_clientes.sql
│   └── fact_transaction_revenue.sql
└── gold/
    └── mostvaluableclient.sql
```

---

## 🥉 4. Camada Bronze - Ingestão

### 📋 **Tabelas Implementadas (5 tabelas)**

| Tabela | Volume CSV | Descrição |
|--------|------------|-----------|
| `bronze.customers` | `/Volumes/lakehouse/raw_public/customers` | Dados de clientes |
| `bronze.transaction_btc` | `/Volumes/lakehouse/raw_public/transacation_btc` | Transações Bitcoin |
| `bronze.transaction_commodities` | `/Volumes/lakehouse/raw_public/transaction_commodities` | Transações Commodities |
| `bronze.quotation_btc` | `/Volumes/lakehouse/raw_public/quotation_btc` | Cotações Bitcoin |
| `bronze.quotation_yfinance` | `/Volumes/lakehouse/raw_public/quotation_yfinance` | Cotações yFinance |

### 🔧 **Configuração cloud_files**

```sql
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/[nome_arquivo]",
  "csv",
  map("header", "true", "inferSchema", "true")
)
```

### 📄 **Exemplos dos CSVs dos Volumes**

#### **customers.csv** (12 linhas)
```csv
customer_id,customer_name,documento,segmento,pais,estado,cidade,created_at
C001,Moraes Ltda.,93.721.408/0001-33,Financeiro,Brasil,RS,das Neves,2022-11-19 19:41:33.009156+00:00
C002,Lopes da Mata S.A.,62.317.450/0001-60,Indústria,Brasil,PA,Teixeira de Sampaio,2024-03-27 13:40:54.766519+00:00
C003,Silveira Borges e Filhos,18.756.402/0001-86,Varejo,Brasil,AL,da Rocha,2023-11-23 12:29:58.750642+00:00
```

#### **transacation_btc.csv** (5.901 linhas)
```csv
transaction_id,data_hora,ativo,quantidade,tipo_operacao,moeda,cliente_id,canal,mercado,arquivo_origem,importado_em
BTCX-00000001,2024-01-01 12:45:00+00:00,BTC,0.42,VENDA,USD,C009,ONLINE,US,btc_planilha.xlsx,2025-08-13 20:41:28.730155+00:00
BTCX-00000002,2024-01-01 18:17:00+00:00,BTC,0.01,COMPRA,USD,C001,RETAIL,US,btc_planilha.xlsx,2025-08-13 20:41:28.730267+00:00
BTCX-00000003,2024-01-01 14:17:00+00:00,BTC,0.1,COMPRA,USD,C010,DISTRIB,BR,btc_planilha.xlsx,2025-08-13 20:41:28.730352+00:00
```

#### **transaction_commodities.csv** (8.819 linhas)
```csv
transaction_id,data_hora,commodity_code,quantidade,tipo_operacao,unidade,moeda,cliente_id,canal,mercado,arquivo_origem,importado_em
COM-00000001,2024-01-01 17:20:00+00:00,GOLD,41.0,VENDA,oz,USD,C001,RETAIL,BR,commodities_operacional.sql,2025-08-13 20:41:29.099959+00:00
COM-00000002,2024-01-01 13:32:00+00:00,OIL,14.0,VENDA,bbl,USD,C007,ONLINE,EU,commodities_operacional.sql,2025-08-13 20:41:29.100021+00:00
COM-00000003,2024-01-01 13:36:00+00:00,SILVER,25.0,VENDA,oz,USD,C002,RETAIL,BR,commodities_operacional.sql,2025-08-13 20:41:29.100075+00:00
```

#### **quotation_btc.csv** (14.182 linhas)
```csv
ativo,preco,moeda,horario_coleta
BTC-USD,42477.25390625,USD,2024-01-01 00:00:00+00:00
BTC-USD,42622.8984375,USD,2024-01-01 01:00:00+00:00
BTC-USD,42576.6015625,USD,2024-01-01 02:00:00+00:00
BTC-USD,42320.73046875,USD,2024-01-01 03:00:00+00:00
```

#### **quotation_yfinance.csv** (27.698 linhas)
```csv
ativo,preco,moeda,horario_coleta
GC=F,2083.199951171875,USD,2024-01-02 05:00:00+00:00
GC=F,2082.699951171875,USD,2024-01-02 06:00:00+00:00
GC=F,2082.10009765625,USD,2024-01-02 07:00:00+00:00
GC=F,2082.199951171875,USD,2024-01-02 08:00:00+00:00
```

### 🔄 **Mapeamento de Símbolos nos Dados**

| CSV Original | Símbolo Original | Símbolo Padronizado |
|--------------|------------------|---------------------|
| **transaction_btc** | BTC | BTC |
| **transaction_commodities** | GOLD | GOLD |
| **transaction_commodities** | OIL | OIL |
| **transaction_commodities** | SILVER | SILVER |
| **quotation_btc** | BTC-USD | BTC |
| **quotation_yfinance** | GC=F | GOLD |
| **quotation_yfinance** | CL=F | OIL |
| **quotation_yfinance** | SI=F | SILVER |

### ✅ **Características Implementadas**
- **Tipo**: `CREATE OR REFRESH STREAMING TABLE`
- **Ingestão**: Incremental via `cloud_files()`
- **Schema**: Inferência automática
- **Timestamp**: `current_timestamp() as ingested_at`

---

## 🥈 5. Camada Silver - Transformação

### 📋 **Tabelas Implementadas (4 tabelas)**

### 5.1 `silver.fact_transaction_assets`

**🔁 Transformações Implementadas:**
- **União**: `transaction_btc` + `transaction_commodities`
- **Casting**: `CAST(data_hora AS TIMESTAMP)`
- **Hora Aproximada**: `date_trunc('hour', CAST(data_hora AS TIMESTAMP))`
- **Símbolo Padronizado**: Mapeamento unificado de ativos

**🎯 Mapeamento de Símbolos:**
```sql
CASE 
  WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('BTC','BTC-USD') THEN 'BTC'
  WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('GOLD','GC=F')   THEN 'GOLD'
  WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('OIL','CL=F')    THEN 'OIL'
  WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('SILVER','SI=F') THEN 'SILVER'
  ELSE 'UNKNOWN'
END AS asset_symbol
```

**🔒 Constraints Implementados:**
- `quantidade > 0`
- `data_hora IS NOT NULL`
- `tipo_operacao IN ('COMPRA','VENDA')`
- `asset_symbol IN ('BTC','GOLD','OIL','SILVER')`

### 5.2 `silver.fact_quotation_assets`

**🔁 Transformações Implementadas:**
- **União**: `quotation_btc` + `quotation_yfinance`
- **Casting**: `CAST(horario_coleta AS TIMESTAMP)`
- **Hora Aproximada**: `date_trunc('hour', CAST(horario_coleta AS TIMESTAMP))`
- **Símbolo Padronizado**: Mesmo mapeamento da tabela de transações

**🔒 Constraints Implementados:**
- `preco > 0`
- `horario_coleta <= current_timestamp()`
- `ativo IS NOT NULL AND ativo != ''`
- `moeda = 'USD'`

### 5.3 `silver.dim_clientes`

**🔁 Transformações Implementadas:**
- **Anonimização**: `SHA2(documento, 256) as documento_hash`
- **Validação**: Segmentos, países e estados válidos

**🔒 Constraints Implementados:**
- `customer_id IS NOT NULL`
- `segmento IN ('Financeiro', 'Indústria', 'Varejo', 'Tecnologia')`
- `pais IN ('Brasil', 'Alemanha', 'Estados Unidos')`
- Validação de estados por país

### 5.4 `silver.fact_transaction_revenue`

**🔁 Transformações Implementadas:**
- **Join**: Transações + Cotações + Clientes
- **Join Logic**: `q.data_hora_aproximada = t.data_hora_aproximada`
- **Cálculos Financeiros**:
  - `gross_value = quantidade × preço`
  - `gross_value_sinal = VENDA(+) / COMPRA(-)`
  - `fee_revenue = gross_value × 0.25%`

**💰 Lógica do Sinal Implementada:**
```sql
CASE 
  WHEN t.tipo_operacao = 'VENDA' THEN (t.quantidade * q.preco)
  WHEN t.tipo_operacao = 'COMPRA' THEN -(t.quantidade * q.preco)
  ELSE 0
END as gross_value_sinal
```

**🔒 Constraints Implementados:**
- `gross_value > 0`
- `fee_revenue > 0`
- `customer_sk IS NOT NULL`
- `preco_cotacao > 0 AND timestamp_cotacao <= data_hora`

---

## 🥇 6. Camada Gold - Métricas de Negócio

### 📋 **Tabela Implementada (1 tabela)**

### 6.1 `gold.mostvaluableclient`

**📊 Métricas Implementadas:**
- `total_transacoes`: COUNT(*) de transações por cliente
- `valor_total`: SUM(gross_value) - valor total das transações
- `ticket_medio`: AVG(gross_value) - valor médio por transação
- `primeira_transacao`: MIN(data_hora) - primeira transação do cliente
- `ultima_transacao`: MAX(data_hora) - última transação do cliente
- `transacoes_ultimos_30_dias`: COUNT com filtro de 30 dias baseado na data máxima da tabela - frequência recente
- `comissao_total`: SUM(fee_revenue) - receita total de taxas
- `ranking_por_transacoes`: RANK() baseado no número de transações
- `classificacao_cliente`: Classificação Top 1/2/3 ou Outros
- `calculated_at`: Timestamp de cálculo

**🏆 Segmentação Implementada:**
- **Ranking**: Baseado em `COUNT(*)` (número total de transações)
- **Top 1**: Cliente com mais transações (RANK = 1)
- **Top 2**: Cliente com segunda maior quantidade (RANK = 2)
- **Top 3**: Cliente com terceira maior quantidade (RANK = 3)
- **Outros**: Demais clientes (RANK > 3)

**🔒 Lógica de Classificação:**
```sql
CASE 
  WHEN RANK() OVER (ORDER BY COUNT(*) DESC) = 1 THEN 'Top 1'
  WHEN RANK() OVER (ORDER BY COUNT(*) DESC) = 2 THEN 'Top 2'
  WHEN RANK() OVER (ORDER BY COUNT(*) DESC) = 3 THEN 'Top 3'
  ELSE 'Outros'
END AS classificacao_cliente
```

**📈 Ordenação:**
- Resultados ordenados por `total_transacoes DESC` (maior para menor)

**⏰ Lógica dos Últimos 30 Dias:**
```sql
COUNT(CASE 
  WHEN data_hora >= (SELECT MAX(data_hora) FROM lakehouse.silver.fact_transaction_revenue) - INTERVAL 30 DAYS THEN 1 
END) AS transacoes_ultimos_30_dias
```
- **Base**: Data máxima da tabela `fact_transaction_revenue` (não `current_timestamp()`)
- **Vantagem**: Análise consistente baseada nos dados disponíveis
- **Período**: 30 dias antes da última transação registrada

---

## ⚙️ 7. Configurações Técnicas Implementadas

### 🔄 **Streaming e Incremental Processing**

**TODAS as tabelas Silver e Gold utilizam:**
- **Tipo**: `CREATE OR REFRESH STREAMING TABLE` (não MATERIALIZED VIEW)
- **Fonte**: `FROM STREAM(tabela_origem)`
- **Benefício**: Processamento incremental e evita erros de batch query

### 🔒 **Data Quality com Constraints**

**Sintaxe Oficial Implementada:**
```sql
CONSTRAINT nome_valid EXPECT (condicao) ON VIOLATION DROP ROW
```

**Ações de Violação:**
- `ON VIOLATION DROP ROW`: Remove registros inválidos
- Logs automáticos de violações
- Monitoramento via UI do Databricks

### 🛡️ **Segurança e Anonimização**

- **PII**: `SHA2(documento, 256)` para documentos sensíveis
- **Governança**: Unity Catalog
- **Auditoria**: Lakeflow Lineage completo

### 📊 **Volumes e Ingestão**

- **Formato**: CSV com `header=true` e `inferSchema=true`
- **Caminhos**: `/Volumes/lakehouse/raw_public/[arquivo]`
- **Incremental**: `cloud_files()` para versionamento automático

---

## 🎯 8. Resultados e Métricas de Negócio

### 📈 **Métricas Financeiras Implementadas**

1. **Valor Total**: Soma de todas as transações (gross_value)
2. **Receita de Taxa**: 0.25% sobre valor total (fee_revenue)
3. **Ticket Médio**: Valor médio por transação
4. **Frequência**: Transações nos últimos 30 dias
5. **Volume de Transações**: Número total de transações por cliente

### 🏆 **Segmentação de Clientes**

- **Top 1**: Cliente com maior número de transações
- **Top 2**: Cliente com segunda maior quantidade de transações
- **Top 3**: Cliente com terceira maior quantidade de transações
- **Outros**: Demais clientes (RANK > 3)

### 📊 **Análises Disponíveis**

- Ranking por número de transações
- Análise de frequência de transações (últimos 30 dias)
- Identificação de clientes mais ativos (Top 1, 2, 3)
- Métricas de receita por cliente (valor total e taxas)
- Análise temporal (primeira e última transação)
- Segmentação por volume de atividade

---

## ✅ 9. Status de Implementação

### 🎉 **Pipeline 100% Funcional**

- ✅ **5 tabelas Bronze** com ingestão incremental
- ✅ **4 tabelas Silver** com transformações e data quality
- ✅ **1 tabela Gold** com métricas de segmentação
- ✅ **Streaming incremental** funcionando perfeitamente
- ✅ **Join por hora aproximada** resolvendo matching
- ✅ **Símbolos padronizados** (BTC, GOLD, OIL, SILVER)
- ✅ **Métricas financeiras** (valor total, taxas, ticket médio)
- ✅ **Ranking por volume** de transações (Top 1, 2, 3)
- ✅ **Constraints e data quality** implementados
- ✅ **Anonimização** de dados sensíveis
- ✅ **Documentação completa** e atualizada

### 🚀 **Pronto para Produção**

O pipeline está totalmente implementado, testado e funcionando. Todas as transformações, joins, métricas e segmentações estão operacionais e prontas para uso em produção.

---

## 📚 10. Documentação Técnica

### 🔗 **Referências**
- [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/)
- [Data Quality Expectations](https://docs.databricks.com/aws/en/dlt/expectations?language=SQL)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)

### 📁 **Arquivos de Referência**
- `README.md`: Documentação técnica completa
- `requisitos.md`: Este documento de requisitos
- Códigos SQL: Implementação completa em `/transformations/`

---

**🎯 Pipeline de Segmentação de Clientes - IMPLEMENTADO E FUNCIONANDO** ✅