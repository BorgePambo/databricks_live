# Pipeline de Segmentação de Clientes - Lakehouse Flow

## 📋 Visão Geral

Este pipeline implementa uma solução completa de ETL usando **Lakehouse Flow (Delta Live Tables)** do Databricks para segmentação de clientes baseada em comportamento transacional.

## 🏗️ Arquitetura

```
📁 Volumes CSV → 🥉 Bronze → 🥈 Silver → 🥇 Gold
```

### Camadas do Pipeline

1. **Bronze**: Ingestão incremental de dados brutos dos Volumes
2. **Silver**: Transformações, normalização e data quality
3. **Gold**: Agregações de métricas de negócio e segmentação

## 📁 Estrutura de Arquivos

```
Pipeline_live/
├── bronze/
│   ├── customers.sql                  # Ingestão de clientes
│   ├── transacation_btc.sql           # Ingestão de transações Bitcoin
│   ├── transaction_commodities.sql    # Ingestão de transações Commodities
│   ├── quotation_btc.sql              # Ingestão de cotações Bitcoin
│   └── quotation_yfinance.sql        # Ingestão de cotações yFinance
├── silver/
│   ├── fact_transaction_assets.sql    # Fato unificado de transações
│   ├── fact_quotation_assets.sql      # Fato unificado de cotações
│   ├── dim_clientes.sql               # Dimensão de clientes (anonimizada)
│   └── fact_transaction_revenue.sql  # Fato de receita por transação
└── gold/
    └── mostvaluableclient.sql         # Segmentação de clientes mais valiosos
```

## 🥉 Camada Bronze

### Objetivo
Ingestão incremental de dados brutos preservando a estrutura original dos CSVs.

### Características
- **Tipo**: `CREATE OR REFRESH STREAMING TABLE`
- **Fonte**: Volumes do Databricks (`/Volumes/lakehouse_live/raw_public/[arquivo]`)
- **Formato**: CSV com header e schema inferido automaticamente
- **Processamento**: Incremental via `cloud_files()`
- **Timestamp**: Campo `ingested_at` registra quando o dado foi processado

### Tabelas Bronze

| Tabela | Volume CSV | Descrição |
|--------|------------|-----------|
| `bronze.customers` | `/Volumes/lakehouse_live/raw_public/customers` | Dados de clientes |
| `bronze.transaction_btc` | `/Volumes/lakehouse_live/raw_public/transacation_btc` | Transações Bitcoin |
| `bronze.transaction_commodities` | `/Volumes/lakehouse_live/raw_public/transaction_commodities` | Transações Commodities |
| `bronze.quotation_btc` | `/Volumes/lakehouse_live/raw_public/quotation_btc` | Cotações Bitcoin |
| `bronze.quotation_yfinance` | `/Volumes/lakehouse_live/raw_public/quotation_yfinance` | Cotações yFinance |

## 🥈 Camada Silver

### Objetivo
Transformação e normalização dos dados com validações de qualidade.

### Tabelas Silver

#### 1. `silver.fact_transaction_assets`
- **Fonte**: `bronze.transaction_btc` + `bronze.transaction_commodities`
- **Transformações**:
  - União de transações BTC e Commodities
  - Padronização de símbolos (BTC, GOLD, OIL, SILVER)
  - Normalização de timestamps
  - Hora aproximada para join com cotações
- **Constraints**: Quantidade > 0, data_hora NOT NULL, tipo_operacao válido, asset_symbol válido

#### 2. `silver.fact_quotation_assets`
- **Fonte**: `bronze.quotation_btc` + `bronze.quotation_yfinance`
- **Transformações**:
  - União de cotações BTC e yFinance
  - Padronização de símbolos
  - Normalização de timestamps
  - Hora aproximada para join com transações
- **Constraints**: Preço > 0, horário válido, ativo NOT NULL, moeda = USD

#### 3. `silver.dim_clientes`
- **Fonte**: `bronze.customers`
- **Transformações**:
  - Anonimização de documentos (SHA2)
  - Validação de segmentos, países e estados
  - Criação de surrogate key (customer_sk)
- **Constraints**: customer_id NOT NULL, segmento válido, país válido

#### 4. `silver.fact_transaction_revenue`
- **Fonte**: `fact_transaction_assets` + `fact_quotation_assets` + `dim_clientes`
- **Transformações**:
  - Join de transações com cotações (por hora aproximada e símbolo)
  - Join com dimensão de clientes
  - Cálculo de valor bruto (quantidade × preço)
  - Cálculo de receita de taxa (0.25% sobre valor bruto)
  - Aplicação de sinal para COMPRA (-) e VENDA (+)
- **Constraints**: gross_value > 0, fee_revenue > 0, customer_sk NOT NULL, cotação válida

## 🥇 Camada Gold

### Objetivo
Agregação de métricas de negócio e segmentação de clientes.

### Tabela Gold

#### `gold.mostvaluableclient`
- **Fonte**: `silver.fact_transaction_revenue`
- **Métricas Calculadas**:
  - Total de transações por cliente
  - Valor total das transações
  - Ticket médio
  - Primeira e última transação
  - Transações nos últimos 30 dias
  - Receita total de taxas (comissões)
  - Ranking por volume de transações
  - Classificação: Top 1, Top 2, Top 3 ou Outros

## 🔄 Processamento Incremental

Todas as tabelas Silver e Gold utilizam:
- **Tipo**: `CREATE OR REFRESH STREAMING TABLE`
- **Fonte**: `FROM STREAM(tabela_origem)`
- **Benefício**: Processamento incremental e evita erros de batch query

## 🔒 Data Quality

### Constraints Implementados

**Sintaxe Oficial:**
```sql
CONSTRAINT nome_valid EXPECT (condicao) ON VIOLATION DROP ROW
```

**Ações de Violação:**
- `ON VIOLATION DROP ROW`: Remove registros inválidos
- Logs automáticos de violações
- Monitoramento via UI do Databricks

## 🛡️ Segurança

- **PII**: Documentos anonimizados com `SHA2(documento, 256)`
- **Governança**: Unity Catalog
- **Auditoria**: Lakeflow Lineage completo

## 📊 Mapeamento de Símbolos

| CSV Original | Símbolo Original | Símbolo Padronizado |
|--------------|------------------|---------------------|
| transaction_btc | BTC | BTC |
| transaction_commodities | GOLD | GOLD |
| transaction_commodities | OIL | OIL |
| transaction_commodities | SILVER | SILVER |
| quotation_btc | BTC-USD | BTC |
| quotation_yfinance | GC=F | GOLD |
| quotation_yfinance | CL=F | OIL |
| quotation_yfinance | SI=F | SILVER |

## 🚀 Como Usar

1. **Configure os Volumes**: Certifique-se de que os arquivos CSV estão nos Volumes especificados
2. **Crie o Pipeline**: No Databricks, crie um novo Pipeline usando Lakehouse Flow
3. **Configure o Caminho**: Aponte para a pasta `Pipeline_live` ou suas subpastas
4. **Execute**: Execute o pipeline e monitore via UI do Databricks

## 📚 Referências

- [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/)
- [Data Quality Expectations](https://docs.databricks.com/aws/en/dlt/expectations?language=SQL)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)

---

**Desenvolvido para Jornada de Dados - Pipeline de Segmentação de Clientes**

