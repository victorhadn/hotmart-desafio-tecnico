# Desafio Técnico - Analytics Engineer
- Repositório criado para armazenar os scripts em SQL e Python do teste técnico para a vaga de Senior Analytics Engineer

## Stack Técnica

- Banco de Origem - PostgreSQL (origem das tabelas purchase, product_item, purchase_extra_info)
- Processamento - PySpark (uso de cluster, ETL distribuído, agrega e grava a tabela final)
- Armazenamento - Delta Lake em S3 ou Databricks (armazena histórico e permite versionamento de código)
- Consumo - Athena/Redshift (queries de consulta do GMV diário)

## Vantagens da modelagem

- Imútavel: cada execução D-1 grava uma nova linha sem sobrescrever o passado.
- Histórica: permite ver o GMV em qualquer data de processamento.
- Performática: partição pelo campo TRANSACTION_DATE e chave composta evitando duplicidades.
- Simples de manter: leitura das tabelas fonte e gravação diária dos dados.
- Rastreável: com as colunas PROCESSING_DATE e CREATED_AT fica possível auditar quando cada registro foi processado.
- Flexível: se precisar reprocessar um dia específico, podemos usar a TRANSACTION_DATE sem afetar os registros passados.
- Escalável: Spark + Delta Lake trabalham bem juntos com milhões de transações diárias.

### Exemplo do Dataset: FACT_DAILY_GMV_SUBSIDIARY:

| TRANSACTION_DATE | SUBSIDIARY_ID | GMV_AMOUNT | TRANSACTION_COUNT | PROCESSING_DATE | CREATED_AT    |
|-----------------|---------------|------------|-----------------|----------------|-------------------|
| 2025-10-15      | 001           | 12.500,50  | 120             | 2025-10-16     | 2025-10-16 08:00:00 |
| 2025-10-15      | 002           | 9.800,75   | 95              | 2025-10-16     | 2025-10-16 08:00:00 |
| 2025-10-15      | 003           | 15.230,00  | 140             | 2025-10-16     | 2025-10-16 08:00:00 |
| 2025-10-16      | 001           | 13.450,25  | 130             | 2025-10-17     | 2025-10-17 08:00:00 |
| 2025-10-16      | 002           | 11.020,00  | 100             | 2025-10-17     | 2025-10-17 08:00:00 |
| 2025-10-16      | 003           | 14.500,75  | 135             | 2025-10-17     | 2025-10-17 08:00:00 |
