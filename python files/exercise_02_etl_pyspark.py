from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, lit, current_timestamp, current_date, date_sub
)

# Criando a sessão Spark
spark = (
    SparkSession.builder
    .appName("ETL_FACT_DAILY_GMV_SUBSIDIARY")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

# Fazendo a leitura das tabelas do banco (via JDBC)
jdbc_url = "jdbc:postgresql://analyticshotmart:5432/financial_database"
properties = {
    "user": "analytics_engineer",
    "password": "me_contrata_hotmart",
    "driver": "org.postgresql.Driver"
}

purchase = spark.read.jdbc(jdbc_url, "purchase", properties=properties)
product_item = spark.read.jdbc(jdbc_url, "product_item", properties=properties)
purchase_extra = spark.read.jdbc(jdbc_url, "purchase_extra_info", properties=properties)


# Filtrando apenas compras com pagamento efetuado
purchase_paid = purchase.filter(col("release_date").isNotNull())


# Join entre as tabelas mais relevantes
df_joined = (
    purchase_paid.alias("p")
    .join(product_item.alias("pi"), col("p.purchase_id") == col("pi.purchase_id"), "inner")
    .join(purchase_extra.alias("pe"), col("p.purchase_id") == col("pe.purchase_id"), "inner")
    .select(
        col("p.transaction_date"),
        col("pe.subsidiary").alias("subsidiary_id"),
        col("p.purchase_id"),
        col("pi.purchase_value")
    )
)

# Agregando GMV diário por tipo de subsidiária (nacional/internacional)
processing_date = date_sub(current_date(), 1)
fact_gmv = (
    df_joined.groupBy("transaction_date", "subsidiary_id")
    .agg(
        _sum("purchase_value").alias("gmv_amount"),
        countDistinct("purchase_id").alias("transaction_count")
    )
    .withColumn("processing_date", lit(processing_date))
    .withColumn("created_at", current_timestamp())
)

# Escrevendo a tabela final em formato Delta
(
    fact_gmv.write
    .format("delta")
    .mode("overwrite") 
    .option("replaceWhere", f"transaction_date = '{processing_date}'")  
    .partitionBy("transaction_date")
    .saveAsTable("FACT_DAILY_GMV_SUBSIDIARY")
)
