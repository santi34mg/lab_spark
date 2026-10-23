# Databricks notebook source
# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("schema")
silver_schema = dbutils.widgets.get("silver_schema")

spark.sql(f"USE CATALOG `{catalog}`")

# COMMAND ----------

df_bronze = spark.read.table(f"`{catalog}`.`{bronze_schema}`.horarios_variante")
display(df_bronze.limit(10))

# COMMAND ----------

nulos_bronze = df_bronze.select([
    F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)).alias(c)
    for c in df_bronze.columns
])
display(nulos_bronze)

# COMMAND ----------

df_silver = (
    df_bronze
    .withColumn("tipo_dia", F.trim(F.col("tipo_dia")).cast("int"))
    .withColumn("cod_variante", F.trim(F.col("cod_variante")).cast("int"))
    .withColumn("frecuencia", F.trim(F.col("frecuencia")).cast("int"))
    .withColumn("cod_ubic_parada", F.trim(F.col("cod_ubic_parada")).cast("int"))
    .withColumn("ordinal", F.trim(F.col("ordinal")).cast("int"))
    .withColumn("hora", F.trim(F.col("hora")).cast("int"))
    .withColumn("dia_anterior", F.upper(F.trim(F.col("dia_anterior"))))
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "dia_anterior",
    F.when(F.col("dia_anterior") == "", None).otherwise(F.col("dia_anterior")),
)
df_silver = df_silver.fillna({"dia_anterior": "N"})

columnas_obligatorias = [
    "tipo_dia",
    "cod_variante",
    "frecuencia",
    "cod_ubic_parada",
    "ordinal",
    "hora",
]

df_silver = (
    df_silver
    .dropna(subset=columnas_obligatorias)
    .filter(
        F.col("tipo_dia").isin(1, 2, 3)
        & (F.col("ordinal") >= 1)
        & (F.col("hora") >= 0)
    )
    .dropDuplicates(["tipo_dia", "cod_variante", "frecuencia", "cod_ubic_parada", "ordinal"])
)

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{catalog}`.`{silver_schema}`.horarios_variante")
)

