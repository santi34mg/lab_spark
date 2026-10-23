# Databricks notebook source
# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("schema")
silver_schema = dbutils.widgets.get("silver_schema")

spark.sql(f"USE CATALOG `{catalog}`")

# COMMAND ----------

df_bronze = spark.read.table(f"`{catalog}`.`{bronze_schema}`.viajes")
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
    .withColumn("id_viaje", F.col("id_viaje").cast("long"))
    .withColumn(
        "con_tarjeta",
        F.when(F.trim(F.col("con_tarjeta")) == "1", True)
        .when(F.trim(F.col("con_tarjeta")) == "0", False)
        .otherwise(None),
    )
    .withColumn(
        "fecha_evento",
        F.coalesce(
            F.try_to_timestamp(F.col("fecha_evento"), F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp(F.col("fecha_evento"), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")),
            F.try_to_timestamp(F.col("fecha_evento")),
        ),
    )
    .withColumn("tipo_viaje", F.trim(F.col("tipo_viaje")))
    .withColumn("descripcion_tipo_viaje", F.trim(F.col("descripcion_tipo_viaje")))
    .withColumn("grupo_usuario", F.trim(F.col("grupo_usuario")))
    .withColumn("descripcion_grupo_usuario", F.trim(F.col("descripcion_grupo_usuario")))
    .withColumn("grupo_usuario_especifico", F.trim(F.col("grupo_usuario_especifico")))
    .drop("descripcion_grupo_usuario_espe")
    .withColumn("ordinal_de_tramo", F.col("ordinal_de_tramo").cast("int"))
    .withColumn("cantidad_pasajeros", F.col("cantidad_pasajeros").cast("int"))
    .withColumn("codigo_parada_origen", F.col("codigo_parada_origen").cast("int"))
    .withColumn("cod_empresa", F.col("cod_empresa").cast("int"))
    .withColumn("descrip_empresa", F.trim(F.col("descrip_empresa")))
    .withColumn("linea_codigo", F.col("linea_codigo").cast("int"))
    .withColumn("dsc_linea", F.trim(F.col("dsc_linea")))
    .withColumn("sevar_codigo", F.col("sevar_codigo").cast("int"))
)

# COMMAND ----------

cols_texto = [
    "tipo_viaje",
    "descripcion_tipo_viaje",
    "grupo_usuario",
    "descripcion_grupo_usuario",
    "grupo_usuario_especifico",
    "descrip_empresa",
    "dsc_linea",
]
for col in cols_texto:
    df_silver = df_silver.withColumn(col, F.when(F.col(col) == "", None).otherwise(F.col(col)))


df_silver = df_silver.fillna({
    "ordinal_de_tramo": 1,
    "grupo_usuario_especifico": "SIN ESPECIFICAR",
})


columnas_obligatorias = [
    "id_viaje",
    "con_tarjeta",
    "fecha_evento",
    "cantidad_pasajeros",
    "codigo_parada_origen",
    "cod_empresa",
    "linea_codigo",
    "sevar_codigo",
]

df_silver = (
    df_silver
    .dropna(subset=columnas_obligatorias)
    .filter((F.col("cantidad_pasajeros") > 0) & (F.col("ordinal_de_tramo") >= 1))
    .dropDuplicates(["id_viaje", "ordinal_de_tramo", "fecha_evento"])
)

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{catalog}`.`{silver_schema}`.viajes")
)

