# Databricks notebook source
# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
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
    # Hora local de Montevideo. Hasta 2022-08 viene como 2020-01-24T19:00:06.000-03:00 y desde 2022-09 como
    # 2023-01-01 21:23:55: se parsean los primeros 19 caracteres (fecha y hora local, sin el offset) para que
    # los dos formatos queden en la misma escala. Interpretar el -03:00 pasaba esos años a UTC y corría
    # 3 horas toda la distribución horaria anterior a 2022-09.
    .withColumn(
        "fecha_evento",
        F.try_to_timestamp(
            F.regexp_replace(F.substring(F.col("fecha_evento"), 1, 19), "T", " "),
            F.lit("yyyy-MM-dd HH:mm:ss"),
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
# Además de vacíos, la fuente trae "\N" como texto (descripción de grupo 12) y espacios dobles
for col in cols_texto:
    limpio = F.regexp_replace(F.col(col), r"\s+", " ")
    df_silver = df_silver.withColumn(col, F.when(limpio.isin("", "\\N"), None).otherwise(limpio))


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

# ordinal_de_tramo 0 = boleto en efectivo (con_tarjeta = false, ~11% de los tramos): es un viaje válido
df_silver = (
    df_silver
    .dropna(subset=columnas_obligatorias)
    .filter((F.col("cantidad_pasajeros") > 0) & (F.col("ordinal_de_tramo") >= 0))
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

