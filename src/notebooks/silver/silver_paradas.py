# Databricks notebook source
# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("schema")
silver_schema = dbutils.widgets.get("silver_schema")

spark.sql(f"USE CATALOG `{catalog}`")

# COMMAND ----------

df_bronze = spark.read.table(f"`{catalog}`.`{bronze_schema}`.paradas")
display(df_bronze.limit(10))

# COMMAND ----------

nulos_bronze = df_bronze.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_bronze.columns
])
display(nulos_bronze)

# COMMAND ----------

df_silver = df_bronze.toDF(*[c.lower() for c in df_bronze.columns])

if "cod_ubic_p" in df_silver.columns:
    df_silver = df_silver.withColumnRenamed("cod_ubic_p", "cod_ubic_parada")

cols_auditoria = [c for c in ["ucrea", "uact", "idauditori", "idauditoria"] if c in df_silver.columns]
if cols_auditoria:
    df_silver = df_silver.drop(*cols_auditoria)

# COMMAND ----------

for col_name, dtype in df_silver.dtypes:
    if dtype == "string" and col_name != "geometry_wkt":
        df_silver = df_silver.withColumn(col_name, F.trim(F.col(col_name)))
        df_silver = df_silver.withColumn(
            col_name,
            F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)),
        )

# aseguramos los tipos de las columnas clave y coordenadas
df_silver = (
    df_silver
    .withColumn("cod_ubic_parada", F.col("cod_ubic_parada").cast("int"))
    .withColumn("lon", F.col("lon").cast("double"))
    .withColumn("lat", F.col("lat").cast("double"))
)

# descartamos filas sin codigo de parada o sin coordenadas validas y eliminamos duplicados
cols_clave = ["cod_ubic_parada", "lon", "lat", "geometry_wkt"]
df_silver = (
    df_silver
    .dropna(subset=cols_clave)
    .dropDuplicates()
    .withColumn("geometry", F.expr("ST_GeomFromWKT(geometry_wkt, 4326)"))
    .drop("geometry_wkt")
)

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{catalog}`.`{silver_schema}`.paradas")
)

