# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingest
# MAGIC Carga incremental con Auto Loader de los CSVs de viajes STM (generados por `os_unzip`) a la tabla bronce.
# MAGIC
# MAGIC Entrada: `/Volumes/<catalog>/<schema>/<volume>/csv/<mes>/*.csv`
# MAGIC Salida: `<catalog>.<schema>.<table>` (todas las columnas `string`, crudo)
# MAGIC
# MAGIC Usa `trigger(availableNow=True)`: procesa los archivos nuevos desde la última corrida y termina.
# MAGIC El checkpoint lleva el registro de los archivos ya cargados, por lo que re-ejecutar no duplica datos.

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
table = dbutils.widgets.get("table")

base_dir = f"/Volumes/{catalog}/{schema}/{volume}"
source_dir = f"{base_dir}/csv"
checkpoint_dir = f"{base_dir}/_checkpoints/{table}"
target_table = f"{catalog}.{schema}.{table}"

# Cabecera de los CSVs (igual en todos los meses). Todo string en bronce: el formato de
# fecha_evento cambia entre años (ej. 2020-01-24T19:00:06.000-03:00 vs 2023-01-01 21:23:55)
COLUMNS = [
    "id_viaje",
    "con_tarjeta",
    "fecha_evento",
    "tipo_viaje",
    "descripcion_tipo_viaje",
    "grupo_usuario",
    "descripcion_grupo_usuario",
    "grupo_usuario_especifico",
    "descripcion_grupo_usuario_espe",
    "ordinal_de_tramo",
    "cantidad_pasajeros",
    "codigo_parada_origen",
    "cod_empresa",
    "descrip_empresa",
    "linea_codigo",
    "dsc_linea",
    "sevar_codigo",
]
csv_schema = StructType([StructField(c, StringType()) for c in COLUMNS])

# Cada CSV pesa 1-3 GB: micro-batches chicos para que un fallo no pierda todo el progreso
MAX_FILES_PER_TRIGGER = 4

# COMMAND ----------

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    # os_unzip escribe <archivo>.csv.part y después renombra: solo se toman los .csv terminados
    .option("pathGlobFilter", "*.csv")
    .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .schema(csv_schema)
    .load(source_dir)
)

query = stream.writeStream.option("checkpointLocation", checkpoint_dir).trigger(availableNow=True).toTable(target_table)
query.awaitTermination()

# COMMAND ----------

batches = [p for p in query.recentProgress if p["numInputRows"] > 0]
print(f"Micro-batches con datos: {len(batches)}, filas cargadas: {sum(p['numInputRows'] for p in batches)}")
