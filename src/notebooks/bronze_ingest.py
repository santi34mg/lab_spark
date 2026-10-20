# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingest
# MAGIC Carga incremental con Auto Loader de los CSVs STM (generados por `os_unzip`) a una tabla bronce.
# MAGIC Lo usan `bronze_ingest` (viajes) y `horarios_ingest` (horarios_variante y horarios_circular).
# MAGIC
# MAGIC Entrada: `/Volumes/<catalog>/<schema>/<volume>/<source_subdir>/**/*.csv` (`source_subdir` por defecto `csv`)
# MAGIC Salida: `<catalog>.<schema>.<table>` (todas las columnas `string`, crudo)
# MAGIC
# MAGIC Las columnas y el separador de cada tabla están en `SOURCES`, indexado por `table`.
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
# viajes lee todo csv/ (un subdirectorio por mes); cada CSV de horarios tiene su subdirectorio y su esquema
dbutils.widgets.text("source_subdir", "csv")
source_subdir = dbutils.widgets.get("source_subdir")

base_dir = f"/Volumes/{catalog}/{schema}/{volume}"
source_dir = f"{base_dir}/{source_subdir}"
checkpoint_dir = f"{base_dir}/_checkpoints/{table}"
target_table = f"{catalog}.{schema}.{table}"

# Cabecera de los CSVs de cada tabla. Todo string en bronce.
SOURCES = {
    # Igual en todos los meses. El formato de fecha_evento cambia entre años
    # (ej. 2020-01-24T19:00:06.000-03:00 vs 2023-01-01 21:23:55)
    "viajes": {
        "sep": ",",
        "columns": [
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
        ],
    },
    # uptu_pasada_variante.csv: pasadas teóricas por parada de cada variante
    "horarios_variante": {
        "sep": ";",
        "columns": ["tipo_dia", "cod_variante", "frecuencia", "cod_ubic_parada", "ordinal", "hora", "dia_anterior"],
    },
    # uptu_pasada_circular.csv: los valores vienen con espacio y ceros a la izquierda (ej. " 0001")
    "horarios_circular": {
        "sep": ";",
        "columns": [
            "tipo_dia",
            "cod_circular",
            "frecuencia",
            "cod_ubic_parada",
            "cod_variante",
            "parte",
            "ordinal",
            "hora",
            "dia_anterior",
        ],
    },
}
if table not in SOURCES:
    raise ValueError(f"Tabla {table!r} sin esquema definido. Opciones: {list(SOURCES)}")
source = SOURCES[table]
csv_schema = StructType([StructField(c, StringType()) for c in source["columns"]])

# Cada CSV de viajes pesa 1-3 GB: micro-batches chicos para que un fallo no pierda todo el progreso
MAX_FILES_PER_TRIGGER = 4

# COMMAND ----------

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("sep", source["sep"])
    # os_unzip escribe <archivo>.csv.part y después renombra: solo se toman los .csv terminados
    .option("pathGlobFilter", "*.csv")
    .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .schema(csv_schema)
    .load(source_dir)
)

query = stream.writeStream.option("checkpointLocation", checkpoint_dir).trigger(availableNow=True).toTable(target_table)
query.awaitTermination()
