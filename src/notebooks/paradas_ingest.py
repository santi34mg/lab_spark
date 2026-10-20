# Databricks notebook source
# MAGIC %md
# MAGIC # Paradas Ingest
# MAGIC Carga el shapefile de paradas STM (extraído por `paradas_unzip`) a la tabla bronce.
# MAGIC
# MAGIC Entrada: `/Volumes/<catalog>/<schema>/<volume>/<shp_subdir>/<nombre_zip>/*.shp` (`shp_subdir` por defecto `shp`)
# MAGIC Salida: `<catalog>.<schema>.<table>` (atributos del `.dbf` + `lon`, `lat` y `geometry_wkt` en WGS 84)
# MAGIC
# MAGIC Son ~5k puntos: se leen con GeoPandas en el driver, se reproyectan de UTM 21S (EPSG:32721) a
# MAGIC WGS 84 (EPSG:4326) y se pasan a Spark como pandas DataFrame, con la geometría en WKT.
# MAGIC El shapefile es una foto completa: cada corrida sobreescribe la tabla.

# COMMAND ----------

import glob

import geopandas as gpd
import pyogrio

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
table = dbutils.widgets.get("table")
dbutils.widgets.text("shp_subdir", "shp")
shp_subdir = dbutils.widgets.get("shp_subdir")

base_dir = f"/Volumes/{catalog}/{schema}/{volume}"
target_table = f"{catalog}.{schema}.{table}"

# Destino de la reproyección: lon/lat, lo que esperan los mapas y las funciones ST_ de Databricks
TARGET_CRS = "EPSG:4326"

# El .cst de GeoServer dice UTF-8, pero GDAL solo mira el .cpg: sin forzar el encoding lee latin-1
# y los acentos quedan mal (ej. FARIÃ‘A). fcrea y fact traen bytes binarios que no son texto válido
# en ningún encoding, así que se descartan.
BINARY_FIELDS = {"fcrea", "fact"}

# COMMAND ----------

shp_files = sorted(glob.glob(f"{base_dir}/{shp_subdir}/*/*.shp"))
if len(shp_files) != 1:
    raise FileNotFoundError(
        f"Se esperaba un .shp en {base_dir}/{shp_subdir}/ y hay {len(shp_files)}: {shp_files}. "
        "¿Se ejecutó paradas_unzip?"
    )
shp_path = shp_files[0]

fields = [f for f in pyogrio.read_info(shp_path, encoding="utf-8")["fields"] if f not in BINARY_FIELDS]
gdf = gpd.read_file(shp_path, encoding="utf-8", columns=fields)
print(f"{shp_path}: {len(gdf)} features, CRS {gdf.crs}, geometrías {gdf.geom_type.unique().tolist()}")

# COMMAND ----------

gdf = gdf.to_crs(TARGET_CRS)

# Spark no entiende la columna geometry de GeoPandas: se pasa como texto WKT (ST_GeomFromWKT la reconstruye)
pdf = gdf.drop(columns=gdf.geometry.name).assign(
    lon=gdf.geometry.x,
    lat=gdf.geometry.y,
    geometry_wkt=gdf.geometry.to_wkt(),
)

# COMMAND ----------

spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
print(f"{len(pdf)} paradas -> {target_table}")
