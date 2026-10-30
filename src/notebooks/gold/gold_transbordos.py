# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Transbordos
# MAGIC
# MAGIC | Tabla | Grano | Para qué |
# MAGIC |---|---|---|
# MAGIC | `transbordos_mensual` | mes | % de viajes con tarjeta que hacen al menos un transbordo (toda la historia) |
# MAGIC | `transbordos_minutos` | mes × tramo de 5 min × misma línea | minutos entre subidas consecutivas de un viaje |
# MAGIC | `transbordos_pares` | línea origen × línea destino | corredores de transbordo y su parada principal |
# MAGIC
# MAGIC Los viajes se reconstruyen desde los tramos con una ventana por viaje: `lag()` sobre `(mes, id_viaje)` ordenado
# MAGIC por `ordinal_de_tramo` (la fuente reinicia `id_viaje` cada mes). Minutos y pares usan los últimos 12 meses
# MAGIC completos (la red vigente). Los minutos entre subidas incluyen el viaje en el primer ómnibus: **no son espera**.
# MAGIC Las subidas en efectivo no tienen transbordo (son viajes de un solo tramo) y quedan afuera.

# COMMAND ----------

import datetime as dt

from gold_comun import MIN_PCT_TRANSBORDOS, guardar, silver, ultimos_meses_completos
from pyspark.sql import Window
from pyspark.sql import functions as F

MESES_VENTANA = 12
# Subidas consecutivas a más de 3 horas no son un transbordo creíble (ej. un id_viaje que cruza el fin de mes)
MAX_MINUTOS = 180

con_tarjeta = silver("viajes").filter(F.col("ordinal_de_tramo") >= 1)
ordinal = F.col("ordinal_de_tramo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasa de transbordo mensual: `transbordos_mensual`

# COMMAND ----------

mensual = (
    con_tarjeta.groupBy(F.trunc(F.to_date("fecha_evento"), "month").alias("mes"))
    .agg(
        F.count(F.when(ordinal == 1, True)).alias("viajes_tarjeta"),
        F.count(F.when(ordinal == 2, True)).alias("viajes_con_transbordo"),
        F.count(F.when(ordinal >= 2, True)).alias("transbordos"),
    )
    # Meses en que la fuente casi no informa tramos 2..N (jun-2022: 0,05%): la tasa no es ~0, es desconocida
    .withColumn(
        "fuente_sin_transbordos",
        F.col("transbordos") / (F.col("viajes_tarjeta") + F.col("transbordos")) < MIN_PCT_TRANSBORDOS,
    )
)
guardar(
    mensual.select(
        "mes",
        F.year("mes").alias("anio"),
        "viajes_tarjeta",
        "viajes_con_transbordo",
        "transbordos",
        F.round(
            F.when(~F.col("fuente_sin_transbordos"), F.col("viajes_con_transbordo") / F.col("viajes_tarjeta")), 4
        ).alias("tasa_transbordo"),
        F.round(F.when(~F.col("fuente_sin_transbordos"), F.col("transbordos") / F.col("viajes_tarjeta") + 1), 4).alias(
            "subidas_por_viaje"
        ),
        "fuente_sin_transbordos",
    ),
    "transbordos_mensual",
    "Viajes con tarjeta y transbordos por mes. tasa_transbordo = viajes con al menos un transbordo / viajes con "
    "tarjeta. Null en los meses en que la fuente no informa transbordos.",
    {
        "mes": "Primer día del mes",
        "anio": "Año",
        "viajes_tarjeta": "Viajes con tarjeta (subidas con ordinal_de_tramo = 1)",
        "viajes_con_transbordo": "Viajes con al menos un transbordo (subidas con ordinal_de_tramo = 2)",
        "transbordos": "Subidas de transbordo (ordinal_de_tramo >= 2)",
        "tasa_transbordo": "viajes_con_transbordo / viajes_tarjeta (null si la fuente no informa transbordos)",
        "subidas_por_viaje": "Subidas promedio por viaje con tarjeta (1 = nadie transborda)",
        "fuente_sin_transbordos": f"True si menos del {MIN_PCT_TRANSBORDOS:.0%} de las subidas con tarjeta son "
        "transbordos: falla de la fuente (ej. jun-2022), la tasa queda en null",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconstrucción de viajes (últimos 12 meses)

# COMMAND ----------

desde, hasta = ultimos_meses_completos(MESES_VENTANA)
dias_ventana = (hasta - desde).days + 1
print(f"Ventana: {desde} a {hasta} ({dias_ventana} días)")

viaje = Window.partitionBy("mes", "id_viaje").orderBy("ordinal_de_tramo")
transbordos = (
    con_tarjeta.filter(
        (F.col("fecha_evento") >= F.lit(dt.datetime.combine(desde, dt.time())))
        & (F.col("fecha_evento") < F.lit(dt.datetime.combine(hasta + dt.timedelta(days=1), dt.time())))
    )
    .select(
        F.trunc(F.to_date("fecha_evento"), "month").alias("mes"),
        "id_viaje",
        "ordinal_de_tramo",
        "fecha_evento",
        "linea_codigo",
        "codigo_parada_origen",
    )
    .withColumn("linea_origen", F.lag("linea_codigo").over(viaje))
    .withColumn("subida_anterior", F.lag("fecha_evento").over(viaje))
    .filter((ordinal >= 2) & F.col("linea_origen").isNotNull())
    .withColumn("minutos", (F.unix_timestamp("fecha_evento") - F.unix_timestamp("subida_anterior")) / 60)
    .filter(F.col("minutos").between(0, MAX_MINUTOS))
    .select(
        "mes",
        "linea_origen",
        F.col("linea_codigo").alias("linea_destino"),
        F.col("codigo_parada_origen").alias("cod_ubic_parada"),
        "minutos",
        (F.col("linea_origen") == F.col("linea_codigo")).alias("misma_linea"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Minutos entre subidas: `transbordos_minutos`

# COMMAND ----------

tramo = F.least(F.floor(F.col("minutos") / 5) * 5, F.lit(120)).cast("int")
guardar(
    transbordos.groupBy("mes", tramo.alias("minutos_desde"), "misma_linea")
    .agg(F.count("*").alias("transbordos"))
    .withColumn(
        "tramo_minutos",
        F.when(F.col("minutos_desde") >= 120, "120+").otherwise(
            F.concat(
                F.lpad(F.col("minutos_desde").cast("string"), 3, "0"),
                F.lit("-"),
                F.lpad((F.col("minutos_desde") + 5).cast("string"), 3, "0"),
            )
        ),
    )
    .select("mes", "minutos_desde", "tramo_minutos", "misma_linea", "transbordos"),
    "transbordos_minutos",
    "Distribución de minutos entre una subida y la siguiente del mismo viaje, en tramos de 5 minutos (últimos 12 "
    "meses). Incluye el viaje en el primer ómnibus: no es tiempo de espera.",
    {
        "mes": "Primer día del mes",
        "minutos_desde": "Inicio del tramo de 5 minutos (120 = 120 minutos o más)",
        "tramo_minutos": "Etiqueta ordenable del tramo (ej. '015-020', '120+')",
        "misma_linea": "True si vuelve a subir a la misma línea (probable ida y vuelta dentro del boleto)",
        "transbordos": "Cantidad de transbordos",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Corredores de transbordo: `transbordos_pares`
# MAGIC Pares de líneas distintas. La misma línea (~9%) es casi siempre volver dentro de la ventana del boleto, no un
# MAGIC corredor que pida una línea directa.

# COMMAND ----------

entre_lineas = transbordos.filter(~F.col("misma_linea"))
pares = entre_lineas.groupBy("linea_origen", "linea_destino").agg(
    F.count("*").alias("transbordos"), F.percentile_approx("minutos", 0.5).alias("minutos_mediana")
)
# Parada donde más se hace cada transbordo
por_parada = entre_lineas.groupBy("linea_origen", "linea_destino", "cod_ubic_parada").agg(F.count("*").alias("n"))
parada_principal = (
    por_parada.withColumn(
        "orden", F.row_number().over(Window.partitionBy("linea_origen", "linea_destino").orderBy(F.col("n").desc()))
    )
    .filter(F.col("orden") == 1)
    .select("linea_origen", "linea_destino", "cod_ubic_parada", F.col("n").alias("transbordos_parada_principal"))
)
lineas = (
    silver("viajes")
    .filter(F.col("fecha_evento") >= F.lit(dt.datetime.combine(desde, dt.time())))
    .groupBy("linea_codigo")
    .agg(F.max_by("dsc_linea", "fecha_evento").alias("linea"))
)
paradas = silver("paradas").select("cod_ubic_parada", F.col("desc_ubic_").alias("parada_principal"), "lat", "lon")

guardar(
    pares.join(parada_principal, ["linea_origen", "linea_destino"])
    .join(lineas.withColumnsRenamed({"linea_codigo": "linea_origen", "linea": "nombre_origen"}), "linea_origen", "left")
    .join(
        lineas.withColumnsRenamed({"linea_codigo": "linea_destino", "linea": "nombre_destino"}), "linea_destino", "left"
    )
    .join(paradas, "cod_ubic_parada", "left")
    .select(
        F.concat_ws(" → ", "nombre_origen", "nombre_destino").alias("corredor"),
        "linea_origen",
        "nombre_origen",
        "linea_destino",
        "nombre_destino",
        F.lit(desde).alias("ventana_desde"),
        F.lit(hasta).alias("ventana_hasta"),
        "transbordos",
        F.round(F.col("transbordos") / dias_ventana, 1).alias("transbordos_por_dia"),
        F.round("minutos_mediana", 1).alias("minutos_mediana"),
        "cod_ubic_parada",
        "parada_principal",
        "lat",
        "lon",
        F.round(F.col("transbordos_parada_principal") / F.col("transbordos"), 4).alias("pct_en_parada_principal"),
    ),
    "transbordos_pares",
    "Corredores de transbordo: pares línea origen → línea destino (distintas) en los últimos 12 meses, con los "
    "minutos entre subidas y la parada donde más se transborda. Candidatos a coordinar horarios o a una línea directa.",
    {
        "corredor": "Etiqueta 'origen → destino' con los nombres públicos de las líneas",
        "linea_origen": "Código de la línea del tramo anterior",
        "nombre_origen": "Nombre público de la línea de origen",
        "linea_destino": "Código de la línea a la que se transborda",
        "nombre_destino": "Nombre público de la línea de destino",
        "ventana_desde": "Inicio de la ventana",
        "ventana_hasta": "Fin de la ventana",
        "transbordos": "Transbordos origen → destino en la ventana",
        "transbordos_por_dia": "Transbordos promedio por día calendario",
        "minutos_mediana": "Mediana de minutos entre las dos subidas (incluye el viaje en la línea de origen)",
        "cod_ubic_parada": "Parada donde más se hace este transbordo",
        "parada_principal": "Ubicación de esa parada",
        "lat": "Latitud de la parada principal",
        "lon": "Longitud de la parada principal",
        "pct_en_parada_principal": "Proporción de los transbordos del par que ocurren en la parada principal",
    },
)
