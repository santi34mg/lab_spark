# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Paradas: demanda, infraestructura y espera estimada
# MAGIC `paradas_espera`: una fila por parada con ubicación, refugio, demanda de un día hábil típico, oferta programada,
# MAGIC transbordos y **horas-persona de espera** estimadas. Responde dónde poner refugios primero.
# MAGIC
# MAGIC Espera estimada de cada subida = mitad del intervalo programado de su variante en esa parada y franja
# MAGIC (duración de la franja / pasadas / 2), con tope de 15 min: con intervalos largos la gente consulta el horario y
# MAGIC no espera el intervalo completo. En la cabecera de la variante (primera parada) la espera es 0: se sube al ómnibus
# MAGIC estacionado. Las subidas sin pasada programada de su variante en esa parada y franja no se estiman (ver
# MAGIC `cobertura_horario`). Horas a la intemperie = horas de espera en paradas sin refugio.
# MAGIC
# MAGIC Demanda: días hábiles típicos de los últimos 12 meses completos. Paradas, refugios y horarios son la foto actual.

# COMMAND ----------

import datetime as dt

from gold_comun import dias_tipicos, franja, guardar, minutos_franja, silver, ultimos_meses_completos
from pyspark.sql import Window
from pyspark.sql import functions as F

MESES_VENTANA = 12
TOPE_ESPERA_MIN = 15

# COMMAND ----------

desde, hasta = ultimos_meses_completos(MESES_VENTANA)
habiles = dias_tipicos(desde, hasta).filter(F.col("tipo_dia") == "Hábil").select("fecha")
n_habiles = habiles.count()
print(f"Ventana de demanda: {desde} a {hasta} ({n_habiles} días hábiles típicos)")

demanda = (
    silver("viajes")
    .filter(
        (F.col("fecha_evento") >= F.lit(dt.datetime.combine(desde, dt.time())))
        & (F.col("fecha_evento") < F.lit(dt.datetime.combine(hasta + dt.timedelta(days=1), dt.time())))
    )
    .join(F.broadcast(habiles), F.to_date("fecha_evento") == F.col("fecha"))
    .groupBy(
        F.col("codigo_parada_origen").alias("cod_ubic_parada"),
        F.col("sevar_codigo").alias("cod_variante"),
        franja(F.hour("fecha_evento")).alias("franja"),
    )
    .agg(
        F.count("*").alias("subidas"),
        F.count(F.when(F.col("ordinal_de_tramo") >= 2, True)).alias("transbordos"),
    )
)

# Horario vigente de día hábil: pasadas de cada variante por parada y franja, y cabeceras (ordinal 1)
horarios = silver("horarios_variante").filter(F.col("tipo_dia") == 1)
pasadas = horarios.groupBy("cod_variante", "cod_ubic_parada", franja(F.floor(F.col("hora") / 100)).alias("franja")).agg(
    F.count("*").alias("pasadas")
)
cabeceras = (
    horarios.filter(F.col("ordinal") == 1)
    .select("cod_variante", "cod_ubic_parada")
    .distinct()
    .withColumn("es_cabecera", F.lit(True))
)

espera = (
    demanda.join(pasadas, ["cod_variante", "cod_ubic_parada", "franja"], "left")
    .join(cabeceras, ["cod_variante", "cod_ubic_parada"], "left")
    .withColumn("intervalo_min", minutos_franja(F.col("franja")) / F.col("pasadas"))
    # least() ignora nulls: sin pasada programada la espera queda null (no estimada), no en el tope
    .withColumn(
        "espera_min",
        F.when(F.col("es_cabecera"), F.lit(0.0)).when(
            F.col("pasadas") > 0, F.least(F.col("intervalo_min") / 2, F.lit(TOPE_ESPERA_MIN))
        ),
    )
    .groupBy("cod_ubic_parada")
    .agg(
        F.sum("subidas").alias("subidas"),
        F.sum("transbordos").alias("transbordos"),
        F.sum(F.when(F.col("espera_min").isNotNull(), F.col("subidas"))).alias("subidas_con_horario"),
        F.sum(F.col("subidas") * F.col("espera_min")).alias("minutos_espera"),
    )
)

oferta = (
    silver("horarios_variante")
    .filter(F.col("tipo_dia") == 1)
    .groupBy("cod_ubic_parada")
    .agg(F.count("*").alias("pasadas_dia_habil"), F.countDistinct("cod_variante").alias("variantes_programadas"))
)

# COMMAND ----------

refugio = F.col("refugio")
paradas = (
    silver("paradas")
    .join(espera, "cod_ubic_parada", "left")
    .join(oferta, "cod_ubic_parada", "left")
    .fillna(
        0,
        [
            "subidas",
            "transbordos",
            "subidas_con_horario",
            "minutos_espera",
            "pasadas_dia_habil",
            "variantes_programadas",
        ],
    )
    .withColumn(
        "estado_refugio",
        F.when(refugio == "Sin refugio", "Sin refugio")
        .when(refugio == "Malo", "Refugio en mal estado")
        .when(refugio.isin("Bueno", "Regular"), "Con refugio")
        .otherwise("Sin dato"),
    )
    .withColumn(
        "piso",
        F.when(F.col("situacion").isin("Tierra", "Cuneta"), "Tierra o cuneta")
        .when(F.col("situacion").isin("Acera", "Hormigon"), "Vereda o pavimento")
        .otherwise("Sin dato"),
    )
    .withColumn("horas_espera_dia", F.col("minutos_espera") / 60 / n_habiles)
    .withColumn(
        "horas_espera_intemperie",
        F.when(F.col("estado_refugio") == "Sin refugio", F.col("horas_espera_dia")).otherwise(F.lit(0.0)),
    )
)
# Ranking de prioridad entre las paradas sin refugio y peso acumulado (para "N paradas concentran el X%")
prioridad = Window.partitionBy("estado_refugio").orderBy(F.col("horas_espera_intemperie").desc(), "cod_ubic_parada")
paradas = paradas.withColumn(
    "ranking_intemperie", F.when(F.col("estado_refugio") == "Sin refugio", F.row_number().over(prioridad))
).withColumn(
    "pct_acumulado_intemperie",
    F.when(
        F.col("estado_refugio") == "Sin refugio",
        F.sum("horas_espera_intemperie").over(prioridad.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        / F.sum("horas_espera_intemperie").over(Window.partitionBy("estado_refugio")),
    ),
)

guardar(
    paradas.select(
        "cod_ubic_parada",
        F.col("desc_ubic_").alias("ubicacion"),
        "lat",
        "lon",
        "estado_refugio",
        F.col("tipo_refug").alias("tipo_refugio"),
        "piso",
        F.lit(desde).alias("ventana_desde"),
        F.lit(hasta).alias("ventana_hasta"),
        F.round(F.col("subidas") / n_habiles, 1).alias("subidas_dia_habil"),
        F.round(F.col("transbordos") / n_habiles, 1).alias("transbordos_dia_habil"),
        "pasadas_dia_habil",
        "variantes_programadas",
        # try_divide: con ANSI activo una división por cero (parada sin subidas) corta el job
        F.round(F.try_divide("minutos_espera", "subidas_con_horario"), 2).alias("espera_media_min"),
        F.round("horas_espera_dia", 2).alias("horas_espera_dia"),
        F.round("horas_espera_intemperie", 2).alias("horas_espera_intemperie"),
        "ranking_intemperie",
        F.round("pct_acumulado_intemperie", 4).alias("pct_acumulado_intemperie"),
        F.round(F.try_divide("subidas_con_horario", "subidas"), 4).alias("cobertura_horario"),
    ),
    "paradas_espera",
    "Paradas STM con ubicación, estado del refugio, demanda de un día hábil típico, oferta programada, transbordos y "
    "horas-persona de espera estimadas (mitad del intervalo programado, tope 15 min). Prioriza dónde poner refugios.",
    {
        "cod_ubic_parada": "Código de la parada",
        "ubicacion": "Ubicación (cruce de calles)",
        "lat": "Latitud WGS 84",
        "lon": "Longitud WGS 84",
        "estado_refugio": "Sin refugio, Refugio en mal estado, Con refugio (bueno o regular) o Sin dato",
        "tipo_refugio": "Tipo de refugio según el relevamiento de la IM",
        "piso": "Superficie de espera: vereda o pavimento, tierra o cuneta, sin dato",
        "ventana_desde": "Inicio de la ventana de demanda",
        "ventana_hasta": "Fin de la ventana de demanda",
        "subidas_dia_habil": "Subidas promedio en un día hábil típico",
        "transbordos_dia_habil": "Subidas de transbordo (tramo 2 en adelante) por día hábil: nodos de transbordo",
        "pasadas_dia_habil": "Pasadas de ómnibus programadas por día hábil (todas las variantes)",
        "variantes_programadas": "Variantes (recorridos) con pasadas programadas en la parada",
        "espera_media_min": "Espera estimada promedio por subida, en minutos",
        "horas_espera_dia": "Horas-persona de espera estimadas por día hábil",
        "horas_espera_intemperie": "Horas-persona de espera por día hábil si la parada no tiene refugio (si no, 0)",
        "ranking_intemperie": "Prioridad entre las paradas sin refugio (1 = más horas de espera a la intemperie)",
        "pct_acumulado_intemperie": "Proporción acumulada de las horas a la intemperie hasta esta parada del ranking",
        "cobertura_horario": "Proporción de las subidas con pasada programada de su variante (base de la espera)",
    },
)
