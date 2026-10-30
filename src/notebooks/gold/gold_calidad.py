# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Calidad del dato
# MAGIC `calidad_mensual`: control de linaje y calidad por mes entre las capas. Filas en bronze y en silver, filas
# MAGIC descartadas, días con datos, peso del efectivo, transbordos informados y match de las subidas contra las
# MAGIC paradas y los horarios vigentes. Deja a la vista las fallas de la fuente (ej. jun-2022 sin transbordos) y los
# MAGIC meses que no se pueden comparar.

# COMMAND ----------

from databricks.sdk.runtime import dbutils, spark
from gold_comun import MIN_PCT_TRANSBORDOS, catalog, guardar, silver
from pyspark.sql import functions as F

bronze_schema = dbutils.widgets.get("bronze_schema")

# COMMAND ----------

# Bronze guarda fecha_evento como texto en dos formatos; los dos empiezan con yyyy-MM en hora local
bronze = (
    spark.read.table(f"`{catalog}`.`{bronze_schema}`.viajes")
    .groupBy(F.substring("fecha_evento", 1, 7).alias("anio_mes"))
    .agg(F.count("*").alias("filas_bronze"))
)

codigos_parada = silver("paradas").select(F.col("cod_ubic_parada").alias("codigo_parada_origen")).distinct()
codigos_variante = silver("horarios_variante").select(F.col("cod_variante").alias("sevar_codigo")).distinct()

silver_mes = (
    silver("viajes")
    .join(F.broadcast(codigos_parada.withColumn("parada_ok", F.lit(1))), "codigo_parada_origen", "left")
    .join(F.broadcast(codigos_variante.withColumn("variante_ok", F.lit(1))), "sevar_codigo", "left")
    .groupBy(F.date_format("fecha_evento", "yyyy-MM").alias("anio_mes"))
    .agg(
        F.count("*").alias("filas_silver"),
        F.countDistinct(F.to_date("fecha_evento")).alias("dias_con_datos"),
        F.count(F.when(~F.col("con_tarjeta"), True)).alias("subidas_efectivo"),
        F.count(F.when(F.col("ordinal_de_tramo") >= 2, True)).alias("transbordos"),
        F.sum("parada_ok").alias("con_parada"),
        F.sum("variante_ok").alias("con_variante"),
    )
)

mes = F.to_date(F.concat(F.col("anio_mes"), F.lit("-01")))
calidad = (
    bronze.join(silver_mes, "anio_mes", "full")
    .fillna(0, ["filas_bronze", "filas_silver", "dias_con_datos", "subidas_efectivo", "transbordos"])
    .withColumn("mes", mes)
    .withColumn("dias_del_mes", F.dayofmonth(F.last_day(mes)))
    .withColumn("pct_efectivo", F.try_divide("subidas_efectivo", "filas_silver"))
    .withColumn(
        "observacion",
        F.concat_ws(
            "; ",
            F.when(
                F.try_divide("transbordos", "filas_silver") < MIN_PCT_TRANSBORDOS,
                F.lit("La fuente no informa transbordos"),
            ),
            F.when(F.col("subidas_efectivo") == 0, F.lit("Sin subidas en efectivo en silver")),
            # El 1/1 y el 1/5 no hay servicio: un mes con uno o dos días sin datos no está incompleto
            F.when(F.col("dias_con_datos") < F.col("dias_del_mes") - 2, F.lit("Mes incompleto")),
            F.when(F.col("mes").between("2020-03-01", "2020-06-30"), F.lit("Emergencia sanitaria COVID-19")),
        ),
    )
)

guardar(
    calidad.select(
        "mes",
        F.year("mes").alias("anio"),
        "filas_bronze",
        "filas_silver",
        (F.col("filas_bronze") - F.col("filas_silver")).alias("filas_descartadas"),
        F.round(F.try_divide(F.col("filas_bronze") - F.col("filas_silver"), F.col("filas_bronze")), 6).alias(
            "pct_descartado"
        ),
        "dias_con_datos",
        "dias_del_mes",
        F.round("pct_efectivo", 4).alias("pct_efectivo"),
        "transbordos",
        F.round(F.try_divide("con_parada", "filas_silver"), 4).alias("pct_match_parada"),
        F.round(F.try_divide("con_variante", "filas_silver"), 4).alias("pct_match_variante"),
        F.when(F.col("observacion") != "", F.col("observacion")).alias("observacion"),
    ),
    "calidad_mensual",
    "Control de calidad y linaje por mes: filas en bronze y silver, descartes, días con datos, efectivo, transbordos "
    "informados y match contra paradas y horarios vigentes.",
    {
        "mes": "Primer día del mes",
        "anio": "Año",
        "filas_bronze": "Filas del mes en bronze.viajes (CSV crudos)",
        "filas_silver": "Filas del mes en silver.viajes (tipadas y limpias)",
        "filas_descartadas": "filas_bronze - filas_silver (nulos obligatorios, cantidades inválidas, duplicados)",
        "pct_descartado": "Proporción de filas de bronze que no pasan a silver",
        "dias_con_datos": "Días del mes con al menos una subida en silver",
        "dias_del_mes": "Días calendario del mes",
        "pct_efectivo": "Proporción de subidas en efectivo (sin tarjeta)",
        "transbordos": "Subidas de transbordo informadas (casi 0 = la fuente no trae transbordos ese mes)",
        "pct_match_parada": "Proporción de subidas cuya parada existe en la capa de paradas (foto actual)",
        "pct_match_variante": "Proporción de subidas cuya variante existe en el horario vigente (cae hacia atrás "
        "porque el horario es una foto actual)",
        "observacion": "Alertas del mes: fuente sin transbordos, sin efectivo, mes incompleto, COVID",
    },
)
