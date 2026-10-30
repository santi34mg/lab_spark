# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Líneas: demanda y oferta programada
# MAGIC
# MAGIC | Tabla | Grano | Para qué |
# MAGIC |---|---|---|
# MAGIC | `lineas_mensual` | mes × línea × tipo de día | evolución y ranking de líneas (subidas por día típico) |
# MAGIC | `oferta_demanda_linea` | línea × tipo de día × franja | subidas por salida programada en la ventana reciente |
# MAGIC
# MAGIC **Subidas por salida programada** = subidas promedio en un día típico / salidas que el horario vigente programa
# MAGIC ese día. Una salida es un recorrido de una variante, identificado por `(cod_variante, frecuencia)`; `frecuencia`
# MAGIC es la hora de salida (HHMM0). Mide cuánta gente sube por cada ómnibus programado, **no** la ocupación: sin datos
# MAGIC de descenso no hay carga a bordo. El horario es una foto actual, por eso se cruza solo con los últimos meses.

# COMMAND ----------

import datetime as dt

from gold_comun import dias_tipicos, franja, gold, guardar, silver, tipo_dia_horarios, ultimos_meses_completos
from pyspark.sql import Window
from pyspark.sql import functions as F

# Meses de demanda que se cruzan con el horario vigente (los más recientes, completos)
MESES_VENTANA = 2

viajes = silver("viajes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evolución por línea: `lineas_mensual`

# COMMAND ----------

tipicos = gold("demanda_dia").filter(~F.col("es_atipico")).select("fecha", "mes", "tipo_dia")
dias_por_mes = tipicos.groupBy("mes", "tipo_dia").agg(F.count("*").alias("dias_tipicos"))

mensual = (
    viajes.join(F.broadcast(tipicos), F.to_date("fecha_evento") == F.col("fecha"))
    .groupBy("mes", "tipo_dia", "linea_codigo")
    .agg(
        F.count("*").alias("subidas"),
        F.max_by("dsc_linea", "fecha_evento").alias("dsc_linea"),
        F.max_by("descrip_empresa", "fecha_evento").alias("descrip_empresa"),
    )
    .join(dias_por_mes, ["mes", "tipo_dia"])
)
# Nombre y empresa vigentes de cada línea (el último mes en que aparece), para no partir una línea en dos series
ultimo = Window.partitionBy("linea_codigo").orderBy(F.col("mes").desc())
mensual = mensual.withColumn("linea", F.first("dsc_linea").over(ultimo)).withColumn(
    "empresa", F.regexp_replace(F.first("descrip_empresa").over(ultimo), r"\.", "")
)
guardar(
    mensual.select(
        "mes",
        F.year("mes").alias("anio"),
        "tipo_dia",
        "linea_codigo",
        "linea",
        "empresa",
        "dias_tipicos",
        "subidas",
        F.round(F.col("subidas") / F.col("dias_tipicos"), 1).alias("subidas_por_dia"),
    ),
    "lineas_mensual",
    "Subidas por línea, mes y tipo de día, con el promedio por día típico (sin feriados, paros ni eventos).",
    {
        "mes": "Primer día del mes",
        "anio": "Año",
        "tipo_dia": "Hábil, Sábado o Domingo",
        "linea_codigo": "Código interno de la línea (linea_codigo de la fuente)",
        "linea": "Nombre público vigente de la línea (ej. 121, CE1, L16)",
        "empresa": "Empresa que opera la línea",
        "dias_tipicos": "Días típicos de ese tipo en el mes",
        "subidas": "Subidas en los días típicos del mes",
        "subidas_por_dia": "Promedio de subidas por día típico",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oferta vs demanda: `oferta_demanda_linea`

# COMMAND ----------

desde, hasta = ultimos_meses_completos(MESES_VENTANA)
print(f"Ventana de demanda: {desde} a {hasta}")
dias = dias_tipicos(desde, hasta)
n_dias = dias.groupBy("tipo_dia").agg(F.count("*").alias("dias_tipicos"))

demanda = (
    viajes.filter(
        (F.col("fecha_evento") >= F.lit(dt.datetime.combine(desde, dt.time())))
        & (F.col("fecha_evento") < F.lit(dt.datetime.combine(hasta + dt.timedelta(days=1), dt.time())))
    )
    .join(F.broadcast(dias), F.to_date("fecha_evento") == F.col("fecha"))
    .groupBy(
        "linea_codigo",
        F.col("sevar_codigo").alias("cod_variante"),
        "tipo_dia",
        franja(F.hour("fecha_evento")).alias("franja"),
    )
    .agg(F.count("*").alias("subidas"))
)

horarios = silver("horarios_variante").withColumn("tipo_dia", tipo_dia_horarios(F.col("tipo_dia")))
# Salidas por la hora de partida (frecuencia = HHMM0) y pasadas por parada según la hora de pasada (hora = HMM)
salidas = horarios.groupBy("cod_variante", "tipo_dia", franja(F.floor(F.col("frecuencia") / 1000)).alias("franja")).agg(
    F.countDistinct("frecuencia").alias("salidas")
)
pasadas = horarios.groupBy("cod_variante", "tipo_dia", franja(F.floor(F.col("hora") / 100)).alias("franja")).agg(
    F.count("*").alias("pasadas")
)

# La línea de cada variante sale de la demanda: horarios no trae la línea. Una variante programada que no tuvo
# ninguna subida en la ventana no se puede asignar a una línea y queda afuera.
variante_linea = demanda.groupBy("cod_variante").agg(F.max_by("linea_codigo", "subidas").alias("linea_codigo"))
cruce = (
    demanda.drop("linea_codigo")
    .join(salidas, ["cod_variante", "tipo_dia", "franja"], "full")
    .join(pasadas, ["cod_variante", "tipo_dia", "franja"], "full")
    .join(variante_linea, "cod_variante")
    .fillna(0, ["subidas", "salidas", "pasadas"])
    # Subidas de variantes sin ninguna salida programada ese tipo de día: no tienen denominador
    .withColumn(
        "variante_programada",
        F.max("salidas").over(Window.partitionBy("cod_variante", "tipo_dia")) > 0,
    )
)


def resumir(df, *grupo):
    return df.groupBy("linea_codigo", "tipo_dia", *grupo).agg(
        F.sum("subidas").alias("subidas"),
        F.sum(F.when(F.col("variante_programada"), F.col("subidas")).otherwise(0)).alias("subidas_con_horario"),
        F.sum("salidas").alias("salidas_programadas"),
        F.sum("pasadas").alias("pasadas_programadas"),
        F.countDistinct(F.when(F.col("salidas") > 0, F.col("cod_variante"))).alias("variantes"),
    )


nombres = (
    gold("lineas_mensual").select("linea_codigo", "linea", "empresa").distinct()
)  # nombre vigente: igual en todas las filas de la línea
oferta_demanda = (
    resumir(cruce, "franja")
    .unionByName(resumir(cruce).withColumn("franja", F.lit("0. Día completo")))
    .join(n_dias, "tipo_dia")
    .join(nombres, "linea_codigo", "left")
    .withColumn("subidas_por_dia", F.col("subidas") / F.col("dias_tipicos"))
    .withColumn("subidas_con_horario_por_dia", F.col("subidas_con_horario") / F.col("dias_tipicos"))
)
guardar(
    oferta_demanda.select(
        "linea_codigo",
        "linea",
        "empresa",
        "tipo_dia",
        "franja",
        F.lit(desde).alias("ventana_desde"),
        F.lit(hasta).alias("ventana_hasta"),
        "dias_tipicos",
        F.round("subidas_por_dia", 1).alias("subidas_por_dia"),
        "salidas_programadas",
        "pasadas_programadas",
        "variantes",
        F.round(
            F.when(
                F.col("salidas_programadas") > 0, F.col("subidas_con_horario_por_dia") / F.col("salidas_programadas")
            ),
            2,
        ).alias("subidas_por_salida"),
        F.round(
            F.when(
                F.col("pasadas_programadas") > 0, F.col("subidas_con_horario_por_dia") / F.col("pasadas_programadas")
            ),
            3,
        ).alias("subidas_por_pasada"),
        # try_divide: hay franjas con salidas y sin subidas, y con ANSI activo dividir por cero corta el job
        F.round(F.try_divide("subidas_con_horario", "subidas"), 4).alias("cobertura_horario"),
    ),
    "oferta_demanda_linea",
    "Subidas por salida programada por línea, tipo de día y franja: demanda de los últimos meses completos (días "
    "típicos) contra el horario vigente. Mide subidas por ómnibus programado, no ocupación (no hay descensos).",
    {
        "linea_codigo": "Código interno de la línea",
        "linea": "Nombre público vigente de la línea",
        "empresa": "Empresa que opera la línea",
        "tipo_dia": "Hábil, Sábado o Domingo (en horarios, domingo incluye feriados)",
        "franja": "Franja horaria amplia, o '0. Día completo' para el total del día",
        "ventana_desde": "Inicio de la ventana de demanda",
        "ventana_hasta": "Fin de la ventana de demanda",
        "dias_tipicos": "Días típicos de ese tipo en la ventana",
        "subidas_por_dia": "Subidas promedio por día típico (todas las variantes de la línea)",
        "salidas_programadas": "Salidas por día en el horario vigente, según la hora de partida de cada recorrido",
        "pasadas_programadas": "Pasadas por parada por día en el horario vigente (salidas × paradas del recorrido)",
        "variantes": "Variantes (recorridos/sentidos) de la línea con salidas programadas",
        "subidas_por_salida": "Subidas por día / salidas programadas: cuánta gente sube por cada ómnibus que sale",
        "subidas_por_pasada": "Subidas por día / pasadas programadas: corrige por el largo del recorrido",
        "cobertura_horario": "Proporción de las subidas de la línea cuya variante tiene horario ese tipo de día",
    },
)
