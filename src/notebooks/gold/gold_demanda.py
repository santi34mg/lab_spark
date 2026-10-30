# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Demanda
# MAGIC Agrega `silver.viajes` (~1.900M subidas) en las tablas de demanda que consume el dashboard:
# MAGIC
# MAGIC | Tabla | Grano | Para qué |
# MAGIC |---|---|---|
# MAGIC | `dim_grupo_usuario`, `dim_tipo_viaje` | código | una descripción por código + categoría para los gráficos |
# MAGIC | `demanda_diaria` | fecha × empresa × grupo × tipo de viaje × tipo de tramo | cubo base, filtros del dashboard |
# MAGIC | `demanda_dia` | fecha | total del día, valor esperado y días atípicos (feriados, paros, eventos) |
# MAGIC | `demanda_mensual` | mes × tipo de día × categoría de usuario | evolución y recuperación vs pre-pandemia |
# MAGIC | `demanda_hora` | fecha × hora × categoría de usuario | heatmap hora × día de la semana |
# MAGIC | `perfil_horario` | año × tipo de día × hora | forma de la curva diaria y su evolución |
# MAGIC
# MAGIC `silver.viajes` se lee cuatro veces, siempre agregando (dos dimensiones, cubo diario y cubo horario). El resto
# MAGIC sale de tablas gold chicas, porque serverless no permite cachear DataFrames.

# COMMAND ----------

import datetime as dt

from dateutil.easter import easter
from gold_comun import (
    MIN_PCT_TRANSBORDOS,
    UMBRAL_ATIPICO,
    franja,
    gold,
    guardar,
    mapear,
    silver,
    tipo_dia,
    tipo_tramo,
)
from pyspark.sql import Window
from pyspark.sql import functions as F

viajes = silver("viajes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensiones de grupo de usuario y tipo de viaje
# MAGIC La fuente escribe el mismo código de distintas formas (ej. tipo 9 como `EST.G` y `EST. G`) y algunos códigos
# MAGIC nuevos llegan sin descripción: se toma la descripción más frecuente de cada código.

# COMMAND ----------

# Categorías propias para graficar con pocas series (el detalle queda en la descripción)
CATEGORIA_USUARIO = {
    "0": "Efectivo (sin tarjeta)",
    "1": "Usuario común",
    "2": "Estudiante",
    "12": "Estudiante",
    "3": "Jubilado",
    "4": "Gestión social",
}
CATEGORIA_BOLETO = {
    # común, zonal, céntrico, diferencial, 1 hora y 2 horas
    **dict.fromkeys(["1", "2", "3", "4", "5", "6"], "Tarifa general"),
    **dict.fromkeys(["7", "8", "10", "11"], "Descuento estudiante/jubilado"),  # EST. A/B, JUB. A/B
    **dict.fromkeys(["9", "15"], "Gratuito"),  # estudiante gratuito, gratuito
    "16": "Gestión social",
    **dict.fromkeys(["12", "13"], "Metropolitano"),  # metropolitano entrante/saliente
}


# Hay subidas sin código de tipo de viaje: un código explícito evita que se pierdan en los joins (null != null)
cod_grupo_usuario = F.coalesce(F.col("grupo_usuario"), F.lit("?")).alias("codigo")
cod_tipo_viaje = F.coalesce(F.col("tipo_viaje"), F.lit("?")).alias("codigo")


def dimension(codigo, descripcion):
    """Código -> descripción más frecuente (o 'Código N' si nunca vino con descripción)."""
    conteo = viajes.groupBy(codigo, F.col(descripcion).alias("descripcion")).count()
    mas_frecuente = (
        conteo.filter(F.col("descripcion").isNotNull())
        .groupBy("codigo")
        .agg(F.max_by("descripcion", "count").alias("descripcion"))
    )
    return (
        conteo.groupBy("codigo")
        .agg(F.sum("count").alias("subidas"))
        .join(mas_frecuente, "codigo", "left")
        .withColumn("descripcion", F.coalesce("descripcion", F.concat(F.lit("Código "), F.col("codigo"))))
    )


dim_grupo = dimension(cod_grupo_usuario, "descripcion_grupo_usuario").select(
    F.col("codigo").alias("cod_grupo_usuario"),
    F.initcap("descripcion").alias("grupo_usuario"),
    mapear(F.col("codigo"), CATEGORIA_USUARIO, "Otros (convenios, transporte, turistas)").alias("categoria_usuario"),
    "subidas",
)
guardar(
    dim_grupo,
    "dim_grupo_usuario",
    "Grupos de usuario del STM con una descripción por código y una categoría agregada para graficar.",
    {
        "cod_grupo_usuario": "Código de grupo de usuario de la fuente (0 = sin tarjeta / efectivo)",
        "grupo_usuario": "Descripción más frecuente del código en la fuente",
        "categoria_usuario": "Agrupación propia para gráficos: usuario común, estudiante, jubilado, gestión social, "
        "efectivo, otros",
        "subidas": "Subidas históricas con este código (para dimensionar la categoría)",
    },
)

dim_tipo = dimension(cod_tipo_viaje, "descripcion_tipo_viaje").select(
    F.col("codigo").alias("cod_tipo_viaje"),
    F.col("descripcion").alias("tipo_viaje"),
    mapear(F.col("codigo"), CATEGORIA_BOLETO, "Otros").alias("categoria_boleto"),
    "subidas",
)
guardar(
    dim_tipo,
    "dim_tipo_viaje",
    "Tipos de viaje (boletos) del STM con una descripción por código y una categoría de boleto propia.",
    {
        "cod_tipo_viaje": "Código de tipo de viaje de la fuente",
        "tipo_viaje": "Descripción más frecuente del código (ej. 1 HORA, EST. A, GRATUITO)",
        "categoria_boleto": "Agrupación propia: tarifa general, descuento estudiante/jubilado, gratuito, gestión "
        "social, metropolitano, otros",
        "subidas": "Subidas históricas con este código",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cubo diario: `demanda_diaria`

# COMMAND ----------

cubo = (
    viajes.groupBy(
        F.to_date("fecha_evento").alias("fecha"),
        F.col("cod_empresa"),
        F.col("descrip_empresa"),
        cod_grupo_usuario.alias("cod_grupo_usuario"),
        cod_tipo_viaje.alias("cod_tipo_viaje"),
        tipo_tramo(F.col("ordinal_de_tramo")).alias("tipo_tramo"),
    )
    .agg(F.count("*").alias("subidas"), F.sum("cantidad_pasajeros").alias("pasajeros"))
    .join(F.broadcast(gold("dim_grupo_usuario").drop("subidas")), "cod_grupo_usuario")
    .join(F.broadcast(gold("dim_tipo_viaje").drop("subidas")), "cod_tipo_viaje")
    .select(
        "fecha",
        F.trunc("fecha", "month").alias("mes"),
        F.year("fecha").alias("anio"),
        tipo_dia(F.col("fecha")).alias("tipo_dia"),
        # "C.U.T.C.S.A." -> "CUTCSA"
        F.regexp_replace("descrip_empresa", r"\.", "").alias("empresa"),
        "cod_grupo_usuario",
        "grupo_usuario",
        "categoria_usuario",
        "cod_tipo_viaje",
        "tipo_viaje",
        "categoria_boleto",
        "tipo_tramo",
        "subidas",
        "pasajeros",
    )
)
guardar(
    cubo,
    "demanda_diaria",
    "Cubo diario de subidas al STM por empresa, grupo de usuario, tipo de viaje y tipo de tramo. Base de los "
    "indicadores de demanda. Una subida es un tramo; los viajes son las subidas con tipo_tramo Inicial o Efectivo.",
    {
        "fecha": "Fecha local (Montevideo) de la subida",
        "mes": "Primer día del mes de la fecha",
        "anio": "Año de la fecha",
        "tipo_dia": "Hábil (lunes a viernes), Sábado o Domingo",
        "empresa": "Empresa operadora (CUTCSA, COETC, UCOT, COMESA)",
        "cod_grupo_usuario": "Código de grupo de usuario (ver dim_grupo_usuario)",
        "grupo_usuario": "Descripción del grupo de usuario",
        "categoria_usuario": "Categoría de usuario agregada para gráficos",
        "cod_tipo_viaje": "Código de tipo de viaje (ver dim_tipo_viaje)",
        "tipo_viaje": "Descripción del tipo de viaje (boleto)",
        "categoria_boleto": "Categoría de boleto agregada para gráficos",
        "tipo_tramo": "Inicial (primer tramo con tarjeta), Transbordo (tramos 2..N) o Efectivo",
        "subidas": "Cantidad de subidas (filas de silver.viajes)",
        "pasajeros": "Suma de cantidad_pasajeros (una subida en efectivo puede pagar varios boletos)",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Días atípicos: `demanda_dia`
# MAGIC Esperado = mediana del mismo día de la semana en las 4 semanas anteriores y posteriores. Un día es atípico si
# MAGIC se aparta del esperado en más de `UMBRAL_ATIPICO`. El modelo encuentra los días solo (feriados, paros,
# MAGIC elecciones, Turismo); el calendario sirve únicamente para ponerles nombre.

# COMMAND ----------

ELECCIONES = {
    dt.date(2019, 11, 24): "Elecciones nacionales (balotaje)",
    dt.date(2020, 9, 27): "Elecciones departamentales",
    dt.date(2022, 3, 27): "Referéndum LUC",
    dt.date(2024, 6, 30): "Elecciones internas",
    dt.date(2024, 10, 27): "Elecciones nacionales",
    dt.date(2024, 11, 24): "Elecciones nacionales (balotaje)",
    dt.date(2025, 5, 11): "Elecciones departamentales",
}
INICIO_COVID, FIN_COVID = dt.date(2020, 3, 13), dt.date(2020, 6, 30)


def calendario(anios):
    """Fecha -> nombre del feriado o fecha especial que explica un día atípico."""
    eventos = {}
    for anio in anios:
        for (mes, dia), nombre in {
            (1, 1): "Año Nuevo",
            (1, 6): "Día de Reyes",
            (5, 1): "Día de los Trabajadores",
            (6, 19): "Natalicio de Artigas",
            (7, 18): "Jura de la Constitución",
            (8, 25): "Declaratoria de la Independencia",
            (11, 2): "Día de los Difuntos",
            (12, 24): "Nochebuena",
            (12, 25): "Navidad",
            (12, 31): "Fin de año",
        }.items():
            eventos[dt.date(anio, mes, dia)] = nombre
        # Feriados trasladables (leyes 16.805 y 17.414): martes o miércoles pasan al lunes anterior,
        # jueves o viernes al lunes siguiente
        for (mes, dia), nombre in {
            (4, 19): "Desembarco de los 33 Orientales",
            (5, 18): "Batalla de Las Piedras",
            (10, 12): "Día de la Diversidad Cultural",
        }.items():
            fecha = dt.date(anio, mes, dia)
            dia_semana = fecha.weekday()  # 0 = lunes
            if dia_semana in (1, 2):
                fecha -= dt.timedelta(days=dia_semana)
            elif dia_semana in (3, 4):
                fecha += dt.timedelta(days=7 - dia_semana)
            eventos[fecha] = nombre
        pascua = easter(anio)
        for dias in (48, 47):
            eventos[pascua - dt.timedelta(days=dias)] = "Carnaval"
        for dias in range(2, 7):  # lunes a viernes de la semana anterior a Pascua
            eventos[pascua - dt.timedelta(days=dias)] = "Semana de Turismo"
    eventos.update(ELECCIONES)
    return eventos


# Meses en que la fuente no informa transbordos. En jun-2022 el archivo además llega solo hasta el día 20 y trae
# ~45% más filas por día: sus días se marcan atípicos y no entran en el valor esperado de los días vecinos.
meses_anomalos = (
    gold("demanda_diaria")
    .groupBy("mes")
    .agg(
        F.sum(F.when(F.col("tipo_tramo") == "Transbordo", F.col("subidas")).otherwise(0)).alias("transbordos"),
        F.sum("subidas").alias("subidas"),
    )
    .filter(F.col("transbordos") / F.col("subidas") < MIN_PCT_TRANSBORDOS)
    .select("mes", F.lit(True).alias("fuente_anomala"))
)
totales = (
    gold("demanda_diaria")
    .groupBy("fecha", "mes", "tipo_dia")
    .agg(
        F.sum("subidas").alias("subidas"),
        F.sum(F.when(F.col("tipo_tramo") != "Transbordo", F.col("subidas"))).alias("viajes"),
        F.sum(F.when(F.col("tipo_tramo") == "Efectivo", F.col("subidas"))).alias("subidas_efectivo"),
    )
    .join(F.broadcast(meses_anomalos), "mes", "left")
    .fillna(False, ["fuente_anomala"])
)
esperado = (
    totales.alias("d")
    .join(
        totales.filter(~F.col("fuente_anomala")).alias("v"),
        (F.dayofweek("d.fecha") == F.dayofweek("v.fecha")) & F.abs(F.datediff("d.fecha", "v.fecha")).between(7, 28),
    )
    .groupBy(F.col("d.fecha").alias("fecha"))
    .agg(F.median("v.subidas").alias("subidas_esperadas"))
)

anios = [r[0] for r in totales.select(F.year("fecha")).distinct().collect()]
eventos = spark.createDataFrame(list(calendario(anios).items()), "fecha date, evento string")

# 1 = lunes ... 7 = domingo (dayofweek de Spark empieza en domingo) y etiqueta ordenable para los ejes
dia_semana = ((F.dayofweek("fecha") + 5) % 7) + 1
NOMBRES_DIA = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
nombre_dia = mapear(dia_semana.cast("string"), {str(i): f"{i}. {n}" for i, n in enumerate(NOMBRES_DIA, 1)}, "")

demanda_dia = (
    totales.join(esperado, "fecha", "left")
    .join(eventos, "fecha", "left")
    .withColumn("desvio", F.try_divide("subidas", "subidas_esperadas") - 1)
    .withColumn("periodo_covid", F.col("fecha").between(INICIO_COVID, FIN_COVID))
    .withColumn(
        "evento",
        F.coalesce(
            F.when(F.col("fuente_anomala"), F.lit("Fuente anómala (mes sin transbordos)")),
            "evento",
            F.when(F.col("periodo_covid"), F.lit("Emergencia sanitaria COVID-19")),
        ),
    )
    .select(
        "fecha",
        "mes",
        F.year("fecha").alias("anio"),
        "tipo_dia",
        dia_semana.alias("dia_semana"),
        nombre_dia.alias("nombre_dia"),
        "subidas",
        "viajes",
        "subidas_efectivo",
        F.round("subidas_esperadas").cast("bigint").alias("subidas_esperadas"),
        F.round("desvio", 4).alias("desvio"),
        (F.coalesce(F.abs(F.col("desvio")) >= UMBRAL_ATIPICO, F.lit(False)) | F.col("fuente_anomala")).alias(
            "es_atipico"
        ),
        "evento",
        "periodo_covid",
        "fuente_anomala",
    )
)
guardar(
    demanda_dia,
    "demanda_dia",
    "Total de subidas por día con su valor esperado (mediana del mismo día de la semana ±4 semanas) y marca de día "
    "atípico. Detecta feriados, paros, elecciones y Turismo sin calendario externo; el calendario solo los nombra.",
    {
        "fecha": "Fecha local (Montevideo)",
        "mes": "Primer día del mes",
        "anio": "Año",
        "tipo_dia": "Hábil, Sábado o Domingo",
        "dia_semana": "Día de la semana: 1 = lunes ... 7 = domingo",
        "nombre_dia": "Etiqueta ordenable del día de la semana (ej. '1. Lunes')",
        "subidas": "Subidas del día (tramos, con tarjeta y en efectivo)",
        "viajes": "Viajes del día: subidas iniciales con tarjeta y en efectivo (sin transbordos)",
        "subidas_efectivo": "Subidas pagadas en efectivo (sin tarjeta)",
        "subidas_esperadas": "Mediana de subidas del mismo día de la semana en las 4 semanas previas y posteriores",
        "desvio": "subidas / subidas_esperadas - 1 (ej. -0.6 = 60% menos de lo esperado)",
        "es_atipico": f"True si |desvio| >= {UMBRAL_ATIPICO} o la fuente es anómala: el día no se usa para "
        "promedios de día típico",
        "evento": "Feriado o fecha especial del calendario que explica el día (null = sin feriado registrado: "
        "posible paro o evento)",
        "periodo_covid": "True entre el 13/03/2020 (emergencia sanitaria) y el 30/06/2020",
        "fuente_anomala": f"True si en el mes menos del {MIN_PCT_TRANSBORDOS:.0%} de las subidas son transbordos: "
        "la fuente no los informa (jun-2022: además incompleto y con ~45% más filas por día). No se usa como "
        "referencia ni para promedios",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evolución mensual: `demanda_mensual`
# MAGIC Promedio por día típico de cada tipo de día. Así un mes con feriados o con más fines de semana no parece
# MAGIC tener menos demanda. La base pre-pandemia es el mismo mes calendario antes de marzo de 2020 (nov-2019 a
# MAGIC feb-2020), porque comparar contra un promedio que mezcla verano y clases infla o achica la recuperación.

# COMMAND ----------

tipicos = gold("demanda_dia").filter(~F.col("es_atipico")).select("fecha", "mes", "tipo_dia")
dias_por_mes = tipicos.groupBy("mes", "tipo_dia").agg(F.count("*").alias("dias_tipicos"))

mensual = (
    gold("demanda_diaria")
    .join(tipicos.select("fecha"), "fecha")
    .groupBy("mes", "tipo_dia", "categoria_usuario")
    .agg(
        F.sum("subidas").alias("subidas"),
        F.sum(F.when(F.col("tipo_tramo") != "Transbordo", F.col("subidas"))).alias("viajes"),
    )
    .join(dias_por_mes, ["mes", "tipo_dia"])
    .withColumn("subidas_por_dia", F.col("subidas") / F.col("dias_tipicos"))
    .withColumn("viajes_por_dia", F.col("viajes") / F.col("dias_tipicos"))
)
base = mensual.filter(F.col("mes") < F.lit(INICIO_COVID.replace(day=1))).select(
    F.month("mes").alias("mes_del_anio"),
    "tipo_dia",
    "categoria_usuario",
    F.col("subidas_por_dia").alias("base_subidas_por_dia"),
)
mensual = mensual.withColumn("mes_del_anio", F.month("mes")).join(
    base, ["mes_del_anio", "tipo_dia", "categoria_usuario"], "left"
)
guardar(
    mensual.select(
        "mes",
        F.year("mes").alias("anio"),
        "tipo_dia",
        "categoria_usuario",
        "dias_tipicos",
        "subidas",
        "viajes",
        F.round("subidas_por_dia", 1).alias("subidas_por_dia"),
        F.round("viajes_por_dia", 1).alias("viajes_por_dia"),
        F.round("base_subidas_por_dia", 1).alias("base_subidas_por_dia"),
        F.round(F.col("subidas_por_dia") / F.col("base_subidas_por_dia"), 4).alias("indice_vs_prepandemia"),
    ),
    "demanda_mensual",
    "Subidas y viajes promedio por día típico, por mes, tipo de día y categoría de usuario, con índice de "
    "recuperación contra el mismo mes calendario pre-pandemia (nov-2019 a feb-2020).",
    {
        "mes": "Primer día del mes",
        "anio": "Año",
        "tipo_dia": "Hábil, Sábado o Domingo",
        "categoria_usuario": "Categoría de usuario (ver dim_grupo_usuario)",
        "dias_tipicos": "Días típicos (no atípicos) de ese tipo en el mes",
        "subidas": "Subidas en los días típicos del mes",
        "viajes": "Viajes (sin transbordos) en los días típicos del mes",
        "subidas_por_dia": "Promedio de subidas por día típico",
        "viajes_por_dia": "Promedio de viajes por día típico",
        "base_subidas_por_dia": "Subidas por día típico del mismo mes calendario antes de la pandemia (solo existe "
        "para noviembre a febrero)",
        "indice_vs_prepandemia": "subidas_por_dia / base_subidas_por_dia (1 = mismo nivel que antes de la "
        "pandemia). Sumar ambas columnas antes de dividir para agregar categorías",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribución horaria: `demanda_hora` y `perfil_horario`

# COMMAND ----------

dias = gold("demanda_dia").select("fecha", "tipo_dia", "dia_semana", "nombre_dia", "es_atipico")

por_hora = (
    viajes.groupBy(
        F.to_date("fecha_evento").alias("fecha"),
        F.hour("fecha_evento").alias("hora"),
        F.col("grupo_usuario").alias("cod_grupo_usuario"),
    )
    .agg(F.count("*").alias("subidas"))
    .join(F.broadcast(gold("dim_grupo_usuario").select("cod_grupo_usuario", "categoria_usuario")), "cod_grupo_usuario")
    .groupBy("fecha", "hora", "categoria_usuario")
    .agg(F.sum("subidas").alias("subidas"))
    .join(dias, "fecha")
    .select(
        "fecha",
        F.trunc("fecha", "month").alias("mes"),
        F.year("fecha").alias("anio"),
        "tipo_dia",
        "dia_semana",
        "nombre_dia",
        "es_atipico",
        "hora",
        franja(F.col("hora")).alias("franja"),
        "categoria_usuario",
        "subidas",
    )
)
guardar(
    por_hora,
    "demanda_hora",
    "Subidas por fecha, hora local y categoría de usuario. Fuente del heatmap hora × día de la semana; filtrar "
    "es_atipico = false para ver la semana típica.",
    {
        "fecha": "Fecha local (Montevideo)",
        "mes": "Primer día del mes",
        "anio": "Año",
        "tipo_dia": "Hábil, Sábado o Domingo",
        "dia_semana": "1 = lunes ... 7 = domingo",
        "nombre_dia": "Etiqueta ordenable del día de la semana",
        "es_atipico": "True si el día es feriado, paro o evento (ver demanda_dia)",
        "hora": "Hora local de la subida (0-23)",
        "franja": "Franja horaria amplia (madrugada, mañana, mediodía, tarde, noche)",
        "categoria_usuario": "Categoría de usuario",
        "subidas": "Subidas en esa hora",
    },
)

# COMMAND ----------

dias_anio = (
    gold("demanda_dia").filter(~F.col("es_atipico")).groupBy("anio", "tipo_dia").agg(F.count("*").alias("dias_tipicos"))
)
perfil = (
    gold("demanda_hora")
    .filter(~F.col("es_atipico"))
    .groupBy("anio", "tipo_dia", "hora")
    .agg(F.sum("subidas").alias("subidas"))
    .join(dias_anio, ["anio", "tipo_dia"])
    .withColumn("subidas_por_dia", F.col("subidas") / F.col("dias_tipicos"))
    .withColumn(
        "pct_del_dia",
        F.col("subidas_por_dia") / F.sum("subidas_por_dia").over(Window.partitionBy("anio", "tipo_dia")),
    )
)
guardar(
    perfil.select(
        "anio",
        "tipo_dia",
        "hora",
        franja(F.col("hora")).alias("franja"),
        "dias_tipicos",
        F.round("subidas_por_dia", 1).alias("subidas_por_dia"),
        F.round("pct_del_dia", 5).alias("pct_del_dia"),
    ),
    "perfil_horario",
    "Curva horaria de un día típico por año y tipo de día: subidas promedio por hora y su peso en el día. Permite "
    "ver si la hora pico se aplana con los años (teletrabajo, cambios de horario).",
    {
        "anio": "Año (2019 solo tiene noviembre y diciembre)",
        "tipo_dia": "Hábil, Sábado o Domingo",
        "hora": "Hora local (0-23)",
        "franja": "Franja horaria amplia",
        "dias_tipicos": "Días típicos de ese tipo en el año",
        "subidas_por_dia": "Subidas promedio en esa hora en un día típico",
        "pct_del_dia": "Proporción de las subidas del día que ocurren en esa hora (suma 1 por año y tipo de día)",
    },
)
