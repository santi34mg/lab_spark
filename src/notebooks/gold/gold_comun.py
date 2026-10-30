"""Definiciones comunes de la capa gold STM.

Lo importan los notebooks de esta carpeta: lee los parámetros del job, crea el schema gold si no existe y centraliza
las reglas de negocio, para que todas las tablas usen la misma definición de tipo de día, franja horaria, día típico
y ventana de análisis.

Vocabulario (igual en las tablas y en el dashboard):
- subida: una fila de silver.viajes (un tramo: alguien sube a un ómnibus, con tarjeta o en efectivo).
- viaje: subida inicial (ordinal_de_tramo 0 = efectivo, 1 = primer tramo con tarjeta). Los tramos 2..N del mismo
  id_viaje son transbordos.
- día típico: día sin feriado, paro ni evento (ver demanda_dia.es_atipico). Los promedios "por día" se calculan sobre
  días típicos para que un feriado no baje el promedio de un mes.
"""

import datetime as dt

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{gold_schema}` "
    "COMMENT 'Capa gold STM: métricas agregadas para la toma de decisiones de la IM'"
)

# Desvío contra el valor esperado a partir del cual un día se marca como atípico (feriado, paro, evento)
UMBRAL_ATIPICO = 0.25

# Un mes con menos de esta proporción de subidas de transbordo es una falla de la fuente (lo normal es ~25%).
# Jun-2022 trae 9.847 transbordos sobre 20,7M de subidas (0,05%).
MIN_PCT_TRANSBORDOS = 0.05

# Franjas horarias amplias: absorben la diferencia entre la hora de salida del ómnibus (horarios) y la hora de
# subida de cada pasajero (viajes), que en un recorrido de 45-60 min caen en horas distintas.
# (hora desde, hora hasta, etiqueta). El prefijo numérico ordena las franjas en los gráficos.
FRANJAS = [
    (0, 6, "1. Madrugada (0-6 h)"),
    (6, 10, "2. Mañana (6-10 h)"),
    (10, 15, "3. Mediodía (10-15 h)"),
    (15, 20, "4. Tarde (15-20 h)"),
    (20, 24, "5. Noche (20-24 h)"),
]


def franja(hora):
    """Etiqueta de franja para una columna de hora (0-23)."""
    expr = None
    for desde, hasta, nombre in FRANJAS:
        expr = (F.when if expr is None else expr.when)((hora >= desde) & (hora < hasta), nombre)
    return expr


def minutos_franja(etiqueta):
    """Duración en minutos de la franja (para pasar de pasadas por franja a intervalo entre ómnibus)."""
    expr = None
    for desde, hasta, nombre in FRANJAS:
        expr = (F.when if expr is None else expr.when)(etiqueta == nombre, (hasta - desde) * 60)
    return expr


def tipo_dia(fecha):
    """Hábil / Sábado / Domingo, con las mismas categorías que el tipo_dia 1/2/3 de los horarios."""
    dia = F.dayofweek(fecha)  # 1 = domingo ... 7 = sábado
    return F.when(dia == 1, "Domingo").when(dia == 7, "Sábado").otherwise("Hábil")


def tipo_dia_horarios(codigo):
    """tipo_dia de horarios_variante (1 = hábil, 2 = sábado, 3 = domingo y feriados) a las categorías de gold."""
    return F.when(codigo == 1, "Hábil").when(codigo == 2, "Sábado").when(codigo == 3, "Domingo")


def tipo_tramo(ordinal):
    """Efectivo (0), viaje inicial con tarjeta (1) o transbordo (2..N)."""
    return F.when(ordinal == 0, "Efectivo").when(ordinal == 1, "Inicial").otherwise("Transbordo")


def mapear(codigo, mapa, resto):
    """Traduce una columna con un diccionario de Python; los valores que no están en el mapa toman `resto`."""
    return F.coalesce(F.create_map(*[F.lit(x) for kv in mapa.items() for x in kv])[codigo], F.lit(resto))


def silver(tabla):
    return spark.read.table(f"`{catalog}`.`{silver_schema}`.{tabla}")


def gold(tabla):
    return spark.read.table(f"`{catalog}`.`{gold_schema}`.{tabla}")


def _literal(texto):
    return "'" + texto.replace("\\", "\\\\").replace("'", "\\'") + "'"


def guardar(df, tabla, descripcion, columnas):
    """Sobrescribe la tabla gold (idempotente) y documenta tabla y columnas en Unity Catalog.

    Exige descripción para cada columna: lo que llega a gold es lo que leen el dashboard y Genie.
    """
    sin_descripcion = [c for c in df.columns if c not in columnas]
    if sin_descripcion:
        raise ValueError(f"{tabla}: columnas sin descripción {sin_descripcion}")
    df = df.select([F.col(c).alias(c, metadata={"comment": columnas[c]}) for c in df.columns])
    nombre = f"`{catalog}`.`{gold_schema}`.{tabla}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(nombre)
    spark.sql(f"COMMENT ON TABLE {nombre} IS {_literal(descripcion)}")
    print(f"{nombre}: {spark.read.table(nombre).count():,} filas")


def ultimos_meses_completos(meses):
    """(desde, hasta) de los últimos `meses` meses completos con datos, según gold.demanda_dia."""
    ultimo = gold("demanda_dia").agg(F.max("fecha")).first()[0]
    # Si los datos no llegan a fin de mes, ese mes está incompleto y no entra en la ventana
    hasta = ultimo if (ultimo + dt.timedelta(days=1)).day == 1 else ultimo.replace(day=1) - dt.timedelta(days=1)
    anio, mes = hasta.year, hasta.month - (meses - 1)
    while mes <= 0:
        anio, mes = anio - 1, mes + 12
    return dt.date(anio, mes, 1), hasta


def dias_tipicos(desde, hasta):
    """Días típicos (sin feriados, paros ni eventos) del período, con su tipo de día."""
    return (
        gold("demanda_dia")
        .filter(F.col("fecha").between(desde, hasta) & ~F.col("es_atipico"))
        .select("fecha", "tipo_dia")
    )
