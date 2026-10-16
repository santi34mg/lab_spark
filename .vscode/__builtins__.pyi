from typing import Any

from databricks.sdk.runtime import *  # type: ignore # noqa: F403
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf as U
from pyspark.sql.session import SparkSession

udf = U
dbutils: Any
spark: SparkSession
sc = spark.sparkContext
sqlContext: SQLContext
sql = sqlContext.sql
table = sqlContext.table
getArgument = dbutils.widgets.getArgument

def displayHTML(html): ...
def display(input=None, *args, **kwargs): ...
