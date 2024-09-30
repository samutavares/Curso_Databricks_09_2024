# Databricks notebook source
from datetime import datetime
from pyspark.sql.types import Row
from pyspark.sql import functions as f

# COMMAND ----------

def parseDate(str):
    try:
        return datetime.strptime(str.zfill(10), "%d-%m-%Y").date()
    except ValueError:
        return None

# COMMAND ----------

tListDate = [Row(parseDate(f"{d}-09-2024")) for d in range(1,31)]

# COMMAND ----------

df = spark.createDataFrame(tListDate, ["c_date"])

# COMMAND ----------

display(
    df.dropna()
    .withColumn("year", f.year("c_date"))
    .withColumn("month", f.month("c_date"))
    .withColumn("day", f.day("c_date"))
)
