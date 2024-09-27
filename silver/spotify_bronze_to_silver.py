# Databricks notebook source
import  pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

def reading_source_tables(tables):

    assets = list()
    for table in tables:
        try:
            df = spark.read.table(f"{table}")
            df=df.withColumn("country", F.lit(table[-2:].upper()))
            assets.append(df)
        except Exception as e:
            error_message = f"Couldn't read {table} on 'schema_name', motive: {e}"
            print(error_message)
    return assets


def union_tables(tables):

    df = tables[0]
    for df_2 in tables[1:]:
        df = df.union(df_2)

    return df

# COMMAND ----------

countries= ['US', 'GB', 'DE', 'FR', 'BR', 'CA', 'AU', 'MX', 'IT', 'ES', 'NL', 'SE', 'NO', 'DK', 'FI', 'PL', 'PT', 'AR', 'CL', 'CO', 'PE', 'UG', 'UY']

source_tables = []
for i in countries:
    source_tables.append(f'treinamentodatabricks.bronze.spotify_top_50_{i.lower()}')
assets = reading_source_tables(source_tables)
df = union_tables(assets)


# COMMAND ----------

delta_table = DeltaTable.forName(spark, "treinamentodatabricks.silver.spotify_top_50")
delta_table.alias("old_data").merge(
    df.alias("new_data"),
    "old_data.country = new_data.country AND old_data.day = new_data.day AND old_data.position = new_data.position"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
