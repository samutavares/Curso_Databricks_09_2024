# Databricks notebook source
# MAGIC %sql 
# MAGIC create or replace table treinamentodatabricks.gold.artists as (
# MAGIC select artist,day,count(*) total_appearances_artist from treinamentodatabricks.silver.spotify_top_50
# MAGIC where day=CURRENT_DATE()
# MAGIC group by artist,day)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace table treinamentodatabricks.gold.songs as (
# MAGIC select name as song,day, count(*) as total_appearances_song from treinamentodatabricks.silver.spotify_top_50
# MAGIC where day=CURRENT_DATE()
# MAGIC group by name,day)
