# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração inicial

# COMMAND ----------

import json
import requests
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# COMMAND ----------

path = "/Volumes/workspace/default/movies_data/"
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]

# COMMAND ----------

# MAGIC %md
# MAGIC # Inicialização dos dados

# COMMAND ----------

df_titles = spark.read \
                .option("header", True) \
                .option("sep", "\t") \
                .option("nullValue", "\\N") \
                .csv(f"{path}/inbound/title.basics.tsv")

df_ratings = spark.read \
                .option("header", True) \
                .option("sep", "\t") \
                .option("nullValue", "\\N") \
                .csv(f"{path}/inbound/title.ratings.tsv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Unindo tabelas de notas e dados do filme

# COMMAND ----------

df_movies = df_titles.join(df_ratings, on="tconst", how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtragem dos dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manter apenas filmes e remoção de valores nulos

# COMMAND ----------

df_movies = df_movies.filter(
            (col("titleType") == "movie")
            & (col("runtimeMinutes").isNotNull())
            & (col("genres").isNotNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remoção de colunas desnecessárias

# COMMAND ----------

df_movies = df_movies.drop("titleType", "endYear")

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronização do schema das colunas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renomeação dos nomes de colunas

# COMMAND ----------

df_movies = df_movies.withColumnsRenamed({
    "tconst": "id_tconst",
    "primaryTitle": "nm_primary_title",
    "originalTitle": "nm_original_title",
    "isAdult": "bool_is_adult",
    "startYear": "dt_start_year",
    "runtimeMinutes": "nr_runtime_minutes",
    "genres": "nm_genres",
    "averageRating": "nr_average_rating",
    "numVotes": "nr_num_votes"
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast da tipagem das colunas

# COMMAND ----------

df_movies = (
    df_movies
    .withColumn("bool_is_adult", col("bool_is_adult").cast("boolean"))
    .withColumn("dt_start_year", col("dt_start_year").cast("int"))
    .withColumn("nr_runtime_minutes", col("nr_runtime_minutes").cast("int"))
    .withColumn("nr_average_rating", col("nr_average_rating").cast("double"))
    .withColumn("nr_num_votes", col("nr_num_votes").cast("int"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando os dados como Delta table

# COMMAND ----------

df_movies.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"workspace.default.{notebook_name}")
