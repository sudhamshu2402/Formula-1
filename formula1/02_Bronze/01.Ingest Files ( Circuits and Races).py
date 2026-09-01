# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Circuit .csv

# COMMAND ----------

# MAGIC %md
# MAGIC **Read .csv file**

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file_path_circuits = f"{landing_folder_path}/circuits.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC circuit .csv

# COMMAND ----------

circuit_df = (
    spark.read
        .format('csv')
        .option('header', True)
        .option('inferschema', True)
        .load('/Volumes/formula1/landing/files/circuits.csv')
)

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Adding meta data columns

# COMMAND ----------

from pyspark.sql import functions as F
circuit_final_df = (
    circuit_df
        .withColumn('ingest_timestamp', F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
)

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write bronze delta table

# COMMAND ----------

(
    circuit_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable('formula1.bronze.circuits')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from formula1.bronze.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC Races .csv

# COMMAND ----------

races_df = (
    spark.read
        .format('csv')
        .option('header', True)
        .option('inferschema', True)
        .load('/Volumes/formula1/landing/files/races.csv')
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_final_df = (
    races_df
        .withColumn('ingest_timestamp', F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
)

display(races_final_df)

# COMMAND ----------

(
    races_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable('formula1.bronze.races')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from formula1.bronze.races;