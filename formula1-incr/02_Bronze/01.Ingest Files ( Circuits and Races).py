# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Circuit .csv

# COMMAND ----------

# MAGIC %md
# MAGIC **Read .csv file**

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file_path_circuits = f"{landing_folder_path}/{v_batch_id}/circuits.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC circuit .csv

# COMMAND ----------

circuit_df = (
    spark.read
        .format('csv')
        .option('header', True)
        .option('inferschema', True)
        .load(source_file_path_circuits)
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

circuit_final_df = circuit_final_df.withColumn('batch_id',F.lit(v_batch_id))

# COMMAND ----------

(
    circuit_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .partitionBy('batch_id')
        .option('replaceWhere', f"batch_id = '{v_batch_id}'")
        .saveAsTable('formula1_incr.bronze.circuits')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from formula1_incr.bronze.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC Races .csv

# COMMAND ----------

source_file_path_races = f"{landing_folder_path}/{v_batch_id}/races.csv"

races_df = (
    spark.read
        .format('csv')
        .option('header', True)
        .option('inferschema', True)
        .load(source_file_path_races)
)

# COMMAND ----------

races_final_df = (
    races_df
        .withColumn('ingest_timestamp', F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
        .withColumn('batch_id',F.lit(v_batch_id))
)

display(races_final_df)

# COMMAND ----------

(
    races_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .partitionBy('batch_id')
        .option('replaceWhere', f"batch_id = '{v_batch_id}'")
        .saveAsTable('formula1_incr.bronze.races')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from formula1_incr.bronze.races;