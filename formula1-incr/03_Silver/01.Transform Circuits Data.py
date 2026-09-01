# Databricks notebook source
# MAGIC %md
# MAGIC Transform Circuits Data

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %run ../00_common/03.Silver-helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC Read Bronze table

# COMMAND ----------

circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table)

# COMMAND ----------

# Read without time travel to avoid retention policy errors
circuits_df_current = spark.read.table(bronze_table)
display(circuits_df_current)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

# Read fresh data without time travel and apply selection
circuits_fresh_df = spark.read.table(bronze_table)
circuits_selected_fresh = circuits_fresh_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingest_timestamp"),
    F.col("source_file"),
    F.col("batch_id")
)
display(circuits_selected_fresh)

# COMMAND ----------

# MAGIC %md
# MAGIC Removing curcuit_id null values

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_fresh
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "circuitName": "circuit_name",
            "lat": "latitude",
            "long": "longitude"
        })
)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(F.col("circuit_id").isNotNull())
display(circuits_valid_df)

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])
display(circuits_distinct_df)

# COMMAND ----------

from pyspark.sql.functions import initcap

circuits_final_df = (
    circuits_distinct_df
        .withColumn('circuit_name', initcap(F.col('circuit_name')))
        .withColumn('locality', initcap(F.col('locality')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the transformed data to silver circuits table

# COMMAND ----------

write_to_silver(
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_condition="t.circuit_id = s.circuit_id",
    columns_to_update=[
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "ingest_timestamp",
        "source_file",
        "batch_id"        
    ]
)

# COMMAND ----------

display(spark.table(silver_table))