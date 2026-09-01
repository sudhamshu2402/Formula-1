# Databricks notebook source
# MAGIC %md
# MAGIC Transform Circuits Data

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC Read Bronze table

# COMMAND ----------

circuits_df = spark.table(bronze_table)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import col as F

# COMMAND ----------

circuits_rename_df = circuits_df.select(
    F("circuitId").alias("circuit_id"),
    F("circuitName").alias("circuit_name"),
    F("country").alias("country_name"),
    F("lat").alias("latitude"),
    F("long").alias("longitude"),
    F("locality").alias("locality"),
    F("ingest_timestamp"),
    F("source_file")
)

# COMMAND ----------

display(circuits_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Removing curcuit_id null values

# COMMAND ----------

circuits_valid_df = circuits_rename_df.filter(F("circuit_id").isNotNull())
display(circuits_valid_df)

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.distinct()
display(circuits_distinct_df)

# COMMAND ----------

from pyspark.sql.functions import initcap

circuits_final_df = (
    circuits_distinct_df
        .withColumn('circuit_name', initcap(F('circuit_name')))
        .withColumn('locality', initcap(F('locality')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the transformed data to silver circuits table

# COMMAND ----------

{
    circuits_final_df.write.mode('overwrite').saveAsTable(silver_table)
}

# COMMAND ----------

display(spark.table(silver_table))