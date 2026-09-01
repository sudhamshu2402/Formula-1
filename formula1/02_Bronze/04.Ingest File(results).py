# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file_results =  f"{landing_folder_path}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

result_df = spark.read.format("json").load(source_file_results)
display(result_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

result_df = result_df.withColumn("ingest_timestamp", current_timestamp()) \
                     .withColumn("source_file", col("_metadata.file_path"))
display(result_df)

# COMMAND ----------

result_df.write.mode("overwrite").format("delta").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select season, count(*) from formula1.bronze.results
# MAGIC group by season
# MAGIC order by season asc