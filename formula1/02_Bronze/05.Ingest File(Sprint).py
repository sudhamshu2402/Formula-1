# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file_sprint = f"{landing_folder_path}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

sprints_df = spark.read.format("json").option("multiLine", "true").load(source_file_sprint)
display(sprints_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

sprints_df = sprints_df.withColumn("ingest_timestamp", current_timestamp()) \
.withColumn("source_file", lit(source_file_sprint))
sprints_df.write.mode("overwrite").saveAsTable(table_name)
spark.sql(f"OPTIMIZE {table_name} ZORDER BY (season)")
display(spark.sql(f"SELECT * FROM {table_name}"))

# COMMAND ----------

sprints_df.write.mode("overwrite").format("delta").saveAsTable(table_name)