# Databricks notebook source
dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file_sprint = f"{landing_folder_path}/{v_batch_id}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

sprints_df = spark.read.format("json").option("multiLine", "true").load(source_file_sprint)
display(sprints_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

sprints_df = sprints_df.withColumn("ingest_timestamp", current_timestamp()) \
.withColumn("source_file", lit(source_file_sprint)).withColumn('batch_id', lit(v_batch_id))

# COMMAND ----------

(
    sprints_df
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy('batch_id')
        .option('replaceWhere', f"batch_id = '{v_batch_id}'").saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from formula1_incr.bronze.sprints