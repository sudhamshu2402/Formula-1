# Databricks notebook source
dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file_results =  f"{landing_folder_path}/{v_batch_id}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

result_df = spark.read.format("json").load(source_file_results)
display(result_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

result_df = result_df.withColumn("ingest_timestamp", current_timestamp()) \
                     .withColumn("source_file", col("_metadata.file_path")).withColumn('batch_id', lit(v_batch_id))
display(result_df)

# COMMAND ----------

(
    result_df
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy('batch_id')
        .option('replaceWhere', f"batch_id = '{v_batch_id}'")
        .saveAsTable(table_name)
)