# Databricks notebook source
dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')
v_batch_id

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file_const =  f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name = "constructor"
# DBTITLE 1,Read the source file
const_df = spark.read.json(source_file_const)
from pyspark.sql.functions import current_timestamp
const_df = const_df.withColumn("ingest_timestamp", current_timestamp())
# DBTITLE 1,Add the source file name
from pyspark.sql.functions import lit
const_final_df = const_df.withColumn("source_file", lit(source_file_const)).withColumn('batch_id', lit(v_batch_id))
# DBTITLE 1,Display the dataframe
display(const_final_df)

# COMMAND ----------

(
    const_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .partitionBy('batch_id')
        .option('replaceWhere', f"batch_id = '{v_batch_id}'")
        .saveAsTable('formula1_incr.bronze.constructors')
)