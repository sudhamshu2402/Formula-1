# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file_const =  f"{landing_folder_path}/constructors.json"
table_name = "constructor"
# DBTITLE 1,Read the source file
const_df = spark.read.json(source_file_const)
# DBTITLE 1,Display the dataframe
display(const_df)
# DBTITLE 1,Add the ingestion timestamp
from pyspark.sql.functions import current_timestamp
const_df = const_df.withColumn("ingest_timestamp", current_timestamp())
# DBTITLE 1,Add the source file name
from pyspark.sql.functions import lit
const_final_df = const_df.withColumn("source_file", lit(source_file_const))
# DBTITLE 1,Display the dataframe
display(const_final_df)

# COMMAND ----------

(
    const_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable('formula1.bronze.constructors')
)