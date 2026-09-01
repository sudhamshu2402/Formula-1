# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file_const =  f"{landing_folder_path}/drivers.json"
table_name = "drivers"

# COMMAND ----------

drivers_df = spark.read.json(source_file_const)
display(drivers_df)
drivers_df.printSchema()
from pyspark.sql.functions import col, lit, current_timestamp
drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("ingest_timestamp", lit(current_timestamp())) \
.withColumn("source_file", lit(source_file_const))
display(drivers_df)

# COMMAND ----------

#Write to Delta Lake
drivers_df.write.mode("overwrite").format("delta").saveAsTable(f"{catalog_name}.{bronze_schema}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.drivers
# MAGIC limit(10)