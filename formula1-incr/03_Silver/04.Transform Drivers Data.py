# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %run ../00_common/03.Silver-helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

from pyspark.sql import functions as F
drivers_df = spark.read.option('versionAsOf', 0).table(bronze_table)
drivers_df = spark.table(bronze_table)

# COMMAND ----------

drivers_drop_df = drivers_df.drop('url')

# COMMAND ----------

drivers_rename_df = drivers_drop_df.withColumnsRenamed(
    {'driver_id': 'driver_id' , 'dateOfBirth':'date_of_birth'}
)

# COMMAND ----------

from pyspark.sql.functions import initcap, concat_ws

drivers_names_df = (
    drivers_rename_df
    .withColumn('driver_name', 
                initcap(concat_ws(" ", F.col('name.givenName'), F.col('name.familyName')))
                )
    .drop('name')
)

drivers_final_df = (
    drivers_names_df
    .withColumn('nationality', initcap(F.col('nationality')))
)

drivers_distinct_df = drivers_final_df.dropDuplicates(["driver_id"])

display(drivers_distinct_df)

# COMMAND ----------

write_to_silver(
    input_df=drivers_final_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "ingest_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))