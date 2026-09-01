# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

from pyspark.sql.functions import col as F
drivers_df = spark.table(bronze_table)
display(drivers_df)

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
                initcap(concat_ws(" ", F('name.givenName'), F('name.familyName')))
                )
    .drop('name')
)
drivers_final_df = (
    drivers_names_df
    .withColumn('nationality', initcap(F('nationality')))
)

drivers_distinct_df = drivers_final_df.dropDuplicates(["driver_id"])

display(drivers_distinct_df)

# COMMAND ----------

(
    drivers_distinct_df.write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .saveAsTable(silver_table)
)