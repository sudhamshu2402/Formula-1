# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructor_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_constructors_df = (
    constructor_df
    .join(ref_nationality_region_df, constructor_df.nationality == ref_nationality_region_df.nationality)
    .select(constructor_df.constructor_id, 
            constructor_df.constructor_name, 
            constructor_df.nationality, 
            ref_nationality_region_df.region, 
            )
)

display(dim_constructors_df)

# COMMAND ----------

(
    dim_constructors_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{gold_schema}.dim_constructors")
)