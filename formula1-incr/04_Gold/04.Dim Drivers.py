# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %run ../00_common/04.Gold-helper

# COMMAND ----------

from pyspark.sql import functions as F
target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers").filter(F.col("batch_id") == v_batch_id)
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_drivers_df = (
    drivers_df
        .join(
            ref_nationality_region_df,
            drivers_df.nationality == ref_nationality_region_df.nationality,
            "left"
        )
        .select(
            drivers_df.driver_id,
            drivers_df.driver_name,
            drivers_df.date_of_birth,
            drivers_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)

display(dim_drivers_df)

# COMMAND ----------

write_to_gold(
    input_df=dim_drivers_df,
    target_table=target_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "nationality_region"
    ]
)