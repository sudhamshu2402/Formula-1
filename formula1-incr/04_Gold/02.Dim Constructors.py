# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %run ../00_common/04.Gold-helper

# COMMAND ----------

from pyspark.sql import functions as F

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

constructor_df = spark.table(f"{catalog_name}.{silver_schema}.constructors").filter(F.col("batch_id") == v_batch_id)
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

write_to_gold(
    input_df=dim_constructors_df,
    target_table=target_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "region"
    ]
)