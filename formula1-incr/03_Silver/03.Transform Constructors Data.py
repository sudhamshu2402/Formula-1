# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %run ../00_common/03.Silver-helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql import functions as F
constructors_df = spark.read.option('versionAsOf', 0).table(bronze_table)
constructors_df = spark.table(bronze_table)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_rename_df = (
    constructors_df
    .select(
        F.col('constructorId').alias('constructor_id'),
        F.col('name').alias('constructor_name'),
        F.col('nationality'),
        F.col('ingest_timestamp'),
        F.col('source_file'),
        F.col('batch_id')
    )
)
display(constructors_rename_df)

# COMMAND ----------

constructors_distinct_df = constructors_rename_df.dropDuplicates(['constructor_id'])

display(constructors_distinct_df)

# COMMAND ----------

from pyspark.sql.functions import initcap
constructors_final_df = constructors_distinct_df.withColumn('nationality',initcap(F.col('nationality')))

display(constructors_final_df)

# COMMAND ----------

write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "ingest_timestamp",
        "source_file",
        "batch_id"
    ]
)


# COMMAND ----------

display(spark.table(silver_table))