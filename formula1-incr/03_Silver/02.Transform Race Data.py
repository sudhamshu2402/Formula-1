# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %run ../00_common/03.Silver-helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

from pyspark.sql import functions as F
races_df = spark.read.option('versionAsOf', 0).table(bronze_table)
races_df = spark.table(bronze_table)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_rename_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("circuitId").alias("circuit_id"),
    F.col("raceName").alias("race_name"),
    F.col("date").alias("race_date"),
    F.col("ingest_timestamp"),
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

display(races_rename_df)

# COMMAND ----------

races_distinct_df = races_rename_df.dropDuplicates(['season','round'])
display(races_distinct_df)

# COMMAND ----------

from pyspark.sql.functions import initcap

races_final_df = (
    races_distinct_df
        .withColumn('race_name', initcap(F.col('race_name')))
)
display(races_final_df)

# COMMAND ----------

write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_id",
        "ingest_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))