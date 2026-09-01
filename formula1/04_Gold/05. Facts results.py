# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

from pyspark.sql import functions as F

target_table = f"{catalog_name}.{gold_schema}.facts_results"

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
    .withColumn("session_type", F.lit("race"))
    .drop("ingest_timestamp","source_file","race_name","race_date")
)

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
    .withColumn("session_type", F.lit("sprint"))
    .drop("ingest_timestamp","source_file","race_name","race_date")
)


# COMMAND ----------

results_sprint_df = results_df.unionByName(sprints_df)
display(results_sprint_df)

# COMMAND ----------

facts_results_df =(
    results_sprint_df
    .withColumn("is_win", F.col("final_position") == 1)
    .withColumn("is_podium", F.col("final_position").between(1,3))
    .withColumn("has_points", F.col("points")>0)
)

display(facts_results_df)

# COMMAND ----------

(
    facts_results_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)