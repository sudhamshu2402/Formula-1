# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %run ../00_common/04.Gold-helper

# COMMAND ----------

from pyspark.sql import functions as F

target_table = f"{catalog_name}.{gold_schema}.facts_results"

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
    .filter(F.col("batch_id") == v_batch_id)
    .withColumn("session_type", F.lit("race"))
    .drop("ingest_timestamp","source_file","race_name","race_date", "batch_id", "created_timestamp", "updated_timestamp")
)

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
    .filter(F.col("batch_id") == v_batch_id)
    .withColumn("session_type", F.lit("sprint"))
    .drop("ingest_timestamp","source_file","race_name","race_date", "batch_id", "created_timestamp", "updated_timestamp")
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

write_to_gold(
    input_df=facts_results_df,
    target_table=target_table,
    merge_condition="""
        t.season = s.season
        AND t.round = s.round
        AND t.constructor_id = s.constructor_id
        AND t.driver_id = s.driver_id
        AND t.session_type = s.session_type
    """,
    columns_to_update=[
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "is_win",
        "is_podium",
        "has_points"
    ]
)

# COMMAND ----------

display(spark.table(target_table))