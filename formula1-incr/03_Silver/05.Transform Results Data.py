# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %run ../00_common/03.Silver-helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql import functions as F
results_df = spark.read.option('versionAsOf', 0).table(bronze_table)
results_df = spark.table(bronze_table)

# COMMAND ----------

results_selected_df = results_df.drop("url")
display(results_selected_df)

# COMMAND ----------

results_renamed_df = results_selected_df.withColumnsRenamed(
    {"driverId": "driver_id", 
     "constructorId": "constructor_id", 
     "position": "final_position", 
     "raceId": "race_id",
     "raceName": "race_name",
     "date": "race_date",
     "number":"car_number",
     "laps":"completed_laps",
     "grid": "grid_position",
     "positionText":"final_position_text"})
display(results_renamed_df)

# COMMAND ----------

results_valid_df = results_renamed_df.filter(
    F.col('season').isNotNull() &
    F.col('round').isNotNull() &
    F.col('driver_id').isNotNull() &
    F.col('constructor_id').isNotNull()
)

# COMMAND ----------

display(results_valid_df)

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season", "round", "constructor_id", "driver_id"])
display(results_distinct_df)

# COMMAND ----------

from pyspark.sql.functions import initcap
results_final_df = results_distinct_df.withColumn("race_name", initcap(F.col('race_name')))
display(results_final_df)

# COMMAND ----------

write_to_silver(
    input_df=results_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
    columns_to_update=[
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "ingest_timestamp",
        "source_file",
        "batch_id"
    ]
)