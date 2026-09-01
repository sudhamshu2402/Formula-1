# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %run ../00_common/03.Silver-helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

import pyspark.sql.functions as F

sprints_df = (
    spark.read.table(bronze_table)
    .drop('url')
    .withColumnsRenamed(
        {
            'raceId': 'race_id',
            'driverId': 'driver_id',
            'constructorId': 'constructor_id',
            "position": "final_position", 
            "raceName": "race_name",
            "date": "race_date",
            "number":"car_number",
            "laps":"completed_laps",
            "grid": "grid_position",
            "positionText":"final_position_text",
            
        }
    )
    .filter(
    F.col('season').isNotNull() &
    F.col('round').isNotNull() &
    F.col('driver_id').isNotNull() &
    F.col('constructor_id').isNotNull()
    )
    .dropDuplicates(["season", "round", "constructor_id", "driver_id"])
)

display(sprints_df)

# COMMAND ----------

from pyspark.sql.functions import initcap
sprints_final_df = sprints_df.withColumn("race_name", initcap(F.col('race_name')))
display(sprints_final_df)

# COMMAND ----------

write_to_silver(
    input_df=sprints_final_df,
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

# COMMAND ----------

display(spark.table(silver_table))