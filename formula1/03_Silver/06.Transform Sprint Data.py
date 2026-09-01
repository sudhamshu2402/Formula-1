# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

import pyspark.sql.functions as F
sprints_df = (
    spark.table(bronze_table)
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
            "positionText":"final_position_text"
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

(
    sprints_final_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)