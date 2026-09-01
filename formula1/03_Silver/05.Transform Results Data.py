# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql.functions import col as F
results_df = spark.table(bronze_table)
display(results_df)

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
    F('season').isNotNull() &
    F('round').isNotNull() &
    F('driver_id').isNotNull() &
    F('constructor_id').isNotNull()
)

# COMMAND ----------

display(results_valid_df)

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season", "round", "constructor_id", "driver_id"])
display(results_distinct_df)

# COMMAND ----------

from pyspark.sql.functions import initcap
results_final_df = results_distinct_df.withColumn("race_name", initcap(F('race_name')))
display(results_final_df)

# COMMAND ----------

(
    results_final_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)