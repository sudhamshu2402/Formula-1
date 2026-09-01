# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

from pyspark.sql.functions import col as F
races_df = spark.table(bronze_table)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_rename_df = races_df.select(
    F("season"),
    F("round"),
    F("circuitId").alias("circuit_id"),
    F("raceName").alias("race_name"),
    F("date").alias("race_date"),
    F('circuitId').alias("circuit_id"),
    F("ingest_timestamp"),
    F("source_file")
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
        .withColumn('race_name', initcap(F('race_name')))
)
display(races_final_df)

# COMMAND ----------

# Drop duplicate circuit_id column - rename columns to unique names then select
temp_cols = [f"{col}_{i}" if races_final_df.columns[:i].count(col) > 0 else col for i, col in enumerate(races_final_df.columns)]
races_temp = races_final_df.toDF(*temp_cols)
races_clean_df = races_temp.select("season", "round", "circuit_id", "race_name", "race_date", "ingest_timestamp", "source_file")
races_clean_df.write.format("delta").mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

display(spark.table(silver_table))