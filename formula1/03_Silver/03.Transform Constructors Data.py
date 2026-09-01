# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql.functions import col as F
constructors_df = spark.table(bronze_table)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_rename_df = constructors_df.select(
    F('constructorId').alias('constructor_id'),
    F('name').alias('constructor_name'),
    F('nationality'),
    F('ingest_timestamp'),
    F('source_file')
)

display(constructors_rename_df)

# COMMAND ----------

constructors_distinct_df = constructors_rename_df.dropDuplicates(['constructor_id'])

display(constructors_distinct_df)

# COMMAND ----------

from pyspark.sql.functions import initcap
constructors_final_df = constructors_distinct_df.withColumn('nationality',initcap(F('nationality')))

display(constructors_final_df)

# COMMAND ----------

(
    constructors_final_df.write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .saveAsTable(silver_table)
)