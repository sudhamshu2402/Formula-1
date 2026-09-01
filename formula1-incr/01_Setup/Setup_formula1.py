# Databricks notebook source
# MAGIC %sql
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS formula1_incr;

# COMMAND ----------

# MAGIC %md
# MAGIC Creating schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.silver;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog formula1_incr;
# MAGIC show schemas;

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Volumes in catalog landing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS formula1_incr.landing.files;

# COMMAND ----------


source_path = "/Volumes/workspace/formula1_incr/formula1_landing/"
target_path = "/Volumes/formula1_incr/landing/files/"

dbutils.fs.cp(source_path, target_path, recurse=True)

print("Files successfully moved to /Volumes/formula1_incr/landing/files/")