-- Databricks notebook source
show catalogs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing;

CREATE SCHEMA IF NOT EXISTS formula1.bronze;

CREATE SCHEMA IF NOT EXISTS formula1.silver;

CREATE SCHEMA IF NOT EXISTS formula1.gold;

-- COMMAND ----------

use catalog formula1;
show schemas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Volumes in catalog landing

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS formula1.landing.files;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 1. Create source and target variables
-- MAGIC source_path = "/Volumes/workspace/formula1/formula1_landing/landing/"
-- MAGIC target_path = "/Volumes/formula1/landing/files/"
-- MAGIC
-- MAGIC # 2. Move all files directly into the files volume
-- MAGIC dbutils.fs.cp(source_path, target_path, recurse=True)
-- MAGIC
-- MAGIC print("Files successfully moved to /Volumes/formula1/landing/files/")