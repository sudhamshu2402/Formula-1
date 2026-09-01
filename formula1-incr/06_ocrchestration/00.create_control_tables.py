# Databricks notebook source
# MAGIC %run ../00_common/01.environment_config

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists formula1_incr.control;
# MAGIC
# MAGIC create table if not exists formula1_incr.control.batch_control
# MAGIC (
# MAGIC     batch_id string,
# MAGIC     status string,
# MAGIC     created_timestamp timestamp,
# MAGIC     updated_timestamp timestamp
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_incr.control.batch_control;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM formula1_incr.control.batch_control;