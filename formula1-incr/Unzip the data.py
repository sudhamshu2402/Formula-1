# Databricks notebook source
import zipfile

# 1. Define the exact file paths
zip_path = "/Volumes/workspace/formula1/formula1_landing/formula1-full-load-landing.zip"
extract_path = "/Volumes/workspace/formula1/formula1_landing/"

# 2. Extract the ZIP contents
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print("Files successfully unzipped!")

# COMMAND ----------

# List all extracted contents in the volume
display(dbutils.fs.ls("/Volumes/workspace/formula1/formula1_landing/"))

# COMMAND ----------

# Read circuits.csv
circuits_df = spark.read.csv("/Volumes/workspace/formula1/formula1_landing/landing/circuits.csv", header=True, inferSchema=True)

# Read constructors.json
constructors_df = spark.read.json("/Volumes/workspace/formula1/formula1_landing/landing/constructors.json")

# Read files inside subfolders (e.g., results folder)
results_df = spark.read.json("/Volumes/workspace/formula1/formula1_landing/landing/results/")