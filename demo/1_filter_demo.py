# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

#races_df = spark.read.option('header', True).option('inferSchema', True).csv(filePath)
races_df = spark.read\
.option('inferSchema', True)\
.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

processed_folder_path
