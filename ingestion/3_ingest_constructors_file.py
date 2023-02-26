# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Constructors File

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1- Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
.schema(constructors_schema)\
.json("/mnt/lpbcdatalake/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Dropped unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "constructor_ref")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 4 - Write to datalake as a parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/lpbcdatalake/processed/constructors")
