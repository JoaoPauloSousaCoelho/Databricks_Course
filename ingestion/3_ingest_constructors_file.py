# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Constructors File

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1- Read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Dropped unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "team")\
.withColumn("ingestion_date", current_timestamp())\
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 4 - Write to datalake as a parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
