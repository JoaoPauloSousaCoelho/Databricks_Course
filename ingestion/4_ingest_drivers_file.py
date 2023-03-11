# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the JSON the spark dataframe API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "2021-03-21")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields = [
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

# COMMAND ----------

driver_schema = StructType(fields = [
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), False),
    StructField('number', IntegerType(), False),
    StructField('code', StringType(), False),
    StructField('name', name_schema),
    StructField('dob', DateType(), False),
    StructField('nationality', StringType(), False),
    StructField('url', StringType(), False),
])

# COMMAND ----------

drivers_df = spark.read\
.schema(driver_schema)\
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df\
.withColumnRenamed('driverId', 'driver_id')\
.withColumnRenamed('driverRef','driver_ref')\
.withColumn('ingestion_date', current_timestamp())\
.withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))\
.withColumn('data_source', lit(v_data_source))\
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Dropped unwanted columns from the dataframe

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 4 - Write to datalake as a parquet file

# COMMAND ----------

#drivers_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

#drivers_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
