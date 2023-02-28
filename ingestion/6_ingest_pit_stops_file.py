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

dbutils.widgets("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields = [
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),   
    StructField('stop', StringType(), True),    
    StructField('lap', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('duration', StringType(), True),    
    StructField('milliseconds',IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read\
.schema(pit_stops_schema)\
.option('multiLine', True)\
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

final_df = pit_stops_df\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumn('ingestion_date', current_timestamp())\
.withColumn('data_source', v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 3 - Write to datalake as a parquet file

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

db.notebook.exit("Success")
