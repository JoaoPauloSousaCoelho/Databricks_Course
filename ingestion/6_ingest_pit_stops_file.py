# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the JSON the spark dataframe API

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
.json("/mnt/lpbcdatalake/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

final_df = pit_stops_df\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 3 - Write to datalake as a parquet file

# COMMAND ----------

final_df.write.mode('overwrite').parquet("/mnt/lpbcdatalake/processed/pit_stops")
