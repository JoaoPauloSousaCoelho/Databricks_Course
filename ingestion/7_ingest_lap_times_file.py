# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the JSON the spark dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

lap_times_schema = StructType(fields = [
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True), 
    StructField('lap', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),    
    StructField('milliseconds',IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv("/mnt/lpbcdatalake/raw/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

final_df = lap_times_df\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 3 - Write to datalake as a parquet file

# COMMAND ----------



# COMMAND ----------

final_df.write.mode('overwrite').parquet("/mnt/lpbcdatalake/processed/lap_times")
