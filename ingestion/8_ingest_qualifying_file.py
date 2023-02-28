# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the JSON the spark dataframe API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),   
    StructField('q2', StringType(), True),    
    StructField('q3', StringType(), True)  
    ])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

final_df = qualifying_df\
.withColumnRenamed('qualifyId', 'qualify_id')\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumn('ingestion_date', current_timestamp())
.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 3 - Write to datalake as a parquet file

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")
