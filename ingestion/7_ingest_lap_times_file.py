# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the JSON the spark dataframe API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

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
.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

final_df = lap_times_df\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumn('ingestion_date', current_timestamp())\
.withColumn('data_source', lit(v_data_source))\
.withColumn('data_source', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 3 - Write to datalake as a parquet file

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.lap = src.lap"
merge_delta_data(final_df, "f1_processed", processed_folder_path, "lap_times", merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC   FROM f1_processed.lap_times
# MAGIC   GROUP BY race_id
# MAGIC   ORDER BY race_id DESC;

# COMMAND ----------

#final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

#final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")
