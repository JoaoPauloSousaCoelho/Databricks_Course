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

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields = [
    StructField('resultId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), True),
    StructField('positionOrder', IntegerType(), True),
    StructField('points', FloatType(), True),
    StructField('laps', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),    
    StructField('fastestLap', IntegerType(), True), 
    StructField('rank', IntegerType(), True),
    StructField('fastestLapTime', StringType(), True), 
    StructField('fastestLapSpeed', FloatType(), True), 
    StructField('statusId', StringType(), True)
])

# COMMAND ----------

results_df = spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

#results_df.printSchema()
#display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_with_columns_df = results_df\
.withColumnRenamed('resultId', 'result_id')\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumnRenamed('constructorId','constructor_id')\
.withColumnRenamed('positionText','position_text')\
.withColumnRenamed('positionOrder','position_order')\
.withColumnRenamed('fastestLap','fastest_lap')\
.withColumnRenamed('fastestLapTime','fastest_lap_time')\
.withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
.withColumn('ingestion_date', current_timestamp())\
.withColumn('data_source', lit(v_data_source))\
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Dropped unwanted columns from the dataframe

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC De-Dupe the DataFrame

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 4 - Write to datalake as a parquet file

# COMMAND ----------

# MAGIC %md 
# MAGIC #### METHOD 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### METHOD 2

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results

# COMMAND ----------

#overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/"

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, "f1_processed", processed_folder_path, "results", merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC   FROM f1_processed.results
# MAGIC   GROUP BY race_id
# MAGIC   ORDER BY race_id DESC;

# COMMAND ----------

#results_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/results")

# COMMAND ----------

dbutils.notebook.exit("Success")
