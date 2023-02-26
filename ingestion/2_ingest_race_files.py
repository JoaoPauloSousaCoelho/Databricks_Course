# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuit csv files

# COMMAND ----------

# MAGIC %md
# MAGIC Step-1: Read the csv file using the sparker dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/lpbcdatalake/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

race_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
     StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read\
                .option('header', True)\
                .schema(race_schema)\
                .csv('/mnt/lpbcdatalake/raw/races.csv')                    

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step_2: Add the ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn('ingestion_time', current_timestamp())\
.withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '), col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select Only the Column Required

# COMMAND ----------

races_selected_df= races_with_timestamp_df.select(col("raceId").alias('race_id'), col("year").alias('race_year'), col("round"),
                                        col("circuitId").alias('circuit_id'), col("name"),col('ingestion_time'),                                                             col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 5 - Write to datalake as a parquet file

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').parquet("/mnt/lpbcdatalake/processed/races")
