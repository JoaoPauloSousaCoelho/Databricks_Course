# Databricks notebook source
# MAGIC %fs
# MAGIC ls "/mnt/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/datalakelpbc/demo"

# COMMAND ----------

results_df = spark.read\
.option('inferSchema', True)\
.json("/mnt/datalakelpbc/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').save('/mnt/datalakelpbc/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/datalakelpbc/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

results_df = spark.read.format('delta').load('/mnt/datalakelpbc/demo/results_external')

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').partitionBy('constructorId').saveAsTable('f1_demo.results_partitioned')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Update Delta Table
# MAGIC ##### Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC   WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/"

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/datalakelpbc/demo/results_managed")
deltaTable.update("position<=10", {"points": "21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position>10;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/datalakelpbc/demo/results_managed")
deltaTable.delete("position < 10")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert using Merge

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/datalakelpbc/raw/2021-03-28/"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

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

drivers_day1_df = spark.read\
.schema(driver_schema)\
.json("/mnt/datalakelpbc/raw/2021-03-28/drivers.json")\
.filter("driverId <= 10")\
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read\
.schema(driver_schema)\
.json("/mnt/datalakelpbc/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 AND 15")\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read\
.schema(driver_schema)\
.json("/mnt/datalakelpbc/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20")\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename,surname,createdDate) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename,surname,createdDate) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/datalakelpbc/demo/drivers_merge")

deltaTable.alias("tgt").merge(
      drivers_day3_df.alias("upd"),
      "tgt.driverId = upd.driverId"  
).whenMatchedUpdate(set = {"dob":"upd.dob", "forename":"upd.forename", "surname": "upd.surname", "updatedDate": "current_timestamp()"})\
.whenNotMatchedInsert(values = {
    "driverId": "upd.driverId",
    "dob": "upd.dob",
    "forename": "upd.forename",
    "surname": "upd.surname",
    "createdDate": "current_timestamp()"    
}).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-03-07T21:45:50.000+0000';

# COMMAND ----------

df = spark.read.format("delta").option("timestamp", "2023-03-07T21:45:50.000+0000").load("/mnt/datalakelpbc/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC fornename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC ) 
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC ) USING
# MAGIC PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta;

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/datalakelpbc/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/datalakelpbc/demo/drivers_convert_to_delta_new`
